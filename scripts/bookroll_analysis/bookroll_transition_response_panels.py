from pathlib import Path
import sys
import argparse
import importlib.util
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector

# --------------------------------------
# Paths / imports
# --------------------------------------
CURRENT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = CURRENT_DIR.parent
ROOT_DIR = SCRIPTS_DIR.parent

for p in (CURRENT_DIR, SCRIPTS_DIR, ROOT_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from db_config import DB_CONFIG

# Japanese font
JP_FONT_PATH = SCRIPTS_DIR / "jp_font_setup.py"
spec = importlib.util.spec_from_file_location("jp_font_setup", str(JP_FONT_PATH))
jp_font_setup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(jp_font_setup)
setup_japanese_font = jp_font_setup.setup_japanese_font

# Shared logic
from plot_scores_by_usage_quartile import (
    normalize_subject,
    subject_jp,
    fetch_cohort_ids_from_db_auto_anchor,
    load_db_benesse_scores_student_level,
    drop_first_if_spring,
    build_prep_windows_between_tests,
    query_usage_hours_by_student,
    coerce_student_id_int,
)

# --------------------------------------
# Main
# --------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Transition-level: BookRoll利用変化 × 得点パーセンタイル変化（開始得点四分位別）"
    )
    parser.add_argument("cohort_start_year", type=int)
    parser.add_argument("start_grade", type=int)
    parser.add_argument("subject", type=str)
    args = parser.parse_args()

    cohort_start_year = args.cohort_start_year
    start_grade = args.start_grade
    subject_slug = normalize_subject(args.subject)

    setup_japanese_font()
    conn = mysql.connector.connect(**DB_CONFIG)

    # --------------------------------------
    # Load cohort + scores
    # --------------------------------------
    cohort_ids, _ = fetch_cohort_ids_from_db_auto_anchor(
        conn, cohort_start_year, start_grade, subject_slug
    )

    df_scores = load_db_benesse_scores_student_level(
        conn, cohort_ids, cohort_start_year, start_grade, subject_slug
    )

    # --------------------------------------
    # Identify test points and prep windows
    # --------------------------------------
    points = (
        df_scores.groupby(["exam_year", "exam_round"], as_index=False)
        .agg(date_at=("date_at", "min"))
        .sort_values(["exam_year", "exam_round"])
        .reset_index(drop=True)
    )

    points = drop_first_if_spring(points)
    prep_windows = build_prep_windows_between_tests(points, cohort_start_year)

    # --------------------------------------
    # Build per-student per-exam dataset
    # --------------------------------------
    rows = []

    for _, p in points.iterrows():
        y, rd = int(p.exam_year), int(p.exam_round)
        start, end = prep_windows[(y, rd)]

        df_point = df_scores[
            (df_scores.exam_year == y) &
            (df_scores.exam_round == rd)
        ][["student_id", "score"]].copy()

        df_usage = query_usage_hours_by_student(
            conn,
            df_point.student_id.tolist(),
            start,
            end,
            subject_slug
        )

        df = df_point.merge(df_usage, on="student_id", how="left")
        df["hours"] = pd.to_numeric(df["hours"], errors="coerce").fillna(0.0)
        df["exam_year"] = y
        df["exam_round"] = rd
        rows.append(df)

    conn.close()

    df_all = pd.concat(rows, ignore_index=True)
    df_all = coerce_student_id_int(df_all, "student_id")

    # --------------------------------------
    # Percentile ranks (controls for exam difficulty)
    # --------------------------------------
    df_all["percentile"] = (
        df_all
        .groupby(["exam_year", "exam_round"])["score"]
        .rank(pct=True) * 100
    )

    # --------------------------------------
    # Chronological order + deltas
    # --------------------------------------
    df_all["test_order"] = df_all["exam_year"] * 10 + df_all["exam_round"]
    df_all = df_all.sort_values(["student_id", "test_order"])

    df_all["delta_hours"] = df_all.groupby("student_id")["hours"].diff()
    df_all["delta_percentile"] = df_all.groupby("student_id")["percentile"].diff()

    # --------------------------------------
    # Baseline score quartile (FROM previous exam)
    # --------------------------------------
    df_all["baseline_score"] = (
        df_all.groupby("student_id")["score"].shift(1)
    )

    def safe_qcut(series):
        try:
            return pd.qcut(
                series,
                4,
                labels=["Q1", "Q2", "Q3", "Q4"],
                duplicates="drop"
            )
        except ValueError:
            # If even duplicates="drop" fails, return all NaN
            return pd.Series([np.nan] * len(series), index=series.index)

    df_all["baseline_quartile"] = (
                df_all
                .groupby(["exam_year", "exam_round"])["baseline_score"]
                .transform(safe_qcut)
            )


    df_delta = df_all.dropna(
        subset=["delta_hours", "delta_percentile", "baseline_quartile"]
    ).copy()

    # --------------------------------------
    # Usage change bins
    # --------------------------------------
    bins = [-np.inf, -5, -1, 1, 5, np.inf]
    usage_labels = [
        "大きく減少",
        "やや減少",
        "ほぼ変化なし",
        "やや増加",
        "大きく増加",
    ]

    df_delta["usage_change_bin"] = pd.cut(
        df_delta["delta_hours"],
        bins=bins,
        labels=usage_labels,
    )

    # --------------------------------------
    # Build explicit transitions
    # --------------------------------------
    points_sorted = points.sort_values(["exam_year", "exam_round"]).reset_index(drop=True)

    transitions = []
    for i in range(1, len(points_sorted)):
        prev = points_sorted.iloc[i - 1]
        curr = points_sorted.iloc[i]
        transitions.append({
            "from_year": int(prev.exam_year),
            "from_round": int(prev.exam_round),
            "to_year": int(curr.exam_year),
            "to_round": int(curr.exam_round),
        })

    # --------------------------------------
    # Plot
    # --------------------------------------
    n_panels = len(transitions)
    fig, axes = plt.subplots(
        1, n_panels,
        figsize=(4 * n_panels, 5),
        sharey=True
    )

    if n_panels == 1:
        axes = [axes]

    quartiles = ["Q1", "Q2", "Q3", "Q4"]
    colors = ["#d62728", "#ff7f0e", "#2ca02c", "#1f77b4"]

    for ax, t in zip(axes, transitions):
        y_to = t["to_year"]
        rd_to = t["to_round"]
        label = f"{t['from_year']} R{t['from_round']} → {y_to} R{rd_to}"

        d = df_delta[
            (df_delta.exam_year == y_to) &
            (df_delta.exam_round == rd_to)
        ]

        # Ensure all usage bins exist
        all_bins = pd.Categorical(
            usage_labels,
            categories=usage_labels,
            ordered=True
        )

        agg = (
            d.groupby(["usage_change_bin", "baseline_quartile"], observed=False)
             .agg(
                 delta_pct_median=("delta_percentile", "median"),
                 n_students=("student_id", "nunique"),
             )
             .reset_index()
        )

        for q, c in zip(quartiles, colors):
            dq = agg[agg["baseline_quartile"] == q]
            dq = dq.set_index("usage_change_bin").reindex(all_bins)

            ax.plot(
                dq.index.astype(str),
                dq["delta_pct_median"],
                marker="o",
                color=c,
                label=q
            )

        ax.axhline(0, color="black", linewidth=1)
        ax.set_title(label)
        ax.tick_params(axis="x", rotation=30)

    axes[0].set_ylabel("次回試験での順位変化（パーセンタイル）")

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles, labels,
        title="開始時の得点四分位",
        loc="lower center",
        ncol=4,
        frameon=True
    )

    fig.suptitle(
        f"{subject_jp(subject_slug)} コホート {cohort_start_year}\n"
        "BookRoll利用変化と次回試験での相対順位変化（開始得点四分位別）",
        fontsize=14,
    )

    fig.tight_layout(rect=[0, 0.15, 1, 0.92])

    out_png = ROOT_DIR / f"{subject_slug}_{cohort_start_year}_transition_percentile_response.png"
    fig.savefig(out_png, dpi=200)
    print(f"📈 Saved: {out_png.resolve()}")
    plt.show()


if __name__ == "__main__":
    main()
