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
assert spec and spec.loader
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
        description="Student-level: typical BookRoll利用変化とベネッセ得点変化（全試験）"
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

    points = (
        df_scores.groupby(["exam_year", "exam_round"], as_index=False)
        .agg(date_at=("date_at", "min"))
        .sort_values(["exam_year", "exam_round"])
        .reset_index(drop=True)
    )

    points = drop_first_if_spring(points)
    prep_windows = build_prep_windows_between_tests(points, cohort_start_year)

    # --------------------------------------
    # Build per-student per-test usage
    # --------------------------------------
    per_test = []

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
        per_test.append(df)

    conn.close()

    df_all = pd.concat(per_test, ignore_index=True)
    df_all = coerce_student_id_int(df_all, "student_id")

    # --------------------------------------
    # Chronological ordering
    # --------------------------------------
    df_all["test_order"] = df_all["exam_year"] * 10 + df_all["exam_round"]
    df_all = df_all.sort_values(["student_id", "test_order"])

    # --------------------------------------
    # Transition deltas
    # --------------------------------------
    df_all["delta_hours"] = df_all.groupby("student_id")["hours"].diff()
    df_all["delta_score"] = df_all.groupby("student_id")["score"].diff()

    df_delta = df_all.dropna(subset=["delta_hours", "delta_score"]).copy()

    # --------------------------------------
    # Collapse to student-level (KEY CHANGE)
    # --------------------------------------
    df_student = (
        df_delta.groupby("student_id", as_index=False)
        .agg(
            delta_hours_med=("delta_hours", "median"),
            delta_score_med=("delta_score", "median"),
        )
    )

    # --------------------------------------
    # Bin students by typical usage change
    # --------------------------------------
    bins = [-np.inf, -5, -1, 1, 5, np.inf]
    labels = [
        "大きく減少",
        "やや減少",
        "ほぼ変化なし",
        "やや増加",
        "大きく増加",
    ]

    df_student["usage_change_bin"] = pd.cut(
        df_student["delta_hours_med"],
        bins=bins,
        labels=labels,
    )

    # --------------------------------------
    # Final aggregation (students!)
    # --------------------------------------
    agg = (
        df_student.groupby("usage_change_bin", as_index=False)
        .agg(
            delta_score_median=("delta_score_med", "median"),
            n_students=("student_id", "nunique"),
        )
    )

    # --------------------------------------
    # Plot
    # --------------------------------------
    fig, ax = plt.subplots(figsize=(10, 6))

    ax.bar(
        agg["usage_change_bin"].astype(str),
        agg["delta_score_median"],
        color="#4c72b0",
    )

    for i, r in agg.iterrows():
        ax.text(
            i,
            r["delta_score_median"],
            f"n={int(r['n_students'])}",
            ha="center",
            va="bottom" if r["delta_score_median"] >= 0 else "top",
            fontsize=9,
        )

    ax.axhline(0, color="black", linewidth=1)

    ax.set_ylabel("ベネッセ得点の変化（中央値）")
    ax.set_xlabel("BookRoll利用時間の典型的変化（生徒別）")
    ax.set_title(
        f"{subject_jp(subject_slug)} コホート {cohort_start_year}\n"
        "BookRoll利用時間の変化とベネッセ得点変化（同一生徒内・全試験）"
    )

    fig.tight_layout()

    out_png = ROOT_DIR / f"{subject_slug}_{cohort_start_year}_student_level_usage_vs_score_change.png"
    fig.savefig(out_png, dpi=200)
    print(f"📈 Saved: {out_png.resolve()}")
    plt.show()


if __name__ == "__main__":
    main()
