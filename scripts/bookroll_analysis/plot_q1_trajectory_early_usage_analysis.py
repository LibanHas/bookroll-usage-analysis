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

# Shared helpers
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
        description="ANALYSIS-ONLY: Q1 percentile trajectories × early BookRoll usage (colour-coded)"
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

    # Percentiles
    df_scores["percentile"] = (
        df_scores
        .groupby(["exam_year", "exam_round"])["score"]
        .rank(pct=True) * 100
    )

    # Ordered exam points
    points = (
        df_scores.groupby(["exam_year", "exam_round"], as_index=False)
        .agg(date_at=("date_at", "min"))
        .sort_values(["exam_year", "exam_round"])
        .reset_index(drop=True)
    )

    points = drop_first_if_spring(points)

    exam_labels = [
        f"{int(r.exam_year)}年 第{int(r.exam_round)}回"
        for _, r in points.iterrows()
    ]

    prep_windows = build_prep_windows_between_tests(points, cohort_start_year)

    # --------------------------------------
    # Identify initial Q1 students
    # --------------------------------------
    first_exam = points.iloc[0]
    y0, r0 = int(first_exam.exam_year), int(first_exam.exam_round)

    df_first = df_scores[
        (df_scores.exam_year == y0) &
        (df_scores.exam_round == r0)
    ][["student_id", "percentile"]]

    q1_students = df_first[df_first.percentile <= 25]["student_id"].unique().tolist()

    # --------------------------------------
    # Longitudinal panel (Q1 only)
    # --------------------------------------
    rows = []

    for idx, p in points.iterrows():
        y, rd = int(p.exam_year), int(p.exam_round)
        start, end = prep_windows[(y, rd)]

        df_p = df_scores[
            (df_scores.exam_year == y) &
            (df_scores.exam_round == rd) &
            (df_scores.student_id.isin(q1_students))
        ][["student_id", "percentile"]].copy()

        df_u = query_usage_hours_by_student(
            conn,
            df_p.student_id.tolist(),
            start,
            end,
            subject_slug
        )

        df = df_p.merge(df_u, on="student_id", how="left")
        df["hours"] = df["hours"].fillna(0.0)
        df["order"] = idx
        rows.append(df)

    conn.close()

    df_long = pd.concat(rows, ignore_index=True)
    df_long = coerce_student_id_int(df_long, "student_id")

    # --------------------------------------
    # Early usage (FIRST prep window only)
    # --------------------------------------
    early_usage = (
        df_long
        .sort_values("order")
        .groupby("student_id")
        .first()["hours"]
    )

    usage_group = pd.qcut(
        early_usage,
        q=3,
        labels=["低利用", "中利用", "高利用"],
        duplicates="drop"
    )

    usage_stats = (
    early_usage
    .groupby(usage_group)
    .agg(
        median_hours="median",
        mean_hours="mean",
        n="count"
    )
    )


    df_long["early_usage_group"] = df_long["student_id"].map(usage_group)

    # --------------------------------------
    # Plot (analysis-only)
    # --------------------------------------
    fig, ax = plt.subplots(figsize=(9, 6))

    color_map = {
        "低利用": "#1f77b4",   # blue
        "中利用": "#ff7f0e",   # orange
        "高利用": "#2ca02c",   # green
    }

    group_labels = {
    grp: f"{grp}（中央値 {usage_stats.loc[grp, 'median_hours']:.1f}h）"
    for grp in usage_stats.index
    }


    # Individual trajectories (colour-coded)
    for sid, g in df_long.groupby("student_id"):
        grp = g["early_usage_group"].iloc[0]
        ax.plot(
            g["order"],
            g["percentile"],
            color=color_map[grp],
            alpha=0.25,
            linewidth=1
        )

    # Median trajectories
    for grp, g in df_long.groupby("early_usage_group"):
        med = (
            g.groupby("order")["percentile"]
            .median()
            .sort_index()
        )
        ax.plot(
            med.index,
            med.values,
            color=color_map[grp],
            linewidth=3,
            label=group_labels[grp]
        )


    ax.set_ylim(0, 100)
    ax.set_ylabel("順位（パーセンタイル）")
    ax.set_xlabel("試験")
    ax.set_xticks(range(len(exam_labels)))
    ax.set_xticklabels(exam_labels)
    
    ax.set_title(
        f"{subject_jp(subject_slug)} {cohort_start_year}コホート\n"
        "初期Q1生徒：順位推移 × 初期BookRoll利用量（分析用）"
    )

    ax.legend(title="初期BookRoll利用量", frameon=False)

    fig.tight_layout()
    out = ROOT_DIR / f"{subject_slug}_{cohort_start_year}_Q1_trajectory_early_usage_coloured.png"
    fig.savefig(out, dpi=200)
    plt.show()

    print(f"📈 Saved (analysis-only): {out.resolve()}")

if __name__ == "__main__":
    main()
