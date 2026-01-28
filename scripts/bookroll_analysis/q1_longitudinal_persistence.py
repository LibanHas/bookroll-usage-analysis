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
        description="Initial Q1 students: relative BookRoll usage vs longitudinal recovery"
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
    # Percentiles per exam
    # --------------------------------------
    df_scores["percentile"] = (
        df_scores
        .groupby(["exam_year", "exam_round"])["score"]
        .rank(pct=True) * 100
    )

    # --------------------------------------
    # Ordered exam points
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
    # Identify initial Q1 students
    # --------------------------------------
    first_exam = points.iloc[0]
    y0, r0 = int(first_exam.exam_year), int(first_exam.exam_round)

    df_first = df_scores[
        (df_scores.exam_year == y0) &
        (df_scores.exam_round == r0)
    ][["student_id", "percentile"]]

    q1_students = df_first[df_first.percentile <= 25]["student_id"].unique().tolist()
    print(f"👥 Initial Q1 students: {len(q1_students)}")

    # --------------------------------------
    # Build longitudinal panel (Q1 only)
    # --------------------------------------
    rows = []

    for _, p in points.iterrows():
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
        df["order"] = y * 10 + rd
        rows.append(df)

    conn.close()

    df_long = pd.concat(rows, ignore_index=True)
    df_long = coerce_student_id_int(df_long, "student_id")

    # --------------------------------------
    # Student-level summaries
    # --------------------------------------
    summaries = []

    for sid, g in df_long.groupby("student_id"):
        g = g.sort_values("order")
        if len(g) < 2:
            continue

        usage = g["hours"].values
        pct = g["percentile"].values

        summaries.append({
            "student_id": sid,
            "mean_usage": usage.mean(),
            "net_percentile_gain": pct[-1] - pct[0],
        })

    df_sum = pd.DataFrame(summaries)

    # --------------------------------------
    # Two-stage engagement binning (KEY FIX)
    # --------------------------------------
    df_sum["usage_group"] = "未利用"
    mask_users = df_sum["mean_usage"] > 0

    if mask_users.sum() >= 3:
        df_sum.loc[mask_users, "usage_group"] = pd.qcut(
            df_sum.loc[mask_users, "mean_usage"],
            q=3,
            labels=["低利用", "中利用", "高利用"],
            duplicates="drop"
        )

    order = ["未利用", "低利用", "中利用", "高利用"]
    df_sum["usage_group"] = pd.Categorical(df_sum["usage_group"], order, ordered=True)

    # --------------------------------------
    # Aggregate
    # --------------------------------------
    agg = (
        df_sum
        .groupby("usage_group", as_index=False)
        .agg(
            median_gain=("net_percentile_gain", "median"),
            n=("student_id", "count")
        )
        .sort_values("usage_group")
    )

    # --------------------------------------
    # Ensure all usage groups appear (even if empty)
    # --------------------------------------
    full_index = pd.DataFrame({"usage_group": ["未利用", "低利用", "中利用", "高利用"]})

    agg = (
        full_index
        .merge(agg, on="usage_group", how="left")
    )

    agg["median_gain"] = agg["median_gain"].fillna(0)
    agg["n"] = agg["n"].fillna(0).astype(int)

    # --------------------------------------
    # Plot
    # --------------------------------------
    fig, ax = plt.subplots(figsize=(8, 5))

    ax.bar(agg["usage_group"], agg["median_gain"])

    for i, r in agg.iterrows():
        ax.text(
            i,
            r["median_gain"],
            f"n={r['n']}",
            ha="center",
            va="bottom"
        )

    ax.axhline(0, color="black", linewidth=1)
    ax.set_ylabel("初回 → 最終の順位変化（パーセンタイル）")
    ax.set_xlabel("BookRoll平均利用量（Q1内・相対）")
    ax.set_title(
        f"{subject_jp(subject_slug)} {cohort_start_year}コホート\n"
        "初期Q1生徒におけるBookRoll利用量と長期的な順位回復"
    )

    fig.tight_layout()
    out = ROOT_DIR / f"{subject_slug}_{cohort_start_year}_Q1_relative_usage_longitudinal.png"
    fig.savefig(out, dpi=200)
    print(f"📈 Saved: {out.resolve()}")
    plt.show()


if __name__ == "__main__":
    main()
