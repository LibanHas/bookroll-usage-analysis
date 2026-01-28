from pathlib import Path
import sys
import argparse
import importlib.util
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
        description="Q1 students: BookRoll usage vs percentile recovery"
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
    # Load cohort & scores
    # --------------------------------------
    cohort_ids, anchor = fetch_cohort_ids_from_db_auto_anchor(
        conn, cohort_start_year, start_grade, subject_slug
    )

    df_scores = load_db_benesse_scores_student_level(
        conn, cohort_ids, cohort_start_year, start_grade, subject_slug
    )

    df_scores = coerce_student_id_int(df_scores, "student_id")

    # --------------------------------------
    # Identify exam points
    # --------------------------------------
    points = (
        df_scores.groupby(["exam_year", "exam_round"], as_index=False)
        .agg(date_at=("date_at", "min"))
        .sort_values(["exam_year", "exam_round"])
        .reset_index(drop=True)
    )

    points = drop_first_if_spring(points)

    if len(points) < 2:
        print("⚠️ Need at least 2 exams.")
        return

    baseline = points.iloc[0]
    last_exam = points.iloc[-1]

    # --------------------------------------
    # Baseline percentiles
    # --------------------------------------
    df_scores["percentile"] = (
        df_scores
        .groupby(["exam_year", "exam_round"])["score"]
        .rank(pct=True) * 100
    )

    df_base = df_scores[
        (df_scores.exam_year == baseline.exam_year) &
        (df_scores.exam_round == baseline.exam_round)
    ][["student_id", "percentile"]].copy()

    # Lowest quartile only
    df_base["quartile"] = pd.qcut(
        df_base["percentile"],
        4,
        labels=["Q1", "Q2", "Q3", "Q4"]
    )

    q1_ids = df_base[df_base.quartile == "Q1"]["student_id"].tolist()

    # --------------------------------------
    # Prep windows & usage
    # --------------------------------------
    prep_windows = build_prep_windows_between_tests(points, cohort_start_year)

    usage_rows = []

    for _, p in points.iloc[1:].iterrows():  # AFTER baseline
        y, rd = int(p.exam_year), int(p.exam_round)
        start, end = prep_windows[(y, rd)]

        df_u = query_usage_hours_by_student(
            conn,
            q1_ids,
            start,
            end,
            subject_slug
        )

        usage_rows.append(df_u)

    conn.close()

    df_usage = (
        pd.concat(usage_rows)
        .groupby("student_id", as_index=False)
        .agg(total_hours=("hours", "sum"))
    )

    # --------------------------------------
    # Recovery measurement
    # --------------------------------------
    df_last = df_scores[
        (df_scores.exam_year == last_exam.exam_year) &
        (df_scores.exam_round == last_exam.exam_round)
    ][["student_id", "percentile"]]

    df = (
        df_base[["student_id", "percentile"]]
        .merge(df_last, on="student_id", suffixes=("_base", "_last"))
        .merge(df_usage, on="student_id", how="left")
    )

    df["total_hours"] = df["total_hours"].fillna(0.0)
    df["pct_gain"] = df["percentile_last"] - df["percentile_base"]

    # --------------------------------------
    # Relative engagement tiers (within Q1)
    # --------------------------------------
    df["usage_tier"] = pd.qcut(
        df["total_hours"].rank(method="first"),
        3,
        labels=["低利用", "中利用", "高利用"]
    )

    # --------------------------------------
    # Aggregate & plot
    # --------------------------------------
    agg = (
        df.groupby("usage_tier", as_index=False)
        .agg(
            median_gain=("pct_gain", "median"),
            n_students=("student_id", "count")
        )
    )

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(agg["usage_tier"], agg["median_gain"], color="#4c72b0")

    for i, r in agg.iterrows():
        ax.text(i, r["median_gain"], f"n={r.n_students}", ha="center", va="bottom")

    ax.axhline(0, color="black", linewidth=1)
    ax.set_ylabel("順位変化（パーセンタイル）")
    ax.set_xlabel("BookRoll利用量（Q1内相対）")
    ax.set_title(
        f"{subject_jp(subject_slug)} {cohort_start_year}コホート\n"
        "初期Q1生徒のBookRoll利用と相対順位回復"
    )

    fig.tight_layout()
    out = ROOT_DIR / f"{subject_slug}_{cohort_start_year}_Q1_recovery_vs_usage.png"
    fig.savefig(out, dpi=200)
    print(f"📈 Saved: {out.resolve()}")
    plt.show()


if __name__ == "__main__":
    main()
