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
        description="Post-setback recovery: relative engagement × 2試験後の順位回復"
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
    # Identify exam points
    # --------------------------------------
    points = (
        df_scores.groupby(["exam_year", "exam_round"], as_index=False)
        .agg(date_at=("date_at", "min"))
        .sort_values(["exam_year", "exam_round"])
        .reset_index(drop=True)
    )

    points = drop_first_if_spring(points)

    if len(points) < 3:
        print("⚠️ Not enough exams for post-setback recovery analysis (need ≥ 3).")
        return

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
    # Percentiles per exam (controls difficulty)
    # --------------------------------------
    df_all["percentile"] = (
        df_all
        .groupby(["exam_year", "exam_round"])["score"]
        .rank(pct=True) * 100
    )

    # Chronological order
    df_all["test_order"] = df_all["exam_year"] * 10 + df_all["exam_round"]
    df_all = df_all.sort_values(["student_id", "test_order"])

    # --------------------------------------
    # Identify setbacks and recovery
    # --------------------------------------
    df_all["pct_prev"] = df_all.groupby("student_id")["percentile"].shift(1)
    df_all["pct_next2"] = df_all.groupby("student_id")["percentile"].shift(-1)

    df_all["setback"] = df_all["percentile"] < df_all["pct_prev"]

    df_setback = df_all[df_all["setback"]].copy()
    df_setback = df_setback.dropna(subset=["pct_next2"])

    df_setback["recovery"] = df_setback["pct_next2"] - df_setback["percentile"]

    # --------------------------------------
    # Relative engagement after setback
    # --------------------------------------
    df_setback["hours_next"] = df_all.groupby("student_id")["hours"].shift(-1)
    df_setback = df_setback.dropna(subset=["hours_next"])

    # Rank engagement within setback group
    df_setback["engagement_rank"] = (
        df_setback["hours_next"].rank(pct=True)
    )

    df_setback["engagement_tier"] = pd.cut(
        df_setback["engagement_rank"],
        bins=[0, 0.3, 0.7, 1.0],
        labels=["低エンゲージメント", "中エンゲージメント", "高エンゲージメント"],
        include_lowest=True
    )

    # --------------------------------------
    # Aggregate
    # --------------------------------------
    order = ["低エンゲージメント", "中エンゲージメント", "高エンゲージメント"]

    agg = (
        df_setback
        .groupby("engagement_tier", as_index=False)
        .agg(
            recovery_median=("recovery", "median"),
            n_students=("student_id", "nunique"),
        )
    )

    agg["engagement_tier"] = pd.Categorical(
        agg["engagement_tier"],
        categories=order,
        ordered=True
    )
    agg = agg.sort_values("engagement_tier")

    # --------------------------------------
    # Plot
    # --------------------------------------
    fig, ax = plt.subplots(figsize=(8, 5))

    ax.bar(
        agg["engagement_tier"].astype(str),
        agg["recovery_median"],
        color="#4c72b0"
    )

    for i, r in agg.iterrows():
        ax.text(
            i,
            r["recovery_median"],
            f"n={int(r['n_students'])}",
            ha="center",
            va="bottom" if r["recovery_median"] >= 0 else "top"
        )

    ax.axhline(0, color="black", linewidth=1)
    ax.set_ylabel("2試験後の順位回復（中央値・パーセンタイル）")
    ax.set_xlabel("セットバック後の相対的学習エンゲージメント")

    ax.set_title(
        f"{subject_jp(subject_slug)} コホート {cohort_start_year}\n"
        "相対的な失速後のBookRoll利用と中期的な回復"
    )

    fig.tight_layout()

    out_png = ROOT_DIR / f"{subject_slug}_{cohort_start_year}_post_setback_recovery_relative.png"
    fig.savefig(out_png, dpi=200)
    print(f"📈 Saved: {out_png.resolve()}")
    plt.show()


if __name__ == "__main__":
    main()
