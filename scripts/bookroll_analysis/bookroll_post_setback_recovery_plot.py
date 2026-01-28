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
# Engagement pattern classifier
# --------------------------------------
def classify_engagement_pattern(h1, h2):
    """
    h1 = usage hours in first window after setback
    h2 = usage hours in second window after setback
    """
    if h1 == 0 and h2 == 0:
        return "利用なし"

    if h1 > 0 and h2 == 0:
        return "一時的増加"

    if h1 > 0 and h2 > 0:
        if h2 >= h1 * 0.7:
            return "継続的増加"
        else:
            return "不安定"

    if h1 == 0 and h2 > 0:
        return "遅れて増加"

    return "不明"

# --------------------------------------
# Main
# --------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Post-setback engagement patterns and percentile recovery"
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
    # Identify ordered exam points
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
    # Build long per-student dataset
    # --------------------------------------
    rows = []

    for _, p in points.iterrows():
        y, rd = int(p.exam_year), int(p.exam_round)
        start, end = prep_windows[(y, rd)]

        df_p = df_scores[
            (df_scores.exam_year == y) &
            (df_scores.exam_round == rd)
        ][["student_id", "score", "percentile"]].copy()

        df_u = query_usage_hours_by_student(
            conn,
            df_p.student_id.tolist(),
            start,
            end,
            subject_slug
        )

        df = df_p.merge(df_u, on="student_id", how="left")
        df["hours"] = df["hours"].fillna(0.0)
        df["exam_year"] = y
        df["exam_round"] = rd
        rows.append(df)

    conn.close()

    df = pd.concat(rows, ignore_index=True)
    df = coerce_student_id_int(df, "student_id")

    # --------------------------------------
    # Order exams & compute changes
    # --------------------------------------
    df["order"] = df["exam_year"] * 10 + df["exam_round"]
    df = df.sort_values(["student_id", "order"])

    df["delta_pct"] = df.groupby("student_id")["percentile"].diff()
    df["prev_pct"] = df.groupby("student_id")["percentile"].shift(1)

    # --------------------------------------
    # Identify setbacks (drop ≥ 10 pct points)
    # --------------------------------------
    setbacks = df[
        (df["delta_pct"] <= -10)
    ].copy()

    # --------------------------------------
    # Baseline quartile (before setback)
    # --------------------------------------
    setbacks["baseline_quartile"] = (
        setbacks
        .groupby(["exam_year", "exam_round"])["prev_pct"]
        .transform(lambda s: pd.qcut(s, 4, labels=["Q1","Q2","Q3","Q4"], duplicates="drop"))
    )

    # --------------------------------------
    # Measure engagement after setback
    # --------------------------------------
    records = []

    for _, r in setbacks.iterrows():
        sid = r.student_id
        order = r.order

        future = df[
            (df.student_id == sid) &
            (df.order > order)
        ].sort_values("order")

        if len(future) < 2:
            continue

        h1 = future.iloc[0]["hours"]
        h2 = future.iloc[1]["hours"]

        pct_recovery = future.iloc[1]["percentile"] - r.percentile

        records.append({
            "student_id": sid,
            "baseline_quartile": r.baseline_quartile,
            "engagement_pattern": classify_engagement_pattern(h1, h2),
            "pct_recovery": pct_recovery,
        })

    df_final = pd.DataFrame(records).dropna()

    # --------------------------------------
    # Aggregate
    # --------------------------------------
    agg = (
        df_final
        .groupby(["engagement_pattern", "baseline_quartile"], as_index=False)
        .agg(
            median_recovery=("pct_recovery", "median"),
            n=("student_id", "nunique")
        )
    )

    # --------------------------------------
    # Plot
    # --------------------------------------
    patterns = [
        "利用なし",
        "一時的増加",
        "遅れて増加",
        "継続的増加",
        "不安定",
    ]

    colors = {
        "Q1": "#d62728",
        "Q2": "#ff7f0e",
        "Q3": "#2ca02c",
        "Q4": "#1f77b4",
    }

    fig, ax = plt.subplots(figsize=(10, 6))

    for q, c in colors.items():
        d = agg[agg["baseline_quartile"] == q]
        d = d.set_index("engagement_pattern").reindex(patterns)

        ax.plot(
            patterns,
            d["median_recovery"],
            marker="o",
            label=q,
            color=c
        )

    ax.axhline(0, color="black", linewidth=1)
    ax.set_ylabel("2試験後の順位回復（パーセンタイル）")
    ax.set_xlabel("成績低下後のBookRoll利用パターン")
    ax.set_title(
        f"{subject_jp(subject_slug)} {cohort_start_year}コホート\n"
        "成績低下後の学習行動と中期的な相対順位回復"
    )

    ax.legend(title="開始時の得点四分位")

    fig.tight_layout()

    out = ROOT_DIR / f"{subject_slug}_{cohort_start_year}_post_setback_recovery.png"
    fig.savefig(out, dpi=200)
    print(f"📈 Saved: {out.resolve()}")
    plt.show()


if __name__ == "__main__":
    main()
