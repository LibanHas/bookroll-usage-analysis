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
# Trajectory classification
# --------------------------------------
def classify_trajectory(pcts):
    mean_p = np.mean(pcts)
    slope = (pcts[-1] - pcts[0]) / len(pcts)
    vol = np.std(pcts)

    if mean_p >= 60 and vol < 10:
        return "安定・高位"
    if mean_p <= 40 and vol < 10:
        return "安定・低位"
    if slope >= 5:
        return "上昇型"
    if slope <= -5:
        return "下降型"
    if min(pcts) <= pcts[0] - 10 and pcts[-1] >= pcts[0]:
        return "回復型"
    if vol >= 15:
        return "不安定"
    return "その他"

# --------------------------------------
# Main
# --------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Performance trajectories × BookRoll engagement"
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

    if len(points) < 3:
        print("⚠️ Need ≥3 exams for trajectory analysis.")
        return

    prep_windows = build_prep_windows_between_tests(points, cohort_start_year)

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
        df["exam_order"] = y * 10 + rd
        rows.append(df)

    conn.close()

    df_all = pd.concat(rows, ignore_index=True)
    df_all = coerce_student_id_int(df_all, "student_id")

    df_all["percentile"] = (
        df_all.groupby("exam_order")["score"].rank(pct=True) * 100
    )

    # --------------------------------------
    # Build trajectories
    # --------------------------------------
    traj_rows = []

    for sid, g in df_all.groupby("student_id"):
        pcts = g.sort_values("exam_order")["percentile"].values
        if len(pcts) < 3:
            continue

        traj = classify_trajectory(pcts)
        total_hours = g["hours"].sum()

        traj_rows.append({
            "student_id": sid,
            "trajectory": traj,
            "usage_hours": total_hours
        })

    df_traj = pd.DataFrame(traj_rows)

    # Relative engagement
    df_traj["usage_percentile"] = (
        df_traj["usage_hours"].rank(pct=True) * 100
    )

    # --------------------------------------
    # Aggregate & plot
    # --------------------------------------
    agg = (
        df_traj
        .groupby("trajectory", as_index=False)
        .agg(
            usage_median=("usage_percentile", "median"),
            n_students=("student_id", "count")
        )
        .sort_values("usage_median")
    )

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(agg["trajectory"], agg["usage_median"], color="#4c72b0")

    for i, r in agg.iterrows():
        ax.text(
            i,
            r["usage_median"],
            f"n={r['n_students']}",
            ha="center",
            va="bottom"
        )

    ax.set_ylabel("BookRoll利用（科目内パーセンタイル）")
    ax.set_xlabel("学力推移タイプ")
    ax.set_title(
        f"{subject_jp(subject_slug)} コホート {cohort_start_year}\n"
        "学力推移タイプ別のBookRoll利用"
    )

    fig.tight_layout()
    out = ROOT_DIR / f"{subject_slug}_{cohort_start_year}_trajectory_vs_engagement.png"
    fig.savefig(out, dpi=200)
    print(f"📈 Saved: {out.resolve()}")
    plt.show()

if __name__ == "__main__":
    main()
