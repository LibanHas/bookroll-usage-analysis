from pathlib import Path
import sys
import argparse
import importlib.util

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
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

# --------------------------------------
# Import shared logic
# --------------------------------------
from plot_scores_by_usage_quartile import (
    fetch_cohort_ids_from_db_auto_anchor,
    load_db_benesse_scores_student_level,
    drop_first_if_spring,
    build_prep_windows_between_tests,
    query_usage_hours_by_student,
    assign_quartiles,
)

# --------------------------------------
# Quartile labels
# --------------------------------------
QUARTILE_LABELS_JP = {
    "Q1": "利用時間 最少（下位25％）",
    "Q2": "利用時間 やや少（25–50％）",
    "Q3": "利用時間 やや多（50–75％）",
    "Q4": "利用時間 最多（上位25％）",
}

QUARTILE_ORDER = ["Q1", "Q2", "Q3", "Q4"]

QUARTILE_COLORS = {
    "Q1": "#4c72b0",
    "Q2": "#dd8452",
    "Q3": "#55a868",
    "Q4": "#c44e52",
}

# --------------------------------------
# Core computation
# --------------------------------------
def compute_figures(conn, cohort_start_year, start_grade, subject_slug):

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

    rows = []

    for _, p in points.iterrows():
        y, rd = int(p.exam_year), int(p.exam_round)
        label = f"{y} R{rd}"
        start, end = prep_windows[(y, rd)]

        weeks = pd.date_range(start, end, freq="D").isocalendar().week.nunique()

        df_point = df_scores[
            (df_scores.exam_year == y) &
            (df_scores.exam_round == rd)
        ].copy()

        df_usage = query_usage_hours_by_student(
            conn,
            df_point.student_id.tolist(),
            start,
            end,
            subject_slug,
        )

        df = (
            df_point[["student_id", "score"]]
            .merge(df_usage, on="student_id", how="left")
        )

        # -----------------------------
        # Raw + capped usage
        # -----------------------------
        df["hours_raw"] = pd.to_numeric(df["hours_raw"], errors="coerce").fillna(0.0)


        # Conservative cap (30 min per event equivalent)
        # NOTE: # Raw vs capped usage (capping already applied per-event in SQL)
        df["hours_capped"] = pd.to_numeric(df["hours_capped"], errors="coerce").fillna(0.0)


        # Quartiles based on capped hours
        df["usage_quartile"] = assign_quartiles(df["hours_capped"])
        df = df.dropna(subset=["usage_quartile"])

        agg = (
            df.groupby("usage_quartile", as_index=False)
              .agg(
                  median_score=("score", "median"),
                  n_students=("student_id", "nunique"),
                  total_hours_raw=("hours_raw", "sum"),
                  total_hours_capped=("hours_capped", "sum"),
              )
        )

        agg["weeks"] = weeks

        agg["avg_weekly_hours_raw"] = agg.apply(
            lambda r: (r["total_hours_raw"] / (r["n_students"] * weeks))
            if r["n_students"] and weeks else 0.0,
            axis=1,
        )

        agg["avg_weekly_hours_capped"] = agg.apply(
            lambda r: (r["total_hours_capped"] / (r["n_students"] * weeks))
            if r["n_students"] and weeks else 0.0,
            axis=1,
        )

        for q in QUARTILE_ORDER:
            if q not in set(agg["usage_quartile"]):
                agg = pd.concat([
                    agg,
                    pd.DataFrame([{
                        "usage_quartile": q,
                        "median_score": float("nan"),
                        "n_students": 0,
                        "total_hours_raw": 0.0,
                        "total_hours_capped": 0.0,
                        "weeks": weeks,
                        "avg_weekly_hours_raw": 0.0,
                        "avg_weekly_hours_capped": 0.0,
                    }])
                ])

        for _, r in agg.iterrows():
            rows.append({
                "subject": subject_slug,
                "test_label": label,
                "usage_quartile": r["usage_quartile"],
                "usage_quartile_jp": QUARTILE_LABELS_JP[r["usage_quartile"]],
                "median_score": r["median_score"],
                "n_students": int(r["n_students"]),
                "weeks": int(r["weeks"]),
                "total_hours_raw": float(r["total_hours_raw"]),
                "total_hours_capped": float(r["total_hours_capped"]),
                "avg_weekly_hours_raw": float(r["avg_weekly_hours_raw"]),
                "avg_weekly_hours_capped": float(r["avg_weekly_hours_capped"]),
            })

    return pd.DataFrame(rows)

# --------------------------------------
# Seaborn table (heatmap-style)
# --------------------------------------
def plot_usage_table(df, subject, value_col, title_suffix, out_path):
    d = df[df["subject"] == subject].copy()
    table = (
        d.pivot(index="test_label", columns="usage_quartile", values=value_col)
         .reindex(columns=QUARTILE_ORDER)
         .round(2)
    )

    plt.figure(figsize=(10, max(3, 0.5 * len(table))))
    

    ax = sns.heatmap(
        table, annot=True, fmt=".2f", cmap="Blues",
        cbar_kws={"label": title_suffix},
        linewidths=0.5,
    )
    ax.set_title(f"{'数学' if subject=='math' else '英語'}：BookRoll利用時間（四分位）\n{title_suffix}", pad=12)
    ax.set_xlabel("利用時間 四分位")
    ax.set_ylabel("ベネッセ実施回")
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()


# --------------------------------------
# Main
# --------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cohort_start_year", type=int)
    parser.add_argument("start_grade", type=int)
    args = parser.parse_args()

    setup_japanese_font()
    sns.set_theme(style="white")
    setup_japanese_font()   # ← re-assert after seaborn
    conn = mysql.connector.connect(**DB_CONFIG)


    df_math = compute_figures(conn, args.cohort_start_year, args.start_grade, "math")
    df_eng  = compute_figures(conn, args.cohort_start_year, args.start_grade, "english")

    conn.close()

    df_all = pd.concat([df_math, df_eng], ignore_index=True)

    # ---------------- CSV ----------------
    out_csv = ROOT_DIR / f"math_english_{args.cohort_start_year}_bookroll_figures.csv"
    df_all.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"📊 Figures saved to: {out_csv.resolve()}")

    # ---------------- TABLE FIGURES ----------------
    plot_usage_table(df_all, "math", "avg_weekly_hours_capped", "時間／学生／週（diftime 30分上限）",
                 ROOT_DIR / f"math_usage_table_capped_{args.cohort_start_year}.png")
    plot_usage_table(df_all, "math", "avg_weekly_hours_raw", "時間／学生／週（生diftime・上限なし）",
                 ROOT_DIR / f"math_usage_table_raw_{args.cohort_start_year}.png")


    plot_usage_table(df_all, "english", "avg_weekly_hours_capped", "時間／学生／週（diftime 30分上限）",
                 ROOT_DIR / f"english_usage_table_capped_{args.cohort_start_year}.png")
    plot_usage_table(df_all, "english", "avg_weekly_hours_raw", "時間／学生／週（生diftime・上限なし）",
                 ROOT_DIR / f"english_usage_table_raw_{args.cohort_start_year}.png")


    print("📋 Usage tables saved (seaborn)")

if __name__ == "__main__":
    main()
