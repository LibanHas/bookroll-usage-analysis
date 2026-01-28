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
)

# --------------------------------------
# Main
# --------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Inequality (Q4−Q1 of RAW scores) over time"
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
    conn.close()

    if df_scores.empty:
        raise RuntimeError("No score data found.")

    # --------------------------------------
    # Aggregate inequality per exam
    # --------------------------------------
    rows = []

    for (y, rd), g in df_scores.groupby(["exam_year", "exam_round"]):
        scores = g["score"].dropna()

        if len(scores) < 20:
            continue

        q1 = scores.quantile(0.25)
        q4 = scores.quantile(0.75)

        rows.append({
            "exam_year": y,
            "exam_round": rd,
            "label": f"{y} R{rd}",
            "iqr_raw": q4 - q1,
            "n": len(scores)
        })

    df_ineq = pd.DataFrame(rows)
    df_ineq = df_ineq.sort_values(["exam_year", "exam_round"])

    # --------------------------------------
    # Plot
    # --------------------------------------
    fig, ax = plt.subplots(figsize=(10, 5))

    ax.plot(
        df_ineq["label"],
        df_ineq["iqr_raw"],
        marker="o",
        linewidth=2
    )

    for i, r in df_ineq.iterrows():
        ax.text(
            r["label"],
            r["iqr_raw"],
            f"n={r['n']}",
            ha="center",
            va="bottom"
        )

    ax.set_ylabel("得点格差（Q4 − Q1, 生得点）")
    ax.set_xlabel("ベネッセ実施回（年 × 回）")

    ax.set_title(
        f"{subject_jp(subject_slug)} コホート {cohort_start_year}\n"
        "ベネッセ得点分布の推移（学力格差の拡大／縮小）"
    )

    fig.tight_layout()

    out = ROOT_DIR / f"{subject_slug}_{cohort_start_year}_raw_score_inequality.png"
    fig.savefig(out, dpi=200)
    print(f"📈 Saved: {out.resolve()}")
    plt.show()


if __name__ == "__main__":
    main()
