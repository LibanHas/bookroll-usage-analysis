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
# Quartile labels (JP, explicit)
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
# Core computation (subject-specific)
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

        df["hours"] = pd.to_numeric(df["hours"], errors="coerce").fillna(0.0)
        df["usage_quartile"] = assign_quartiles(df["hours"])
        df = df.dropna(subset=["usage_quartile"])

        agg = (
            df.groupby("usage_quartile", as_index=False)
              .agg(
                  score=("score", "median"),
                  n_students=("student_id", "nunique"),
                  total_hours=("hours", "sum"),
              )
        )

        agg["weeks"] = weeks
        agg["avg_weekly_hours"] = agg.apply(
            lambda r: (r["total_hours"] / (r["n_students"] * weeks))
            if r["n_students"] and weeks else 0.0,
            axis=1,
        )

        for q in QUARTILE_ORDER:
            if q not in set(agg["usage_quartile"]):
                agg = pd.concat([
                    agg,
                    pd.DataFrame([{
                        "usage_quartile": q,
                        "score": float("nan"),
                        "n_students": 0,
                        "total_hours": 0.0,
                        "weeks": weeks,
                        "avg_weekly_hours": 0.0,
                    }])
                ])

        for _, r in agg.iterrows():
            rows.append({
                "subject": subject_slug,
                "test_label": label,
                "usage_quartile": r["usage_quartile"],
                "usage_quartile_jp": QUARTILE_LABELS_JP[r["usage_quartile"]],
                "median_score": r["score"],
                "n_students": int(r["n_students"]),
                "total_hours": float(r["total_hours"]),
                "weeks": int(r["weeks"]),
                "avg_weekly_hours": float(r["avg_weekly_hours"]),
            })

    return pd.DataFrame(rows)


# --------------------------------------
# Main
# --------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cohort_start_year", type=int)
    parser.add_argument("start_grade", type=int)
    args = parser.parse_args()

    setup_japanese_font()
    conn = mysql.connector.connect(**DB_CONFIG)

    df_math = compute_figures(conn, args.cohort_start_year, args.start_grade, "math")
    df_eng  = compute_figures(conn, args.cohort_start_year, args.start_grade, "english")

    conn.close()

    df_all = pd.concat([df_math, df_eng], ignore_index=True)

    # ---------------- CSV (FIGURES) ----------------
    out_csv = ROOT_DIR / f"math_english_{args.cohort_start_year}_bookroll_figures.csv"
    df_all.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"📊 Figures saved to: {out_csv.resolve()}")

    # ---------------- PLOT ----------------
    order = sorted(set(df_math["test_label"]).intersection(df_eng["test_label"]))
    x_map = {lbl: i for i, lbl in enumerate(order)}

    fig, axes = plt.subplots(2, 1, figsize=(13, 8), sharex=True, sharey=True)

    for ax, (df, title) in zip(axes, [(df_math, "数学"), (df_eng, "英語")]):
        bar_width = 0.18
        offsets = {"Q1": -1.5, "Q2": -0.5, "Q3": 0.5, "Q4": 1.5}

        for q in QUARTILE_ORDER:
            d = df[df["usage_quartile"] == q].copy()
            d["x"] = d["test_label"].map(x_map)
            xs = [x + offsets[q] * bar_width for x in d["x"]]
            ax.bar(xs, d["median_score"], width=bar_width,
                   color=QUARTILE_COLORS[q],
                   label=QUARTILE_LABELS_JP[q] if ax is axes[0] else None)

        ax.set_title(title)
        ax.set_ylabel("ベネッセ得点（中央値）")

    axes[-1].set_xticks(range(len(order)))
    axes[-1].set_xticklabels(order, rotation=30, ha="right")
    axes[-1].set_xlabel("ベネッセ実施回（年 × 回）")

    fig.legend(
        title="BookRoll利用時間（授業時間外）",
        loc="lower center",
        ncol=2,
        frameon=True,
    )

    fig.suptitle(
        f"コホート {args.cohort_start_year}（開始学年：中学{args.start_grade}年）\n"
        "BookRoll利用時間（授業時間外）の四分位ごとのベネッセ得点：数学／英語",
        y=0.98
    )

    fig.tight_layout(rect=[0, 0.12, 1, 0.95])

    out_png = ROOT_DIR / f"math_vs_english_{args.cohort_start_year}_usage_quartile_scores.png"
    fig.savefig(out_png, dpi=200)
    print(f"📈 Plot saved to: {out_png.resolve()}")

    plt.show()


if __name__ == "__main__":
    main()
