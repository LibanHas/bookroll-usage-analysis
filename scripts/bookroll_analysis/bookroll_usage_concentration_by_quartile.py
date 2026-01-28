from pathlib import Path
import sys
import argparse
import re
from typing import Dict, Tuple, Optional, List
import importlib.util

import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector

# --------------------------------------
# Paths / imports (identical to your script)
# --------------------------------------
CURRENT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = CURRENT_DIR.parent
ROOT_DIR = SCRIPTS_DIR.parent

for p in (CURRENT_DIR, SCRIPTS_DIR, ROOT_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from db_config import DB_CONFIG

JP_FONT_PATH = SCRIPTS_DIR / "jp_font_setup.py"
spec = importlib.util.spec_from_file_location("jp_font_setup", str(JP_FONT_PATH))
jp_font_setup = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(jp_font_setup)
setup_japanese_font = jp_font_setup.setup_japanese_font

# --------------------------------------
# Helpers (reused)
# --------------------------------------
ROUND_RE = re.compile(r"第(\d+)回")

def normalize_subject(subject: str) -> str:
    s = subject.strip().lower()
    if s in ("math", "maths"):
        return "math"
    if s in ("english", "eng"):
        return "english"
    raise ValueError("Subject must be Math/Maths or English")

def subject_jp(subject_slug: str) -> str:
    return {"math": "数学", "english": "英語"}[subject_slug]

def parse_round_from_name(name: str) -> int:
    m = ROUND_RE.search(str(name))
    return int(m.group(1)) if m else 1

def coerce_student_id_int(df: pd.DataFrame, col="student_id") -> pd.DataFrame:
    df = df.copy()
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=[col])
    df[col] = df[col].astype(int)
    return df

def assign_quartiles(values: pd.Series) -> pd.Series:
    q25, q50, q75 = values.quantile([0.25, 0.50, 0.75])

    def lab(v):
        if v <= q25:
            return "Q1"
        if v <= q50:
            return "Q2"
        if v <= q75:
            return "Q3"
        return "Q4"

    return values.apply(lab)

# --------------------------------------
# Import shared DB logic from your script
# (kept verbatim for correctness)
# --------------------------------------
from plot_scores_by_usage_quartile import (
    fetch_cohort_ids_from_db_auto_anchor,
    load_db_benesse_scores_student_level,
    drop_first_if_spring,
    build_prep_windows_between_tests,
    query_usage_hours_by_student,
)

# --------------------------------------
# Main
# --------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="BookRoll利用時間の集中度（四分位別・総利用時間シェア）"
    )
    parser.add_argument("cohort_start_year", type=int)
    parser.add_argument("start_grade", type=int)
    parser.add_argument("subject", type=str)
    parser.add_argument("--keep-first-r1", action="store_true")
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

    if not args.keep_first_r1:
        points = drop_first_if_spring(points)

    prep_windows = build_prep_windows_between_tests(points, cohort_start_year)

    rows = []

    for _, p in points.iterrows():
        y, rd = int(p.exam_year), int(p.exam_round)
        label = f"{y} R{rd}"
        start, end = prep_windows[(y, rd)]

        df_point = df_scores[
            (df_scores.exam_year == y) &
            (df_scores.exam_round == rd)
        ].copy()

        df_usage = query_usage_hours_by_student(
            conn,
            df_point.student_id.tolist(),
            start,
            end,
            subject_slug
        )

        df = (
            df_point[["student_id"]]
            .merge(df_usage, on="student_id", how="left")
            .fillna({"hours": 0.0})
        )

        df["usage_quartile"] = assign_quartiles(df["hours"])

        total_hours = df["hours"].sum()

        for q, g in df.groupby("usage_quartile"):
            rows.append({
                "test_label": label,
                "usage_quartile": q,
                "share": g["hours"].sum() / total_hours if total_hours > 0 else 0
            })

    conn.close()

    df_out = pd.DataFrame(rows)

    # --------------------------------------
    # Plot: 100% stacked bars
    # --------------------------------------
    order = df_out["test_label"].drop_duplicates().tolist()
    x = range(len(order))

    fig, ax = plt.subplots(figsize=(12, 6))

    bottoms = [0.0] * len(order)
    colors = {"Q1": "#d9d9d9", "Q2": "#bdbdbd", "Q3": "#969696", "Q4": "#525252"}
    labels_jp = {
        "Q1": "下位25%",
        "Q2": "25–50%",
        "Q3": "50–75%",
        "Q4": "上位25%"
    }

    for q in ["Q1", "Q2", "Q3", "Q4"]:
        vals = (
            df_out[df_out.usage_quartile == q]
            .set_index("test_label")
            .reindex(order)["share"]
            .fillna(0)
            .tolist()
        )
        ax.bar(x, vals, bottom=bottoms, label=labels_jp[q], color=colors[q])
        bottoms = [b + v for b, v in zip(bottoms, vals)]

    ax.set_xticks(list(x))
    ax.set_xticklabels(order, rotation=30, ha="right")
    ax.set_ylabel("総利用時間に占める割合")
    ax.set_ylim(0, 1)

    ax.set_title(
        f"{subject_jp(subject_slug)} コホート {cohort_start_year}\n"
        "BookRoll利用時間の集中度（四分位別シェア）"
    )

    ax.legend(title="利用時間の四分位", loc="upper left")
    fig.tight_layout()

    out_png = ROOT_DIR / f"{subject_slug}_{cohort_start_year}_usage_concentration.png"
    fig.savefig(out_png, dpi=200)
    print(f"📊 Saved: {out_png.resolve()}")
    plt.show()


if __name__ == "__main__":
    main()
