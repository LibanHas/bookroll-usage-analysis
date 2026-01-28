from pathlib import Path
from typing import Optional, Dict, Tuple, List
import sys
import argparse
import hashlib
import re

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

ROUND_RE = re.compile(r"第(\d+)回")

# --------------------------------------
# Helpers
# --------------------------------------
def normalize_subject(subject: str) -> str:
    s = subject.strip().lower()
    if s in ("math", "maths"):
        return "math"
    if s in ("english", "eng"):
        return "english"
    raise ValueError("Subject must be Math/Maths or English")


def benesse_subject_like(subject_slug: str) -> str:
    return "%数学%" if subject_slug == "math" else "%英語%"


def parse_round_from_name(name: str) -> int:
    m = ROUND_RE.search(str(name))
    return int(m.group(1)) if m else 1


def build_subject_filter_for_bookroll(subject_slug: str) -> str:
    return "course_title LIKE '%数学%'" if subject_slug == "math" else "course_title LIKE '%英語%'"


def weeks_in_range(start: pd.Timestamp, end: pd.Timestamp) -> int:
    return int(pd.date_range(start, end, freq="D").isocalendar().week.nunique())


def assign_quartiles(values: pd.Series) -> pd.Series:
    q25, q50, q75 = values.quantile([0.25, 0.5, 0.75])

    def lab(v):
        if pd.isna(v):
            return None
        if v <= q25:
            return "Q1"
        if v <= q50:
            return "Q2"
        if v <= q75:
            return "Q3"
        return "Q4"

    return values.apply(lab)


def coerce_student_id_int(df: pd.DataFrame, col="student_id"):
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=[col])
    df[col] = df[col].astype(int)
    return df


def chunked(seq, size=1000):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]

# --------------------------------------
# BookRoll usage (RAW + CAPPED)
# --------------------------------------
def query_usage_hours_by_student(conn, student_ids, start, end, subject_slug):
    if not student_ids:
        return pd.DataFrame(columns=["student_id", "hours_raw", "hours_capped"])

    subject_filter = build_subject_filter_for_bookroll(subject_slug)
    rows = []

    start_dt = pd.Timestamp(start.date())
    end_exclusive = pd.Timestamp(end.date()) + pd.Timedelta(days=1)

    for ids in chunked(student_ids):
        id_list = ",".join(map(str, ids))

        q = f"""
            SELECT
                CAST(ssokid AS UNSIGNED) AS student_id,
                SUM(diftime) / 3600.0 AS hours_raw,
                SUM(LEAST(diftime, 1800)) / 3600.0 AS hours_capped
            FROM artsci_bookroll_difftimes
            WHERE
                operationdate >= '{start_dt}'
                AND operationdate < '{end_exclusive}'
                AND ssokid IN ({id_list})
                AND diftime > 0
                AND {subject_filter}
                AND NOT (
                    DAYOFWEEK(operationdate) BETWEEN 2 AND 6
                    AND TIME(operationdate) >= '08:00:00'
                    AND TIME(operationdate) < '16:00:00'
                )
            GROUP BY ssokid
        """
        df = pd.read_sql(q, conn)
        if not df.empty:
            rows.append(df)

    if not rows:
        return pd.DataFrame(columns=["student_id", "hours_raw", "hours_capped"])

    out = pd.concat(rows, ignore_index=True)
    out = coerce_student_id_int(out)
    out[["hours_raw", "hours_capped"]] = out[["hours_raw", "hours_capped"]].fillna(0.0)

    return out.groupby("student_id", as_index=False).sum()

# --------------------------------------
# Plot helper (RAW / CAPPED)
# --------------------------------------
def plot_usage_by_quartile(df_all, value_col, ylabel, title_suffix, out_png):
    subjects = ["math", "english"]
    fig, axes = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

    for ax, subject in zip(axes, subjects):
        d = df_all[df_all["subject"] == subject].copy()
        order = (
            d[["exam_year", "exam_round", "test_label"]]
            .drop_duplicates()
            .sort_values(["exam_year", "exam_round"])
        )
        x_labels = order["test_label"].tolist()
        x_map = {lbl: i for i, lbl in enumerate(x_labels)}

        for q, offset in zip(["Q1", "Q2", "Q3", "Q4"], [-1.5, -0.5, 0.5, 1.5]):
            dq = d[d["quartile"] == q].copy()
            dq["x"] = dq["test_label"].map(x_map)
            xs = dq["x"] + offset * 0.18
            ax.bar(xs, dq[value_col], width=0.18, label=q)

        ax.set_title("数学" if subject == "math" else "英語")
        ax.set_ylabel(ylabel)
        ax.set_xticks(range(len(x_labels)))
        ax.set_xticklabels(x_labels, rotation=30, ha="right")

    axes[0].legend(title="利用時間 四分位", ncol=4, loc="upper center")
    fig.suptitle(title_suffix, y=0.98)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(out_png, dpi=200)
    print(f"📈 Saved: {out_png}")

# --------------------------------------
# Main
# --------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cohort_start_year", type=int)
    parser.add_argument("start_grade", type=int)
    args = parser.parse_args()

    conn = mysql.connector.connect(**DB_CONFIG)

    rows = []

    for subject in ["math", "english"]:
        # --- cohort + scores ---
        df_scores = pd.read_sql(
            f"""
            SELECT student_id, date_at, name, quiz
            FROM course_student_scores
            WHERE name LIKE '{benesse_subject_like(subject)}'
              AND quiz IS NOT NULL AND quiz > 0
            """,
            conn,
        )

        df_scores["exam_year"] = pd.to_datetime(df_scores["date_at"]).dt.year
        df_scores["exam_round"] = df_scores["name"].apply(parse_round_from_name)
        df_scores["score"] = pd.to_numeric(df_scores["quiz"], errors="coerce")
        df_scores = coerce_student_id_int(df_scores)

        points = (
            df_scores.groupby(["exam_year", "exam_round"], as_index=False)
            .agg(date_at=("date_at", "min"))
            .sort_values(["exam_year", "exam_round"])
        )

        # drop first R1
        if points.iloc[0]["exam_round"] == 1:
            points = points.iloc[1:].reset_index(drop=True)

        prev_dt = None
        for _, p in points.iterrows():
            start = (
                pd.Timestamp(int(p.exam_year), 4, 1)
                if prev_dt is None
                else prev_dt + pd.Timedelta(days=1)
            )
            end = pd.to_datetime(p.date_at) - pd.Timedelta(days=1)
            prev_dt = pd.to_datetime(p.date_at)

            df_point = df_scores[
                (df_scores.exam_year == p.exam_year)
                & (df_scores.exam_round == p.exam_round)
            ].copy()

            df_point["quartile"] = assign_quartiles(df_point["score"])
            df_point = df_point.dropna(subset=["quartile"])

            usage = query_usage_hours_by_student(
                conn,
                df_point.student_id.tolist(),
                start,
                end,
                subject,
            )

            df = df_point.merge(usage, on="student_id", how="left").fillna(0.0)
            weeks = weeks_in_range(start, end)

            agg = (
                df.groupby("quartile", as_index=False)
                .agg(
                    avg_weekly_hours_raw=("hours_raw", lambda x: x.sum() / (len(x) * weeks)),
                    avg_weekly_hours_capped=("hours_capped", lambda x: x.sum() / (len(x) * weeks)),
                )
            )

            for _, r in agg.iterrows():
                rows.append({
                    "subject": subject,
                    "exam_year": p.exam_year,
                    "exam_round": p.exam_round,
                    "test_label": f"{p.exam_year} R{p.exam_round}",
                    "quartile": r["quartile"],
                    "avg_weekly_hours_raw": r["avg_weekly_hours_raw"],
                    "avg_weekly_hours_capped": r["avg_weekly_hours_capped"],
                })

    conn.close()

    df_all = pd.DataFrame(rows)

    # --- RAW PNG ---
    plot_usage_by_quartile(
        df_all,
        "avg_weekly_hours_raw",
        "時間／学生／週（生 diftime）",
        "BookRoll 利用時間（RAW）",
        ROOT_DIR / f"bookroll_usage_RAW_{args.cohort_start_year}.png",
    )

    # --- CAPPED PNG ---
    plot_usage_by_quartile(
        df_all,
        "avg_weekly_hours_capped",
        "時間／学生／週（diftime 30分上限）",
        "BookRoll 利用時間（CAPPED）",
        ROOT_DIR / f"bookroll_usage_CAPPED_{args.cohort_start_year}.png",
    )

if __name__ == "__main__":
    main()
