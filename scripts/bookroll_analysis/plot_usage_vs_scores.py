from pathlib import Path
import sys
import argparse
import re
from typing import Dict, Tuple, Optional

import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector

# --------------------------------------
# Ensure we can import db_config
# --------------------------------------
CURRENT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = CURRENT_DIR.parent
ROOT_DIR = SCRIPTS_DIR.parent

for p in (CURRENT_DIR, SCRIPTS_DIR, ROOT_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from db_config import DB_CONFIG

# --------------------------------------
# Helpers
# --------------------------------------
ROUND_RE = re.compile(r"第(\d+)回")

def normalize_subject(subject: str) -> str:
    s = subject.strip().lower()
    if s in ("math", "maths"):
        return "math"
    if s in ("english", "eng"):
        return "english"
    raise ValueError("Subject must be Math/Maths or English")


def benesse_subject_like(subject_slug: str) -> str:
    # matches within `name` in course_student_scores
    if subject_slug == "math":
        return "%数学%"
    if subject_slug == "english":
        return "%英語%"
    raise ValueError("Subject must be Math/Maths or English")


def parse_round_from_name(name: str) -> int:
    m = ROUND_RE.search(str(name))
    return int(m.group(1)) if m else 1


def build_subject_filter_for_bookroll(subject_slug: str) -> str:
    # matches within `course_title` in artsci_bookroll_difftimes
    if subject_slug == "math":
        return "course_title LIKE '%数学%'"
    if subject_slug == "english":
        return "course_title LIKE '%英語%'"
    raise ValueError("Subject must be Math/Maths or English")


def weeks_in_range(start: pd.Timestamp, end: pd.Timestamp) -> int:
    return int(pd.date_range(start, end, freq="D").isocalendar().week.nunique())


def query_window_usage_by_cohort(conn, cohort_ids, start, end, subject_slug):
    if not cohort_ids:
        return 0.0, 0

    subject_filter = build_subject_filter_for_bookroll(subject_slug)
    id_list = ",".join(str(int(x)) for x in cohort_ids)

    query = f"""
        SELECT
            SUM(diftime) / 3600.0 AS total_hours,
            COUNT(DISTINCT ssokid) AS student_count
        FROM artsci_bookroll_difftimes
        WHERE
            DATE(operationdate) BETWEEN '{start.date()}' AND '{end.date()}'
            AND ssokid IN ({id_list})
            AND diftime > 0
            AND {subject_filter}
            AND NOT (
                DAYOFWEEK(operationdate) BETWEEN 2 AND 6
                AND TIME(operationdate) >= '08:00:00'
                AND TIME(operationdate) <  '16:00:00'
            )
    """
    df = pd.read_sql(query, conn)
    if df.empty:
        return 0.0, 0

    return float(df["total_hours"].fillna(0).iloc[0]), int(df["student_count"].fillna(0).iloc[0])


# --------------------------------------
# Window builder (BookRoll bars)
# --------------------------------------
def build_usage_windows(cohort_start_year, start_grade):
    """
    Builds BookRoll usage windows:
      cohort_start_year Apr–Oct,
      then alternating Nov–Mar and Apr–Oct through JHS,
      and one final Nov–Mar after JHS ends.
    """
    windows = []

    last_jhs_year = cohort_start_year + (3 - start_grade)
    final_year = last_jhs_year + 1

    windows.append((
        pd.Timestamp(cohort_start_year, 4, 1),
        pd.Timestamp(cohort_start_year, 10, 31),
        f"{cohort_start_year} Apr–Oct"
    ))

    for y in range(cohort_start_year + 1, final_year + 1):
        windows.append((
            pd.Timestamp(y - 1, 11, 1),
            pd.Timestamp(y, 3, 31),
            f"{y-1} Nov–{y} Mar"
        ))
        if y <= last_jhs_year:
            windows.append((
                pd.Timestamp(y, 4, 1),
                pd.Timestamp(y, 10, 31),
                f"{y} Apr–Oct"
            ))

    return windows


def find_bar_index_containing_window(df_windows: pd.DataFrame, usage_start: pd.Timestamp, usage_end: pd.Timestamp):
    m = (df_windows["start"] <= usage_start) & (df_windows["end"] >= usage_end)
    idxs = df_windows.index[m].tolist()
    return idxs[0] if idxs else None


# NEW: bar lookup by a single date (test date)
def find_bar_index_containing_date(df_windows: pd.DataFrame, dt: pd.Timestamp):
    dt = pd.Timestamp(dt).normalize()
    m = (df_windows["start"] <= dt) & (df_windows["end"] >= dt)
    idxs = df_windows.index[m].tolist()
    return idxs[0] if idxs else None


# --------------------------------------
# DB: grade helpers + cohort years
# --------------------------------------
def cohort_year_range(cohort_start_year: int, start_grade: int):
    last_jhs_year = cohort_start_year + (3 - start_grade)
    return cohort_start_year, last_jhs_year


def grade_number_for_year(cohort_start_year: int, start_grade: int, exam_year: int) -> int:
    """Returns 1/2/3 for 中1/中2/中3 based on the cohort definition."""
    return int(start_grade + (exam_year - cohort_start_year))


def course_name_grade_clause(grade_num: int) -> str:
    g = int(grade_num)
    return (
        f"(course_name LIKE '%[中{g}]%' "
        f" OR course_name LIKE '%{g}年%[中学]%' "
        f" OR course_name LIKE '%中学{g}年%')"
    )


# --------------------------------------
# DB: Auto-anchor
# --------------------------------------
def find_first_available_anchor(conn, cohort_start_year: int, start_grade: int, subject_slug: str):
    subj_like = benesse_subject_like(subject_slug)

    for y in range(cohort_start_year, cohort_start_year + 5):
        g = grade_number_for_year(cohort_start_year, start_grade, y)
        if g < 1 or g > 3:
            continue
        grade_clause = course_name_grade_clause(g)

        q = f"""
            SELECT date_at, name
            FROM course_student_scores
            WHERE date_at IS NOT NULL
              AND YEAR(date_at) = {y}
              AND (name LIKE '%Benesse%' OR name LIKE '%ベネッセ%')
              AND name LIKE '{subj_like}'
              AND {grade_clause}
              AND quiz IS NOT NULL
              AND quiz > 0
            ORDER BY date_at ASC
            LIMIT 1
        """
        df = pd.read_sql(q, conn)
        if df.empty:
            continue

        dt = pd.to_datetime(df.loc[0, "date_at"], errors="coerce")
        nm = str(df.loc[0, "name"])
        if pd.isna(dt):
            continue

        rd = parse_round_from_name(nm)
        if rd not in (1, 2):
            continue

        return {"exam_year": y, "exam_round": rd, "date_at": dt, "grade_num": g}

    return None


def fetch_cohort_ids_from_db_auto_anchor(conn, cohort_start_year: int, start_grade: int, subject_slug: str):
    anchor = find_first_available_anchor(conn, cohort_start_year, start_grade, subject_slug)
    if anchor is None:
        print("👥 Cohort IDs: no anchor found.")
        return [], None

    subj_like = benesse_subject_like(subject_slug)
    y = int(anchor["exam_year"])
    rd = int(anchor["exam_round"])
    g = int(anchor["grade_num"])
    grade_clause = course_name_grade_clause(g)

    q_ids = f"""
        SELECT DISTINCT student_id
        FROM course_student_scores
        WHERE date_at IS NOT NULL
          AND YEAR(date_at) = {y}
          AND (name LIKE '%Benesse%' OR name LIKE '%ベネッセ%')
          AND name LIKE '{subj_like}'
          AND name LIKE '%第{rd}回%'
          AND {grade_clause}
          AND quiz IS NOT NULL
          AND quiz > 0
    """
    df = pd.read_sql(q_ids, conn)
    cohort = sorted(pd.to_numeric(df["student_id"], errors="coerce").dropna().astype(int).unique().tolist())

    print(f"📍 Anchor auto-selected: {y} R{rd} grade={g} date={anchor['date_at'].date()}")
    print(f"👥 Cohort IDs from DB anchor: {y} R{rd} grade={g} ({subject_slug}) -> {len(cohort)} students")

    return cohort, anchor


# --------------------------------------
# DB: Benesse points (DB-only, grade-restricted)
# --------------------------------------
def load_db_benesse_points_db_only(conn, cohort_ids, cohort_start_year: int, start_grade: int, subject_slug: str) -> pd.DataFrame:
    if not cohort_ids:
        return pd.DataFrame(columns=["exam_year", "exam_round", "median_score", "date_at", "n_students"])

    subj_like = benesse_subject_like(subject_slug)
    y0, y_last = cohort_year_range(cohort_start_year, start_grade)
    id_list = ",".join(str(int(x)) for x in cohort_ids)

    q = f"""
        SELECT student_id, date_at, name, quiz, course_name
        FROM course_student_scores
        WHERE student_id IN ({id_list})
          AND date_at IS NOT NULL
          AND YEAR(date_at) BETWEEN {y0} AND {y_last}
          AND (name LIKE '%Benesse%' OR name LIKE '%ベネッセ%')
          AND name LIKE '{subj_like}'
          AND quiz IS NOT NULL
          AND quiz > 0
    """
    df = pd.read_sql(q, conn)
    if df.empty:
        return pd.DataFrame(columns=["exam_year", "exam_round", "median_score", "date_at", "n_students"])

    df["date_at"] = pd.to_datetime(df["date_at"], errors="coerce")
    df["quiz"] = pd.to_numeric(df["quiz"], errors="coerce")
    df = df.dropna(subset=["date_at", "quiz"]).copy()

    df["exam_year"] = df["date_at"].dt.year.astype(int)
    df["exam_round"] = df["name"].apply(parse_round_from_name).astype(int)
    df = df[df["exam_round"].isin([1, 2])].copy()

    # Grade restriction per exam_year
    keep_rows = []
    for y in range(y0, y_last + 1):
        g = grade_number_for_year(cohort_start_year, start_grade, y)
        s = df[df["exam_year"] == y].copy()
        if s.empty:
            continue
        pat = re.compile(rf"\[中{g}\]|{g}年.*\[中学\]|中学{g}年")
        s = s[s["course_name"].astype(str).str.contains(pat)]
        keep_rows.append(s)

    if not keep_rows:
        return pd.DataFrame(columns=["exam_year", "exam_round", "median_score", "date_at", "n_students"])

    df = pd.concat(keep_rows, ignore_index=True)

    out = (
        df.groupby(["exam_year", "exam_round"], as_index=False)
          .agg(
              median_score=("quiz", "median"),
              date_at=("date_at", "min"),  # earliest test date for this point
              n_students=("student_id", "nunique"),
          )
          .sort_values(["exam_year", "exam_round"])
          .reset_index(drop=True)
    )
    return out


def drop_first_if_spring(scores_by_exam: pd.DataFrame) -> pd.DataFrame:
    if scores_by_exam.empty:
        return scores_by_exam
    scores_by_exam = scores_by_exam.sort_values(["exam_year", "exam_round"]).reset_index(drop=True)
    if int(scores_by_exam.loc[0, "exam_round"]) == 1:
        y = int(scores_by_exam.loc[0, "exam_year"])
        print(f"⏭️  Dropping earliest Benesse point because it is spring (R1): {y} R1")
        return scores_by_exam.iloc[1:].reset_index(drop=True)
    return scores_by_exam


# --------------------------------------
# NEW: Between-tests windows using actual date_at
# --------------------------------------
def build_prep_windows_between_tests(
    scores_by_exam: pd.DataFrame,
) -> Dict[Tuple[int, int], Tuple[pd.Timestamp, pd.Timestamp, str]]:
    """
    Input must contain one row per exam point with columns:
      exam_year, exam_round, date_at

    Rule:
      - First kept point: Apr 1 of that exam_year -> (date_at - 1 day)
      - Subsequent points: (prev date_at + 1 day) -> (date_at - 1 day)
    """
    s = scores_by_exam.sort_values(["exam_year", "exam_round"]).reset_index(drop=True).copy()
    s["date_at"] = pd.to_datetime(s["date_at"], errors="coerce")
    s = s.dropna(subset=["date_at"]).copy()

    out: Dict[Tuple[int, int], Tuple[pd.Timestamp, pd.Timestamp, str]] = {}
    prev_dt: Optional[pd.Timestamp] = None

    for i, r in s.iterrows():
        y = int(r["exam_year"])
        rd = int(r["exam_round"])
        dt = pd.Timestamp(r["date_at"]).normalize()

        if i == 0:
            start = pd.Timestamp(y, 4, 1)
            end = dt - pd.Timedelta(days=1)
        else:
            start = prev_dt + pd.Timedelta(days=1)
            end = dt - pd.Timedelta(days=1)

        out[(y, rd)] = (start, end, f"{y} R{rd}")
        prev_dt = dt

    return out


# --------------------------------------
# Main
# --------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cohort_start_year", type=int)
    parser.add_argument("start_grade", type=int)
    parser.add_argument("subject", type=str)

    args = parser.parse_args()
    cohort_start_year = args.cohort_start_year
    start_grade = args.start_grade
    subject_slug = normalize_subject(args.subject)

    print("🔥 RUNNING DB-ONLY SCRIPT (Benesse from DB only, grade-restricted, auto-anchor; BETWEEN-TEST windows)")

    conn = mysql.connector.connect(**DB_CONFIG)

    # 1) Cohort IDs from DB (auto-selected anchor that exists)
    cohort_ids, anchor = fetch_cohort_ids_from_db_auto_anchor(conn, cohort_start_year, start_grade, subject_slug)
    if not cohort_ids:
        conn.close()
        raise RuntimeError("No cohort IDs could be determined from DB (no Benesse data found for any anchor).")

    # 2) Benesse points from DB (restricted to cohort and correct grade per year)
    scores_by_exam = load_db_benesse_points_db_only(conn, cohort_ids, cohort_start_year, start_grade, subject_slug)
    if scores_by_exam.empty:
        conn.close()
        raise RuntimeError("No Benesse score points found for this cohort in DB (after grade restriction).")

    scores_by_exam = drop_first_if_spring(scores_by_exam)
    if scores_by_exam.empty:
        conn.close()
        raise RuntimeError("All Benesse points were dropped (unexpected).")

    print("📌 Benesse points (DB-only, cohort+grade restricted):")
    print(scores_by_exam[["exam_year", "exam_round", "median_score", "date_at", "n_students"]].to_string(index=False))

    # Build prep windows (actual date_at; between tests; first = Apr 1 .. test-1)
    prep_windows = build_prep_windows_between_tests(scores_by_exam)

    # 3) BookRoll windows + usage bars (UNCHANGED bars)
    windows = build_usage_windows(cohort_start_year, start_grade)
    records = []
    for start, end, label in windows:
        total, n_students = query_window_usage_by_cohort(conn, cohort_ids, start, end, subject_slug)
        weeks = weeks_in_range(start, end)
        avg = total / (n_students * weeks) if n_students and weeks else 0.0
        records.append({"label": label, "start": start, "end": end, "avg_weekly_hours": avg})

    conn.close()
    df_windows = pd.DataFrame(records)

    # --------------------------------------
    # Plot
    # --------------------------------------
    fig, ax1 = plt.subplots(figsize=(11, 6))

    x = list(range(len(df_windows)))
    ax1.bar(x, df_windows["avg_weekly_hours"], color="C0")
    ax1.set_xticks(x)
    ax1.set_xticklabels(df_windows["label"], rotation=30, ha="right")
    ax1.set_ylabel("Avg weekly BookRoll hours")

    # Benesse points placed by TEST DATE bar (R1 stays; R2 shifts to next window)
    ax2 = ax1.twinx()
    xs, ys, labels = [], [], []

    for _, r in scores_by_exam.iterrows():
        y = int(r["exam_year"])
        rd = int(r["exam_round"])
        dt = pd.to_datetime(r["date_at"], errors="coerce")
        n = int(r["n_students"]) if pd.notna(r["n_students"]) else None

        if pd.isna(dt):
            continue

        i_date = find_bar_index_containing_date(df_windows, dt)
        if i_date is None:
            continue

        # IMPORTANT FIX:
        # - R1 (Apr-ish) should be in the Apr–Oct window (same bar)
        # - R2 (Sep/Oct) should be in the subsequent Nov–Mar window (+1)
        i_plot = i_date + (1 if rd == 2 else 0)
        if i_plot >= len(df_windows):
            continue

        xs.append(i_plot)
        ys.append(float(r["median_score"]))
        labels.append(f"Benesse {y} R{rd} (n={n}) {dt.date()}")

        print(f"📍 {y} R{rd}: test_date={dt.date()} -> bar={i_date} plotted_at={i_plot}")

    ax2.plot(xs, ys, marker="o", linewidth=2.5, color="C1", markersize=7, zorder=5)
    ax2.set_ylabel("Median Benesse score (quiz)")

    for x_, y_, lbl in zip(xs, ys, labels):
        ax2.annotate(
            lbl,
            (x_, y_),
            xytext=(0, 8),
            textcoords="offset points",
            ha="center",
            fontsize=8,
            color="black",
        )

    ax1.set_title(
        f"{subject_slug.capitalize()} cohort {cohort_start_year} (start grade {start_grade})\n"
        "BookRoll usage vs Benesse scores (DB-only; grade-restricted; auto-anchor)\n"
        "(R1 plotted in same Apr–Oct window; R2 plotted in subsequent Nov–Mar window)"
    )

    fig.tight_layout()
    output_png = ROOT_DIR / f"{subject_slug}_{cohort_start_year}cohort_usage_vs_scores_db_only_grade_restricted.png"
    fig.savefig(output_png, dpi=200)
    print(f"📈 Plot saved to: {output_png.resolve()}")
    plt.show()


if __name__ == "__main__":
    main()
