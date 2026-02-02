from pathlib import Path
from typing import Optional, Dict, Tuple
import sys
import argparse
import hashlib
import re

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
    # matches within name in course_student_scores
    if subject_slug == "math":
        return "%数学%"
    if subject_slug == "english":
        return "%英語%"
    raise ValueError("Subject must be Math/Maths or English")


def parse_round_from_name(name: str) -> int:
    m = ROUND_RE.search(str(name))
    return int(m.group(1)) if m else 1


def build_subject_filter_for_bookroll(subject_slug: str) -> str:
    # matches within course_title in artsci_bookroll_difftimes
    if subject_slug == "math":
        return "course_title LIKE '%数学%'"
    if subject_slug == "english":
        return "course_title LIKE '%英語%'"
    raise ValueError("Subject must be Math/Maths or English")


def weeks_in_range(start: pd.Timestamp, end: pd.Timestamp) -> int:
    # inclusive range for week counting
    return int(
        pd.date_range(start, end, freq="D")
        .isocalendar()
        .week
        .nunique()
    )


def assign_quartiles(scores: pd.Series) -> pd.Series:
    q25 = scores.quantile(0.25)
    q50 = scores.quantile(0.50)
    q75 = scores.quantile(0.75)

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

    return scores.apply(lab)


def chunked(seq, size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _hash_student_ids(student_ids) -> str:
    s = ",".join(map(str, sorted(map(int, student_ids))))
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:10]


def make_cache_path(
    cache_dir: Path,
    subject_slug: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    cohort_start_year: int,
    start_grade: int,
    exam_year: int,
    exam_round: int,
    student_ids_hash: str,
) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    key = (
        f"{subject_slug}|{start.date()}|{end.date()}|"
        f"cohort={cohort_start_year}|g={start_grade}|"
        f"{exam_year}|{exam_round}|ids={student_ids_hash}"
    )
    h = hashlib.md5(key.encode("utf-8")).hexdigest()[:12]
    return cache_dir / f"usage_by_student_{h}.csv.gz"


def coerce_student_id_int(df: pd.DataFrame, col: str = "student_id") -> pd.DataFrame:
    """
    Force student_id to int, dropping rows where it can't be converted.
    Prevents merge() failures due to object vs int dtype mismatches.
    """
    if col not in df.columns:
        raise KeyError(f"Missing required column: {col}")
    out = df.copy()
    out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=[col]).copy()
    out[col] = out[col].astype(int)
    return out


def query_usage_hours_by_student(
    conn,
    student_ids,
    start,
    end,
    subject_slug,
    cache_path: Optional[Path] = None,
):
    if not student_ids:
        return pd.DataFrame(columns=["student_id", "hours"])

    if cache_path is not None and cache_path.exists():
        df_cached = pd.read_csv(cache_path)
        df_cached = coerce_student_id_int(df_cached, "student_id")
        df_cached["hours"] = pd.to_numeric(
            df_cached["hours"], errors="coerce"
        ).fillna(0.0)
        return df_cached

    subject_filter = build_subject_filter_for_bookroll(subject_slug)
    all_rows = []

    # Half-open interval (>= start, < end+1day)
    start_dt = pd.Timestamp(start.date())
    end_exclusive = pd.Timestamp(end.date()) + pd.Timedelta(days=1)

    for ids_chunk in chunked(student_ids, 1000):
        id_list = ",".join(str(int(x)) for x in ids_chunk)

        query = f"""
            SELECT
                CAST(ssokid AS UNSIGNED) AS student_id,
                SUM(diftime) / 3600.0 AS hours
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

        df = pd.read_sql(query, conn)
        if not df.empty:
            df = coerce_student_id_int(df, "student_id")
            df["hours"] = pd.to_numeric(
                df["hours"], errors="coerce"
            ).fillna(0.0)
            all_rows.append(df)

    if not all_rows:
        out = pd.DataFrame(columns=["student_id", "hours"])
    else:
        out = (
            pd.concat(all_rows, ignore_index=True)
            .groupby("student_id", as_index=False)["hours"]
            .sum()
        )

    if cache_path is not None:
        out.to_csv(cache_path, index=False, compression="gzip")

    return out


# --------------------------------------
# Grade helpers
# --------------------------------------
def cohort_year_range(cohort_start_year: int, start_grade: int):
    last_jhs_year = cohort_start_year + (3 - start_grade)
    return cohort_start_year, last_jhs_year


def grade_number_for_year(
    cohort_start_year: int, start_grade: int, exam_year: int
) -> int:
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
def find_first_available_anchor(
    conn,
    cohort_start_year: int,
    start_grade: int,
    subject_slug: str,
):
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

        return {
            "exam_year": y,
            "exam_round": rd,
            "date_at": dt,
            "grade_num": g,
        }

    return None


def fetch_cohort_ids_from_db_auto_anchor(
    conn,
    cohort_start_year: int,
    start_grade: int,
    subject_slug: str,
):
    anchor = find_first_available_anchor(
        conn, cohort_start_year, start_grade, subject_slug
    )
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
    cohort = sorted(
        pd.to_numeric(df["student_id"], errors="coerce")
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )

    print(
        f"📍 Anchor auto-selected: {y} R{rd} grade={g} "
        f"date={anchor['date_at'].date()}"
    )
    print(
        f"👥 Cohort IDs from DB anchor: {y} R{rd} grade={g} "
        f"({subject_slug}) -> {len(cohort)} students"
    )

    return cohort, anchor


# --------------------------------------
# DB: Student-level Benesse scores
# --------------------------------------
def load_db_benesse_scores_student_level(
    conn,
    cohort_ids,
    cohort_start_year: int,
    start_grade: int,
    subject_slug: str,
) -> pd.DataFrame:
    """
    Student-level Benesse rows (DB-only), grade-restricted per year.
    Output columns:
      student_id, exam_year, exam_round, score, date_at
    """
    if not cohort_ids:
        return pd.DataFrame(
            columns=["student_id", "exam_year", "exam_round", "score", "date_at"]
        )

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
        return pd.DataFrame(
            columns=["student_id", "exam_year", "exam_round", "score", "date_at"]
        )

    df = coerce_student_id_int(df, "student_id")
    df["date_at"] = pd.to_datetime(df["date_at"], errors="coerce")
    df["score"] = pd.to_numeric(df["quiz"], errors="coerce")
    df = df.dropna(subset=["date_at", "score"]).copy()

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
        return pd.DataFrame(
            columns=["student_id", "exam_year", "exam_round", "score", "date_at"]
        )

    df = pd.concat(keep_rows, ignore_index=True)

    # One score per student per point (mean if duplicates)
    df = (
        df.groupby(["student_id", "exam_year", "exam_round"], as_index=False)
        .agg(score=("score", "mean"), date_at=("date_at", "min"))
    )
    df = coerce_student_id_int(df, "student_id")

    return df


def drop_first_if_spring(points: pd.DataFrame) -> pd.DataFrame:
    """
    Drop the earliest test point if it is spring (R1).
    """
    if points.empty:
        return points

    points = points.sort_values(
        ["exam_year", "exam_round"]
    ).reset_index(drop=True)

    if int(points.loc[0, "exam_round"]) == 1:
        y = int(points.loc[0, "exam_year"])
        print(
            f"⏭️ Dropping earliest Benesse point because it is spring (R1): "
            f"{y} R1"
        )
        return points.iloc[1:].reset_index(drop=True)

    return points


# --------------------------------------
# Between-tests window builder
# --------------------------------------
def build_prep_windows_between_tests(
    points_df: pd.DataFrame,
    first_window_start: pd.Timestamp,
) -> Dict[Tuple[int, int], Tuple[pd.Timestamp, pd.Timestamp, str]]:
    """
    points_df must contain: exam_year, exam_round, date_at
    Sorted ascending.

    Rule:
      - First point: [first_window_start .. (date_at - 1 day)]
      - Subsequent points: [(prev_date_at + 1 day) .. (date_at - 1 day)]
    """
    pts = (
        points_df.sort_values(["exam_year", "exam_round"])
        .reset_index(drop=True)
        .copy()
    )
    pts["date_at"] = pd.to_datetime(pts["date_at"], errors="coerce")
    pts = pts.dropna(subset=["date_at"]).copy()

    out: Dict[Tuple[int, int], Tuple[pd.Timestamp, pd.Timestamp, str]] = {}
    prev_dt: Optional[pd.Timestamp] = None

    for i, r in pts.iterrows():
        y = int(r["exam_year"])
        rd = int(r["exam_round"])
        dt = pd.Timestamp(r["date_at"]).normalize()

        if i == 0:
            start = pd.Timestamp(first_window_start.date())
            end = dt - pd.Timedelta(days=1)
        else:
            start = prev_dt + pd.Timedelta(days=1)
            end = dt - pd.Timedelta(days=1)

        label = f"{y} R{rd}"
        out[(y, rd)] = (start, end, label)
        prev_dt = dt

    return out


# --------------------------------------
# Main
# --------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description=(
            "Plot BookRoll usage by Benesse score quartiles "
            "(DB-only Benesse; grade-restricted; auto-anchor; "
            "windows from actual date_at)."
        )
    )
    parser.add_argument("cohort_start_year", type=int)
    parser.add_argument("start_grade", type=int)
    parser.add_argument("subject", type=str)
    parser.add_argument(
        "--no-drop-first-r1",
        action="store_true",
        default=False,
        help="Do NOT drop earliest R1 point (default is to drop earliest R1).",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        default=False,
        help="Disable DB caching for BookRoll usage queries.",
    )

    args = parser.parse_args()

    cohort_start_year = int(args.cohort_start_year)
    start_grade = int(args.start_grade)
    subject_slug = normalize_subject(args.subject)

    print(
        "🔥 RUNNING QUARTILE SCRIPT "
        "(DB-ONLY BENESSE; grade-restricted; "
        "auto-anchor; BETWEEN-TEST windows)"
    )
    print(
        f"Subject: {subject_slug}, "
        f"Cohort start: {cohort_start_year}, "
        f"Start grade: 中{start_grade}"
    )

    conn = mysql.connector.connect(**DB_CONFIG)

    # 1) Cohort IDs from DB anchor
    cohort_ids, anchor = fetch_cohort_ids_from_db_auto_anchor(
        conn, cohort_start_year, start_grade, subject_slug
    )
    if not cohort_ids:
        conn.close()
        raise RuntimeError(
            "No cohort IDs could be determined from DB "
            "(no Benesse anchor found)."
        )

    # 2) Student-level Benesse scores
    df_scores = load_db_benesse_scores_student_level(
        conn, cohort_ids, cohort_start_year, start_grade, subject_slug
    )
    if df_scores.empty:
        conn.close()
        raise RuntimeError(
            "No Benesse scores found in DB "
            "(after grade restriction)."
        )

    # Distinct exam points
    points_df = (
        df_scores.groupby(["exam_year", "exam_round"], as_index=False)
        .agg(date_at=("date_at", "min"))
        .sort_values(["exam_year", "exam_round"])
        .reset_index(drop=True)
    )

    if not args.no_drop_first_r1:
        points_df = drop_first_if_spring(points_df)

    if points_df.empty:
        conn.close()
        raise RuntimeError(
            "No exam points left after dropping earliest R1."
        )

    print("\n📊 Benesse test points found (DB-only):")
    for i, row in enumerate(points_df.itertuples(index=False), start=1):
        print(
            f" {i}. {int(row.exam_year)} "
            f"R{int(row.exam_round)} "
            f"date_at={pd.to_datetime(row.date_at).date()}"
        )

    first_window_start = pd.Timestamp(
        int(points_df.iloc[0]["exam_year"]), 4, 1
    )
    windows = build_prep_windows_between_tests(
        points_df, first_window_start
    )

    rows = []
    cache_dir = ROOT_DIR / "cache_bookroll_usage"
    use_cache = not args.no_cache

    for exam_year, exam_round, point_date in points_df[
        ["exam_year", "exam_round", "date_at"]
    ].itertuples(index=False, name=None):

        exam_year = int(exam_year)
        exam_round = int(exam_round)

        df_point = df_scores[
            (df_scores["exam_year"] == exam_year)
            & (df_scores["exam_round"] == exam_round)
        ].copy()

        df_point = coerce_student_id_int(df_point, "student_id")
        df_point["quartile"] = assign_quartiles(df_point["score"])
        df_point = df_point.dropna(subset=["quartile"]).copy()

        usage_start, usage_end, label = windows[(exam_year, exam_round)]
        if usage_end < usage_start:
            print(
                f"⚠️ Skipping {label}: "
                f"window end < start "
                f"({usage_start.date()} to {usage_end.date()})"
            )
            continue

        weeks = weeks_in_range(usage_start, usage_end)

        print(
            f"\n🧪 Test: {label} "
            f"(date_at={pd.to_datetime(point_date).date()})"
        )
        print(
            f" Prep window -> {usage_start.date()} "
            f"to {usage_end.date()} ({weeks} weeks)"
        )

        student_ids = df_point["student_id"].astype(int).tolist()
        ids_hash = _hash_student_ids(student_ids)

        cache_path = None
        if use_cache:
            cache_path = make_cache_path(
                cache_dir=cache_dir,
                subject_slug=subject_slug,
                start=usage_start,
                end=usage_end,
                cohort_start_year=cohort_start_year,
                start_grade=start_grade,
                exam_year=exam_year,
                exam_round=exam_round,
                student_ids_hash=ids_hash,
            )

        df_usage = query_usage_hours_by_student(
            conn=conn,
            student_ids=student_ids,
            start=usage_start,
            end=usage_end,
            subject_slug=subject_slug,
            cache_path=cache_path,
        )

        df_usage = coerce_student_id_int(df_usage, "student_id")
        # 🔎 DEBUG: confirm data exists before aggregation
        print("DEBUG df_point size:", len(df_point))
        print("DEBUG df_usage size:", len(df_usage))
        df_merged = df_point.merge(
            df_usage, on="student_id", how="left"
        )
        df_merged["hours"] = pd.to_numeric(
            df_merged["hours"], errors="coerce"
        ).fillna(0.0)

        agg = (
            df_merged.groupby("quartile", as_index=False)
            .agg(
                total_hours=("hours", "sum"),
                n_students=("student_id", "nunique"),
                n_with_usage=("has_usage", "sum"),
            )
        )


        for q in ["Q1", "Q2", "Q3", "Q4"]:
            if q not in set(agg["quartile"]):
                agg = pd.concat(
                    [
                        agg,
                        pd.DataFrame(
                            [{
                                "quartile": q,
                                "total_hours": 0.0,
                                "n_students": 0,
                            }]
                        ),
                    ],
                    ignore_index=True,
                )

        agg["avg_weekly_hours"] = agg.apply(
            lambda r: (
                r["total_hours"] / (r["n_students"] * weeks)
                if r["n_students"] and weeks
                else 0.0
            ),
            axis=1,
        )

         # ===============================
        # ✅ NEW: PRINT TABLE PER WINDOW
        # ===============================
        print("\n📋 Summary table for this prep window:")
        print(
            agg.sort_values("quartile")[
                ["quartile", "n_students", "total_hours", "avg_weekly_hours"]
            ].to_string(index=False)
        )
        # ===============================


        for _, r in agg.sort_values("quartile").iterrows():
            rows.append(
                {
                    "exam_year": exam_year,
                    "exam_round": exam_round,
                    "test_label": f"{exam_year} R{exam_round}",
                    "quartile": r["quartile"],
                    "n_students": int(r["n_students"]),
                    "total_hours": float(r["total_hours"]),
                    "weeks": int(weeks),
                    "avg_weekly_hours": float(r["avg_weekly_hours"]),
                }
            )

            print(
                f" {r['quartile']}: "
                f"n={int(r['n_students']):3d} "
                f"total_hours={float(r['total_hours']):10.2f} "
                f"avg_weekly={float(r['avg_weekly_hours']):.8f}"
            )


    conn.close()

    df_out = pd.DataFrame(rows)
    if df_out.empty:
        print("No results to plot.")
        return

    df_out["x_key"] = (
        df_out["exam_year"].astype(str)
        + "_R"
        + df_out["exam_round"].astype(str)
    )

    x_order = (
        df_out[["exam_year", "exam_round", "test_label", "x_key"]]
        .drop_duplicates()
        .sort_values(["exam_year", "exam_round"])
    )

    x_labels = x_order["test_label"].tolist()
    x_keys = x_order["x_key"].tolist()
    x_pos = {k: i for i, k in enumerate(x_keys)}

    fig, ax = plt.subplots(figsize=(12, 6))

    for q in ["Q1", "Q2", "Q3", "Q4"]:
        d = df_out[df_out["quartile"] == q].copy()
        d["x"] = d["x_key"].map(x_pos)
        d = d.sort_values("x")
        ax.plot(
            d["x"],
            d["avg_weekly_hours"],
            marker="o",
            linewidth=2.5,
            label=q,
        )

    ax.set_xticks(range(len(x_labels)))
    ax.set_xticklabels(x_labels, rotation=30, ha="right")
    ax.set_ylabel("Avg weekly BookRoll hours (out-of-school)")
    ax.set_xlabel("Benesse test point (calendar year × round)")
    ax.set_title(
        f"{subject_slug.capitalize()} cohort {cohort_start_year} "
        f"(start grade 中{start_grade})\n"
        "BookRoll usage by Benesse score quartile (Q1–Q4)\n"
        "(Benesse from DB only; grade-restricted; "
        "auto-anchor; prep windows from actual test dates)"
        + (
            ""
            if args.no_drop_first_r1
            else "\n(earliest R1 dropped)"
        )
    )

    ax.legend(title="Benesse quartile")
    fig.tight_layout()

    output_png = (
        ROOT_DIR
        / f"{subject_slug}_{cohort_start_year}"
        f"cohort_usage_by_quartile_DB_ONLY.png"
    )
    fig.savefig(output_png, dpi=200)

    print(f"\n📈 Plot saved to: {output_png.resolve()}")
    plt.show()


if __name__ == "__main__":
    main()
