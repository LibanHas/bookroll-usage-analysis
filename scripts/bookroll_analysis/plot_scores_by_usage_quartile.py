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
# Paths / imports
# --------------------------------------
CURRENT_DIR = Path(__file__).resolve().parent          # .../scripts/bookroll_analysis
SCRIPTS_DIR = CURRENT_DIR.parent                      # .../scripts
ROOT_DIR = SCRIPTS_DIR.parent                         # .../ (project root)

for p in (CURRENT_DIR, SCRIPTS_DIR, ROOT_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from db_config import DB_CONFIG

# Load jp_font_setup.py from scripts/ even when running from scripts/bookroll_analysis/
JP_FONT_PATH = SCRIPTS_DIR / "jp_font_setup.py"
spec = importlib.util.spec_from_file_location("jp_font_setup", str(JP_FONT_PATH))
jp_font_setup = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(jp_font_setup)
setup_japanese_font = jp_font_setup.setup_japanese_font

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


def subject_jp(subject_slug: str) -> str:
    return {"math": "数学", "english": "英語"}[subject_slug]


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


def coerce_student_id_int(df: pd.DataFrame, col: str = "student_id") -> pd.DataFrame:
    if col not in df.columns:
        raise KeyError(f"Missing required column: {col}")
    out = df.copy()
    out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=[col]).copy()
    out[col] = out[col].astype(int)
    return out


def assign_quartiles(values: pd.Series) -> pd.Series:
    q25 = values.quantile(0.25)
    q50 = values.quantile(0.50)
    q75 = values.quantile(0.75)

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


def chunked(seq, size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def query_usage_hours_by_student(conn, student_ids, start, end, subject_slug):
    """
    Return raw and capped BookRoll usage hours per student.

    - hours_raw: SUM(diftime)
    - hours_capped: SUM(LEAST(diftime, 1800))  # cap each event at 30 minutes

    Time window: [start, end] inclusive (implemented as half-open).
    """
    if not student_ids:
        return pd.DataFrame(columns=["student_id", "hours_raw", "hours_capped"])

    subject_filter = build_subject_filter_for_bookroll(subject_slug)
    all_rows = []

    start_dt = pd.Timestamp(start.date())
    end_exclusive = pd.Timestamp(end.date()) + pd.Timedelta(days=1)

    for ids_chunk in chunked(student_ids, 1000):
        id_list = ",".join(str(int(x)) for x in ids_chunk)

        q = f"""
            SELECT
                CAST(ssokid AS UNSIGNED) AS student_id,
                SUM(diftime) / 3600.0 AS hours_raw,
                SUM(LEAST(diftime, 1800)) / 3600.0 AS hours_capped
            FROM artsci_bookroll_difftimes
            WHERE
                operationdate >= '{start_dt}'
                AND operationdate <  '{end_exclusive}'
                AND ssokid IN ({id_list})
                AND diftime > 0
                AND {subject_filter}
                AND NOT (
                    DAYOFWEEK(operationdate) BETWEEN 2 AND 6
                    AND TIME(operationdate) >= '08:00:00'
                    AND TIME(operationdate) <  '16:00:00'
                )
            GROUP BY ssokid
        """

        df = pd.read_sql(q, conn)
        if not df.empty:
            df = coerce_student_id_int(df, "student_id")
            df["hours_raw"] = pd.to_numeric(df["hours_raw"], errors="coerce").fillna(0.0)
            df["hours_capped"] = pd.to_numeric(df["hours_capped"], errors="coerce").fillna(0.0)
            all_rows.append(df)

    if not all_rows:
        return pd.DataFrame(columns=["student_id", "hours_raw", "hours_capped"])

    out = pd.concat(all_rows, ignore_index=True)

    # Safety: if a student appears in multiple chunks (shouldn’t, but be robust)
    out = (
        out.groupby("student_id", as_index=False)
           .agg(hours_raw=("hours_raw", "sum"),
                hours_capped=("hours_capped", "sum"))
    )

    return out



# --------------------------------------
# Grade helpers
# --------------------------------------
def cohort_year_range(cohort_start_year: int, start_grade: int):
    last_jhs_year = cohort_start_year + (3 - start_grade)
    return cohort_start_year, last_jhs_year


def grade_number_for_year(cohort_start_year: int, start_grade: int, exam_year: int) -> int:
    return int(start_grade + (exam_year - cohort_start_year))


def course_name_grade_clause(grade_num: int) -> str:
    g = int(grade_num)
    return (
        f"(course_name LIKE '%[中{g}]%' "
        f" OR course_name LIKE '%{g}年%[中学]%' "
        f" OR course_name LIKE '%中学{g}年%')"
    )


# --------------------------------------
# DB: Auto-anchor -> cohort IDs
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
# DB: Student-level Benesse scores (grade-restricted)
# --------------------------------------
def load_db_benesse_scores_student_level(conn, cohort_ids, cohort_start_year: int, start_grade: int, subject_slug: str) -> pd.DataFrame:
    if not cohort_ids:
        return pd.DataFrame(columns=["student_id", "exam_year", "exam_round", "date_at", "score"])

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
        return pd.DataFrame(columns=["student_id", "exam_year", "exam_round", "date_at", "score"])

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
        return pd.DataFrame(columns=["student_id", "exam_year", "exam_round", "date_at", "score"])

    df = pd.concat(keep_rows, ignore_index=True)

    # One score per student per point (mean if duplicates)
    df = (
        df.groupby(["student_id", "exam_year", "exam_round"], as_index=False)
          .agg(score=("score", "mean"), date_at=("date_at", "min"))
    )
    df = coerce_student_id_int(df, "student_id")
    return df


def drop_first_if_spring(points: pd.DataFrame) -> pd.DataFrame:
    if points.empty:
        return points
    points = points.sort_values(["exam_year", "exam_round"]).reset_index(drop=True)
    if int(points.loc[0, "exam_round"]) == 1:
        y = int(points.loc[0, "exam_year"])
        print(f"⏭️  Dropping earliest Benesse point because it is spring (R1): {y} R1")
        return points.iloc[1:].reset_index(drop=True)
    return points


# --------------------------------------
# Prep windows: first=Apr1..test-1; then between-tests
# --------------------------------------

def build_prep_windows_between_tests(
    points: pd.DataFrame,
    cohort_start_year: int,
) -> Dict[Tuple[int, int], Tuple[pd.Timestamp, pd.Timestamp]]:
    """
    Rule:
      - First kept point: Apr 1 of cohort_start_year -> (date_at - 1 day)
      - Subsequent points: (prev date_at + 1 day) -> (date_at - 1 day)
    """
    s = points.sort_values(["exam_year", "exam_round"]).reset_index(drop=True).copy()
    s["date_at"] = pd.to_datetime(s["date_at"], errors="coerce")
    s = s.dropna(subset=["date_at"]).copy()

    out: Dict[Tuple[int, int], Tuple[pd.Timestamp, pd.Timestamp]] = {}
    prev_dt: Optional[pd.Timestamp] = None

    for i, r in s.iterrows():
        y = int(r["exam_year"])
        rd = int(r["exam_round"])
        dt = pd.Timestamp(r["date_at"]).normalize()

        if i == 0:
            start = pd.Timestamp(int(cohort_start_year), 4, 1)   # ✅ changed
            end = dt - pd.Timedelta(days=1)
        else:
            start = prev_dt + pd.Timedelta(days=1)
            end = dt - pd.Timedelta(days=1)

        if end < start:
            print(f"⚠️  Window end < start for {y} R{rd}: {start.date()} -> {end.date()}")

        out[(y, rd)] = (start, end)
        prev_dt = dt

    return out

# --------------------------------------
# Main
# --------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="棒グラフ：BookRoll利用時間の四分位ごとのベネッセ得点（準備期間：初回=4/1〜前日、その後=前回翌日〜前日）"
    )
    parser.add_argument("cohort_start_year", type=int)
    parser.add_argument("start_grade", type=int)
    parser.add_argument("subject", type=str)
    parser.add_argument("--keep-first-r1", action="store_true", help="最初のテストがR1でも削除しない")
    parser.add_argument("--score-stat", choices=["median", "mean"], default="median", help="四分位の代表値（中央値/平均）")
    args = parser.parse_args()

    cohort_start_year = int(args.cohort_start_year)
    start_grade = int(args.start_grade)
    subject_slug = normalize_subject(args.subject)

    # Japanese font before plotting
    setup_japanese_font()

    conn = mysql.connector.connect(**DB_CONFIG)

    cohort_ids, _anchor = fetch_cohort_ids_from_db_auto_anchor(conn, cohort_start_year, start_grade, subject_slug)
    if not cohort_ids:
        conn.close()
        raise RuntimeError("コホートIDが見つかりません（アンカーがありません）。")

    df_scores = load_db_benesse_scores_student_level(conn, cohort_ids, cohort_start_year, start_grade, subject_slug)
    if df_scores.empty:
        conn.close()
        raise RuntimeError("学年制約後のベネッセ得点が見つかりません。")

    points = (
        df_scores.groupby(["exam_year", "exam_round"], as_index=False)
        .agg(date_at=("date_at", "min"), n_students=("student_id", "nunique"))
        .sort_values(["exam_year", "exam_round"])
        .reset_index(drop=True)
    )

    if not args.keep_first_r1:
        points = drop_first_if_spring(points)

    if points.empty:
        conn.close()
        raise RuntimeError("プロット対象のテスト点がありません（R1削除後）。")

    print("\n📌 Points used for prep windows:")
    print(points[["exam_year","exam_round","date_at","n_students"]].to_string(index=False))

    prep_windows = build_prep_windows_between_tests(points, cohort_start_year)

    print("\n🪟 Prep windows:")
    for (y, rd), (st, en) in prep_windows.items():
        print(f"{y} R{rd}: {st.date()} -> {en.date()}")

    legend_map = {
        "Q1": "利用時間 下位25%（最少）",
        "Q2": "利用時間 25–50%（やや少）",
        "Q3": "利用時間 50–75%（やや多）",
        "Q4": "利用時間 上位25%（最多）",
    }

    rows: List[dict] = []
    for _, p in points.iterrows():
        y = int(p["exam_year"])
        rd = int(p["exam_round"])
        test_label = f"{y} R{rd}"

        start, end = prep_windows[(y, rd)]

        df_point = df_scores[(df_scores["exam_year"] == y) & (df_scores["exam_round"] == rd)].copy()
        df_point = coerce_student_id_int(df_point, "student_id")
        student_ids = df_point["student_id"].astype(int).tolist()

        df_usage = query_usage_hours_by_student(
        conn,
        df_point.student_id.tolist(),
        start,
        end,
        subject_slug,
        )

        df = df_point.merge(df_usage, on="student_id", how="left")

        df["hours_raw"] = pd.to_numeric(df["hours_raw"], errors="coerce").fillna(0.0)
        df["hours_capped"] = pd.to_numeric(df["hours_capped"], errors="coerce").fillna(0.0)

        # 🔑 Quartiles based on capped usage
        df["usage_quartile"] = assign_quartiles(df["hours_capped"])
        df = df.dropna(subset=["usage_quartile"]).copy()

        if args.score_stat == "median":
            agg = df.groupby("usage_quartile", as_index=False).agg(score=("score", "median"), n=("student_id", "nunique"))
        else:
            agg = df.groupby("usage_quartile", as_index=False).agg(score=("score", "mean"), n=("student_id", "nunique"))

        for q in ["Q1", "Q2", "Q3", "Q4"]:
            if q not in set(agg["usage_quartile"]):
                agg = pd.concat([agg, pd.DataFrame([{"usage_quartile": q, "score": float("nan"), "n": 0}])], ignore_index=True)

        agg = agg.sort_values("usage_quartile")

        for _, r in agg.iterrows():
            rows.append({
                "exam_year": y,
                "exam_round": rd,
                "test_label": test_label,
                "usage_quartile": r["usage_quartile"],
                "score": float(r["score"]) if pd.notna(r["score"]) else float("nan"),
                "n": int(r["n"]),
            })

    conn.close()

    df_out = pd.DataFrame(rows)
    if df_out.empty:
        raise RuntimeError("プロット用データが空です。")

    order = (
        df_out[["exam_year", "exam_round", "test_label"]]
        .drop_duplicates()
        .sort_values(["exam_year", "exam_round"])
        .reset_index(drop=True)
    )
    x_labels = order["test_label"].tolist()
    x_map = {lbl: i for i, lbl in enumerate(x_labels)}

    # --------------------------------------
    # Plot
    # --------------------------------------
    fig, ax = plt.subplots(figsize=(12, 6))

    bar_width = 0.18
    offsets = {"Q1": -1.5, "Q2": -0.5, "Q3": 0.5, "Q4": 1.5}

    for q in ["Q1", "Q2", "Q3", "Q4"]:
        d = df_out[df_out["usage_quartile"] == q].copy()
        d["x"] = d["test_label"].map(x_map)
        d = d.sort_values("x")

        xs = [x + offsets[q] * bar_width for x in d["x"].tolist()]
        ax.bar(xs, d["score"].tolist(), width=bar_width, label=legend_map[q])

    ax.set_xticks(range(len(x_labels)))
    ax.set_xticklabels(x_labels, rotation=30, ha="right")
    ax.set_ylabel(f"ベネッセ得点（{'中央値' if args.score_stat=='median' else '平均'}）")
    ax.set_xlabel("ベネッセ実施回（年×回）")

    grade_jp = f"中学{start_grade}年"
    subj_jp = subject_jp(subject_slug)

    ax.set_title(
        f"{subj_jp} コホート {cohort_start_year}（開始学年：{grade_jp}）\n"
        "BookRoll利用時間の四分位ごとのベネッセ得点\n"
        "（準備期間：初回=4/1〜テスト前日、その後=前回テスト翌日〜今回テスト前日；DBのみ；学年制約あり）"
    )

    # Legend above the chart (compact), and reserve TOP space so nothing overlaps.
    # --- Legend: put it BELOW the plot (no overlap, no shifting) ---
    handles, labels = ax.get_legend_handles_labels()

    # Put legend at the figure-level (not inside the axes)
    fig.legend(
    handles, labels,
    title="利用時間の四分位",
    loc="lower center",
    bbox_to_anchor=(0.5, 0.01),
    ncol=2,              # <- stacked 4 lines
    frameon=True,
    fontsize=10,
    title_fontsize=10,
    )

    fig.tight_layout(rect=[0, 0.18, 1, 1])


    out_png = ROOT_DIR / f"{subject_slug}_{cohort_start_year}cohort_scores_by_usage_quartile_JP.png"
    fig.savefig(out_png, dpi=200)
    print(f"\n📈 Plot saved to: {out_png.resolve()}")
    plt.show()


if __name__ == "__main__":
    main()
