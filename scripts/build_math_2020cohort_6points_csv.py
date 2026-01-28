# scripts/build_math_2020cohort_6points_csv.py

from pathlib import Path
import re
import mysql.connector
import pandas as pd

# ---------- DB CONFIG ----------


BASE_DIR = Path(".").resolve()

# 2021 中2 数学 (both rounds, with student_id + raw_score + exam_round)
CSV_2021 = BASE_DIR / "logpalette_2021_chu2_maths_with_ids.csv"

OUT_CSV = BASE_DIR / "math_2020cohort_6points_long.csv"


# ---------- Helpers ----------
def extract_year_from_course(course_name: str):
    s = str(course_name)
    m = re.search(r"(\d{4})年度", s)
    return int(m.group(1)) if m else None


def extract_grade_jhs(course_name: str):
    s = str(course_name)

    # [中1] / [中2] / [中3] pattern (e.g. "数学[中1]" )
    m = re.search(r"\[中([123])\]", s)
    if m:
        return f"中{m.group(1)}"

    # 2022-style: "2022年度3年A組数学[中学]"
    # → look for "年度" followed by the grade digit
    m2 = re.search(r"年度([123])年.*数学\[中学\]", s)
    if m2:
        return f"中{m2.group(1)}"

    # 2023-style: "中学1年A組数学[中学]"
    m3 = re.search(r"中学([123])年", s)
    if m3:
        return f"中{m3.group(1)}"

    return None



def extract_exam_round_from_name(exam_name: str):
    s = str(exam_name)

    # "Benesse模試2回"
    m = re.search(r"模試(\d)回", s)
    if m:
        return int(m.group(1))

    # "第1回 数学"
    m2 = re.search(r"第(\d)回", s)
    if m2:
        return int(m2.group(1))

    return None


# ---------- Step 1: Load 2020 & 2022 Math from DB ----------
def load_db_math_2020_2022() -> tuple[pd.DataFrame, pd.DataFrame]:
    print("🔌 Connecting to MySQL for Math...")
    conn = mysql.connector.connect(**DB_CONFIG)

    query = """
        SELECT
            id,
            student_id,
            course_id,
            course_name,
            name,
            quiz,
            min,
            max,
            scaled,
            created_at
        FROM course_student_scores
        WHERE (name LIKE '%Benesse%' OR name LIKE '%ベネッセ%')
          AND course_name LIKE '%数学%'
          AND scaled IS NOT NULL
          AND scaled > 0;
    """

    df = pd.read_sql(query, conn)
    conn.close()
    print(f"📥 Loaded {len(df):,} Benesse Math rows from DB (scaled > 0)")

    df["year"] = df["course_name"].apply(extract_year_from_course)
    df["grade"] = df["course_name"].apply(extract_grade_jhs)
    df["exam_round"] = df["name"].apply(extract_exam_round_from_name)

    # Keep only rows with parsed info & student_id
    df = df[df["year"].notna()]
    df = df[df["grade"].notna()]
    df = df[df["exam_round"].notna()]
    df = df[df["student_id"].notna()]

    # Use scaled*100 as percent
    df["score_percent"] = df["scaled"] * 100.0
    df["subject"] = "数学"
    df["source"] = "db"

    # --- 2020 中1 Math ---
    df_2020 = df[(df["year"] == 2020) & (df["grade"] == "中1")].copy()
    print("\n=== 2020 中1 Math (DB) exam_round counts ===")
    print(df_2020["exam_round"].value_counts())

    # --- 2022 candidates: all 2022 Math rows ---
    df_2022_candidates = df[df["year"] == 2022].copy()
    print("\n=== 2022 Math grade distribution (raw) ===")
    print(df_2022_candidates["grade"].value_counts(dropna=False))

    # First try grade == "中3"
    df_2022 = df_2022_candidates[df_2022_candidates["grade"] == "中3"].copy()

    # Fallback: use course_name pattern if grade-based filter yields nothing
    if df_2022.empty:
        print("\n⚠️ No 2022 rows with grade == '中3' via regex; using course_name pattern fallback...")
        mask_3nen = df_2022_candidates["course_name"].astype(str).str.contains("3年", na=False)
        mask_chu3 = df_2022_candidates["course_name"].astype(str).str.contains("[中3]", na=False)
        df_2022 = df_2022_candidates[mask_3nen | mask_chu3].copy()

    print("\n=== 2022 中3 Math (DB) exam_round counts (after fallback, if any) ===")
    print(df_2022["exam_round"].value_counts())

    return df_2020, df_2022


# ---------- Step 2: Load 2021 中2 Math from LogPalette ----------
def load_logpalette_2021_chu2_math() -> pd.DataFrame:
    print(f"\n📥 Loading 2021 中2 Math LogPalette CSV: {CSV_2021}")
    df = pd.read_csv(CSV_2021)

    print("\n=== Columns in 2021 CSV ===")
    print(df.columns.tolist())
    print(f"Rows: {len(df)}")

    if "student_id" not in df.columns:
        raise ValueError("student_id column not found in 2021 CSV")

    # Find raw score column
    if "raw_score" in df.columns:
        score_col = "raw_score"
    elif "score_raw" in df.columns:
        score_col = "score_raw"
    else:
        raise ValueError("No raw score column found (expected 'raw_score' or 'score_raw')")

    if "exam_round" not in df.columns:
        raise ValueError("2021 CSV must have an exam_round column.")

    # Coerce exam_round to int 1/2
    def to_round(x):
        s = str(x)
        m = re.search(r"(\d)", s)
        return int(m.group(1)) if m else None

    df["exam_round"] = df["exam_round"].apply(to_round)

    df["year"] = 2021
    df["grade"] = "中2"
    df["subject"] = "数学"

    df["score_percent"] = df[score_col].astype(float)
    df["source"] = "logpalette"

    print("\n=== 2021 中2 Math exam_round counts ===")
    print(df["exam_round"].value_counts())

    return df[["student_id", "year", "grade", "subject", "exam_round", "score_percent", "source"]]


def main():
    # 1) 2020 & 2022 from DB
    df_2020, df_2022 = load_db_math_2020_2022()

    # Ensure student_id are ints
    df_2020["student_id"] = df_2020["student_id"].astype(int)
    df_2022["student_id"] = df_2022["student_id"].astype(int)

    # Keep both 第1回 & 第2回
    df_2020 = df_2020[df_2020["exam_round"].isin([1, 2])].copy()
    df_2022 = df_2022[df_2022["exam_round"].isin([1, 2])].copy()

    # 2) 2021 from LogPalette
    df_2021 = load_logpalette_2021_chu2_math()
    df_2021["student_id"] = df_2021["student_id"].astype(int)
    df_2021 = df_2021[df_2021["exam_round"].isin([1, 2])].copy()

    # 3) Intersection of IDs present in all three years
    ids_2020 = set(df_2020["student_id"].unique())
    ids_2021 = set(df_2021["student_id"].unique())
    ids_2022 = set(df_2022["student_id"].unique())

    common_ids = ids_2020 & ids_2021 & ids_2022

    print("\n=== Intersection diagnostics (2020/2021/2022) ===")
    print(f"IDs in 2020: {len(ids_2020)}")
    print(f"IDs in 2021: {len(ids_2021)}")
    print(f"IDs in 2022: {len(ids_2022)}")
    print(f"Intersection size (present in all 3 years): {len(common_ids)}")
    print("Sample common IDs:", sorted(list(common_ids))[:10])

    if not common_ids:
        print("\n⚠️ No common IDs across 2020/2021/2022. Nothing to output.")
        return

    df_2020_common = df_2020[df_2020["student_id"].isin(common_ids)].copy()
    df_2021_common = df_2021[df_2021["student_id"].isin(common_ids)].copy()
    df_2022_common = df_2022[df_2022["student_id"].isin(common_ids)].copy()

    print(f"\n2020 rows for common cohort: {len(df_2020_common)} (IDs: {df_2020_common['student_id'].nunique()})")
    print(f"2021 rows for common cohort: {len(df_2021_common)} (IDs: {df_2021_common['student_id'].nunique()})")
    print(f"2022 rows for common cohort: {len(df_2022_common)} (IDs: {df_2022_common['student_id'].nunique()})")

    # 4) Combine into long-format table
    combined = pd.concat(
        [
            df_2020_common[["student_id", "year", "grade", "subject", "exam_round", "score_percent", "source"]],
            df_2021_common[["student_id", "year", "grade", "subject", "exam_round", "score_percent", "source"]],
            df_2022_common[["student_id", "year", "grade", "subject", "exam_round", "score_percent", "source"]],
        ],
        ignore_index=True,
    )

    def make_label(row):
        y = int(row["year"])
        g = str(row["grade"])
        r = int(row["exam_round"])
        return f"{y}年度 {g} 第{r}回"

    combined["time_label"] = combined.apply(make_label, axis=1)

    print("\n=== Combined counts per time_label ===")
    print(combined.groupby("time_label").size())

    combined.sort_values(["student_id", "year", "exam_round"], inplace=True)
    combined.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print(f"\n💾 Saved 6-point Math cohort CSV to:\n{OUT_CSV.resolve()}")


if __name__ == "__main__":
    main()
