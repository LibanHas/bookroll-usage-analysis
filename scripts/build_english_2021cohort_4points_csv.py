# scripts/build_english_2021cohort_4points_csv.py

from pathlib import Path
import re
import mysql.connector
import pandas as pd



BASE_DIR = Path(".").resolve()

# 2021 中1 英語 (both rounds, with student_id + raw_score + exam_round)
CSV_2021 = BASE_DIR / "logpalette_2021_chu1_english_with_ids.csv"

OUT_CSV = BASE_DIR / "english_2021cohort_4points_long.csv"


# ---------- Helpers ----------
def extract_year_from_course(course_name: str):
    s = str(course_name)
    m = re.search(r"(\d{4})年度", s)
    return int(m.group(1)) if m else None


def extract_grade_jhs(course_name: str):
    s = str(course_name)

    # [中1] / [中2] / [中3]
    m = re.search(r"\[中([123])\]", s)
    if m:
        return f"中{m.group(1)}"

    # 2022 style: "1年A組英語[中学]"
    m2 = re.search(r"([123])年.*英語\[中学\]", s)
    if m2:
        return f"中{m2.group(1)}"

    # 2023 style: "中学1年A組[英語]"
    m3 = re.search(r"中学([123])年", s)
    if m3:
        return f"中{m3.group(1)}"

    return None


def extract_exam_round_from_name(exam_name: str):
    s = str(exam_name)

    m = re.search(r"模試(\d)回", s)   # "Benesse模試2回"
    if m:
        return int(m.group(1))

    m2 = re.search(r"第(\d)回", s)    # "第1回 英語"
    if m2:
        return int(m2.group(1))

    return None


# ---------- Step 1: Load 2022 English from DB (中2) ----------
def load_db_english_2022_chu2() -> pd.DataFrame:
    print("🔌 Connecting to MySQL for 2022 English...")
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
          AND course_name LIKE '%英語%'
          AND scaled IS NOT NULL
          AND scaled > 0
    """

    df = pd.read_sql(query, conn)
    conn.close()
    print(f"📥 Loaded {len(df):,} Benesse English rows from DB (scaled > 0)")

    df["year"] = df["course_name"].apply(extract_year_from_course)
    df["grade"] = df["course_name"].apply(extract_grade_jhs)
    df["exam_round"] = df["name"].apply(extract_exam_round_from_name)

    # Only 2022 中2
    df = df[(df["year"] == 2022) & (df["grade"] == "中2")]
    df = df[df["exam_round"].notna()]
    df = df[df["student_id"].notna()]

    # Use scaled*100 as percent
    df["score_percent"] = df["scaled"] * 100.0
    df["subject"] = "英語"
    df["source"] = "db"

    print("\n=== 2022 中2 English (DB) exam_round counts ===")
    print(df["exam_round"].value_counts())

    # We only care about 第1回 and 第2回
    df = df[df["exam_round"].isin([1, 2])].copy()

    return df


# ---------- Step 2: Load 2021 中1 English from LogPalette ----------
def load_logpalette_2021_chu1_english() -> pd.DataFrame:
    print(f"\n📥 Loading 2021 中1 English LogPalette CSV: {CSV_2021}")
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

    # exam_round should already be 1/2, but normalise anyway
    if "exam_round" not in df.columns:
        raise ValueError("2021 CSV must have an exam_round column.")

    def to_round(x):
        s = str(x)
        m = re.search(r"(\d)", s)
        return int(m.group(1)) if m else None

    df["exam_round"] = df["exam_round"].apply(to_round)

    # Ensure metadata
    if "year" not in df.columns:
        df["year"] = 2021
    if "grade" not in df.columns:
        df["grade"] = "中1"
    if "subject" not in df.columns:
        df["subject"] = "英語"

    # Raw scores are already 0–100
    df["score_percent"] = df[score_col].astype(float)
    df["source"] = "logpalette"

    print("\n=== 2021 中1 English exam_round counts ===")
    print(df["exam_round"].value_counts())

    # Only 第1回 & 第2回
    df = df[df["exam_round"].isin([1, 2])].copy()

    return df[["student_id", "year", "grade", "subject", "exam_round", "score_percent", "source"]]


def main():
    # 1) 2021 中1 English from LogPalette
    df_2021 = load_logpalette_2021_chu1_english()
    df_2021["student_id"] = df_2021["student_id"].astype(int)
    ids_2021 = set(df_2021["student_id"].unique())
    print(f"\n👥 2021 中1 English cohort size (distinct IDs): {len(ids_2021)}")

    # 2) 2022 中2 English from DB
    df_2022 = load_db_english_2022_chu2()
    df_2022["student_id"] = df_2022["student_id"].astype(int)
    ids_2022 = set(df_2022["student_id"].unique())
    print(f"\n📊 Distinct student_id in 2022 中2 English (DB): {len(ids_2022)}")

    # 3) Intersection = true cohort that appears in both years
    common_ids = ids_2021 & ids_2022
    print("\n=== Intersection diagnostics (2021/2022) ===")
    print(f"IDs in 2021: {len(ids_2021)}")
    print(f"IDs in 2022: {len(ids_2022)}")
    print(f"Intersection size (present in both years): {len(common_ids)}")
    print("Sample common IDs:", sorted(list(common_ids))[:10])

    # Restrict to common cohort
    df_2021_cohort = df_2021[df_2021["student_id"].isin(common_ids)].copy()
    df_2022_cohort = df_2022[df_2022["student_id"].isin(common_ids)].copy()

    print(f"\n2021 rows for common cohort: {len(df_2021_cohort)} "
          f"(distinct IDs: {df_2021_cohort['student_id'].nunique()})")
    print(f"2022 rows for common cohort: {len(df_2022_cohort)} "
          f"(distinct IDs: {df_2022_cohort['student_id'].nunique()})")

    # 4) Build combined long-format table
    # Target points:
    #   (2021, 中1, round=1)
    #   (2021, 中1, round=2)
    #   (2022, 中2, round=1)
    #   (2022, 中2, round=2)

    combined = pd.concat(
        [
            df_2021_cohort[["student_id", "year", "grade", "subject", "exam_round", "score_percent", "source"]],
            df_2022_cohort[["student_id", "year", "grade", "subject", "exam_round", "score_percent", "source"]],
        ],
        ignore_index=True,
    )

    # Add readable labels
    def make_label(row):
        y = int(row["year"])
        g = str(row["grade"])
        r = int(row["exam_round"])
        return f"{y}年度 {g} 第{r}回"

    combined["time_label"] = combined.apply(make_label, axis=1)

    print("\n=== Combined counts per time_label ===")
    print(combined.groupby("time_label").size())

    # Sort nicely and save
    combined.sort_values(["student_id", "year", "exam_round"], inplace=True)
    combined.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print(f"\n💾 Saved 4-point cohort CSV to:\n{OUT_CSV.resolve()}")


if __name__ == "__main__":
    main()
