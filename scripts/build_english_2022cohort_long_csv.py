from pathlib import Path
import re
import mysql.connector
import pandas as pd



BASE_DIR = Path(".").resolve()

CSV_2023 = BASE_DIR / "logpalette_2023_chu2_english_with_ids.csv"
CSV_2024 = BASE_DIR / "logpalette_2024_chu3_english_with_ids.csv"

OUT_CSV = BASE_DIR / "english_2022cohort_long.csv"


def extract_year_from_course(course_name: str):
    s = str(course_name)
    m = re.search(r"(\d{4})年度", s)
    return int(m.group(1)) if m else None


def extract_grade_jhs(course_name: str):
    s = str(course_name)
    m = re.search(r"\[中([123])\]", s)
    if m:
        return f"中{m.group(1)}"
    m2 = re.search(r"([123])年.*英語\[中学\]", s)
    if m2:
        return f"中{m2.group(1)}"
    m3 = re.search(r"中学([123])年", s)
    if m3:
        return f"中{m3.group(1)}"
    return None


def extract_exam_round_from_name(exam_name: str):
    s = str(exam_name)
    m = re.search(r"模試(\d)回", s)
    if m:
        return int(m.group(1))
    m2 = re.search(r"第(\d)回", s)
    if m2:
        return int(m2.group(1))
    return None


def load_db_english_2022_chu1() -> pd.DataFrame:
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

    df = df[(df["year"] == 2022) & (df["grade"] == "中1")]
    df = df[df["exam_round"].notna()]
    df = df[df["student_id"].notna()]

    df["score_percent"] = df["scaled"] * 100.0
    df["subject"] = "英語"
    df["source"] = "db"

    print("\n=== 2022 中1 English (DB) exam_round counts ===")
    print(df["exam_round"].value_counts())

    return df


def load_logpalette_english(path: Path, default_year: int, default_grade: str) -> pd.DataFrame:
    print(f"\n📥 Loading English LogPalette CSV: {path}")
    df = pd.read_csv(path)

    print("\n=== Columns in CSV ===")
    print(df.columns.tolist())
    print(f"Rows: {len(df)}")

    if "student_id" not in df.columns:
        raise ValueError("student_id column not found in CSV")

    if "raw_score" in df.columns:
        score_col = "raw_score"
    elif "score_raw" in df.columns:
        score_col = "score_raw"
    else:
        raise ValueError("No raw score column found (expected 'raw_score' or 'score_raw')")

    if "exam_round" not in df.columns:
        raise ValueError("CSV must have an exam_round column.")

    def to_round(x):
        s = str(x)
        m = re.search(r"(\d)", s)
        return int(m.group(1)) if m else None

    df["exam_round"] = df["exam_round"].apply(to_round)

    if "year" not in df.columns:
        df["year"] = default_year
    if "grade" not in df.columns:
        df["grade"] = default_grade
    if "subject" not in df.columns:
        df["subject"] = "英語"

    df["score_percent"] = df[score_col].astype(float)
    df["source"] = "logpalette"

    print(f"\n=== {default_year} {default_grade} English exam_round counts ===")
    print(df["exam_round"].value_counts())

    return df[["student_id", "year", "grade", "subject", "exam_round", "score_percent", "source"]]


def main():
    df_2022 = load_db_english_2022_chu1()
    df_2022["student_id"] = df_2022["student_id"].astype(int)
    ids_2022 = set(df_2022["student_id"].unique())
    print(f"\n👥 2022 中1 English cohort size (distinct IDs): {len(ids_2022)}")

    df_2023 = load_logpalette_english(CSV_2023, default_year=2023, default_grade="中2")
    df_2023["student_id"] = df_2023["student_id"].astype(int)
    ids_2023 = set(df_2023["student_id"].unique())

    df_2024 = load_logpalette_english(CSV_2024, default_year=2024, default_grade="中3")
    df_2024["student_id"] = df_2024["student_id"].astype(int)
    ids_2024 = set(df_2024["student_id"].unique())

    print("\n=== Intersection diagnostics (2022/2023/2024) ===")
    print("IDs in 2022:", len(ids_2022))
    print("IDs in 2023:", len(ids_2023))
    print("IDs in 2024:", len(ids_2024))

    common_ids = ids_2022 & ids_2023 & ids_2024
    print("Intersection size (present in all 3 years):", len(common_ids))
    print("Sample common IDs:", sorted(list(common_ids))[:10])

    df_2022_c = df_2022[df_2022["student_id"].isin(common_ids)].copy()
    df_2023_c = df_2023[df_2023["student_id"].isin(common_ids)].copy()
    df_2024_c = df_2024[df_2024["student_id"].isin(common_ids)].copy()

    print(f"\n2022 rows for common cohort: {len(df_2022_c)} (IDs: {df_2022_c['student_id'].nunique()})")
    print(f"2023 rows for common cohort: {len(df_2023_c)} (IDs: {df_2023_c['student_id'].nunique()})")
    print(f"2024 rows for common cohort: {len(df_2024_c)} (IDs: {df_2024_c['student_id'].nunique()})")

    combined = pd.concat([df_2022_c, df_2023_c, df_2024_c], ignore_index=True)

    def make_label(row):
        y = int(row["year"])
        g = str(row["grade"])
        r = int(row["exam_round"])
        return f"{y}年度 {g} 第{r}回"

    combined["time_label"] = combined.apply(make_label, axis=1)

    print("\n=== Combined counts per time_label ===")
    print(combined.groupby("time_label").size())

    combined.sort_values(["student_id", "year", "grade", "exam_round"], inplace=True)
    combined.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print(f"\n💾 Saved 2022 cohort long CSV to:\n{OUT_CSV.resolve()}")


if __name__ == "__main__":
    main()
