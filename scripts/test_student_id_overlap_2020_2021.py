from pathlib import Path
import pandas as pd
import mysql.connector

# ---------- CONFIG ----------
CSV_2021 = Path(".") / "logpalette_2021_chu2_english_with_ids.csv"


# ----------------------------


def load_2020_chu1_english_ids():
    """
    Get distinct student_id for 2020年度英語[中1] Benesse from DB.
    """
    print("🔌 Loading 2020 中1 English from DB...")
    conn = mysql.connector.connect(**DB_CONFIG)

    query = """
        SELECT DISTINCT student_id
        FROM course_student_scores
        WHERE (name LIKE '%Benesse%' OR name LIKE '%ベネッセ%')
          AND course_name LIKE '2020年度英語[中1]%%'
          AND scaled IS NOT NULL
          AND scaled > 0
          AND student_id IS NOT NULL
    """

    df = pd.read_sql(query, conn)
    conn.close()

    print(f"📥 2020 中1 rows: {len(df)} (distinct student_id: {df['student_id'].nunique()})")
    return set(df["student_id"].dropna().astype(int))


def load_2021_chu2_english_ids():
    """
    Get distinct student_id for 2021年度中2英語 from the LogPalette CSV.
    (We assume the CSV already has columns: student_id, year, grade, exam_round, raw_score)
    """
    print(f"📥 Loading 2021 中2 English from CSV: {CSV_2021}")
    df = pd.read_csv(CSV_2021)

    print("\n=== 2021 CSV column names ===")
    print(df.columns.tolist())

    # Filter to 2021 中2 英語 if year/grade columns exist
    if "year" in df.columns and "grade" in df.columns:
        df = df[(df["year"] == 2021) & (df["grade"] == "中2")]

    print(f"📥 2021 中2 rows after filter: {len(df)}")
    print("Distinct student_id in 2021 data:", df["student_id"].nunique())

    return set(df["student_id"].dropna().astype(int))


def main():
    ids_2020 = load_2020_chu1_english_ids()
    ids_2021 = load_2021_chu2_english_ids()

    print("\n=== Basic counts ===")
    print("2020 中1 English student_id count:", len(ids_2020))
    print("2021 中2 English student_id count:", len(ids_2021))

    intersection = ids_2020 & ids_2021
    print("\n=== Overlap ===")
    print("Intersection size:", len(intersection))

    # Show a few IDs from each set and the intersection
    print("\nSample 2020 IDs:", sorted(list(ids_2020))[:10])
    print("Sample 2021 IDs:", sorted(list(ids_2021))[:10])
    print("Sample intersecting IDs:", sorted(list(intersection))[:10])


if __name__ == "__main__":
    main()
