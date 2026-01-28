from pathlib import Path
import pandas as pd
import mysql.connector
from db_config import DB_CONFIG

# ======================================
# CONFIG
# ======================================

BASE_DIR = Path(__file__).resolve().parents[1]

# 2023 / 2024 merged LogPalette + IDs
CSV_2023 = BASE_DIR / "logpalette_2023_chu2_maths_with_ids.csv"
CSV_2024 = BASE_DIR / "logpalette_2024_chu3_maths_with_ids.csv"

# Output
OUTPUT_CSV = BASE_DIR / "math_2022cohort_6points_long.csv"


# ======================================
# HELPERS
# ======================================

def query_db_for_2022_math_chu1() -> pd.DataFrame:
    """
    Get 2022 Chu1 Benesse math scores from course_student_scores.
    We:
      - filter by 2022年度, 数学, Benesse模試1回/2回, scaled > 0
      - (optionally) filter by '1年' in course_name to identify 中1 classes
      - map name -> exam_round (1 or 2)
      - set year=2022, grade='中1'
      - convert scaled (0–1) to raw_score (0–100)
    """
    print("\n🔌 Querying MySQL for 2022 中1 Math (Benesse)...")

    conn = mysql.connector.connect(**DB_CONFIG)

    q = """
        SELECT
            student_id,
            scaled,
            course_name,
            name
        FROM course_student_scores
        WHERE name IN ('Benesse模試1回', 'Benesse模試2回')
          AND course_name LIKE '%2022年度%'
          AND course_name LIKE '%数学%'
          AND course_name LIKE '%1年%'
          AND scaled > 0
    """

    df = pd.read_sql(q, conn)
    conn.close()

    print(f"\nRaw 2022 rows from DB (after filters): {len(df)}")
    print("Unique `name` values:", df["name"].unique())
    print("Sample course_name values:")
    print(df["course_name"].drop_duplicates().head())

    # Normalize student_id to string
    df["student_id"] = df["student_id"].astype(str).str.strip()

    # Map exam_round from `name`
    round_map = {
        "Benesse模試1回": 1,
        "Benesse模試2回": 2,
    }
    df["exam_round"] = df["name"].map(round_map)
    df = df.dropna(subset=["exam_round"])
    df["exam_round"] = df["exam_round"].astype(int)

    # Attach metadata
    df["year"]  = 2022
    df["grade"] = "中1"

    # Convert scaled (0–1) to 0–100
    df["raw_score"] = df["scaled"] * 100.0

    # For consistency with LP files
    df["subject"] = "数学"

    print("\n=== 2022 中1 Math DB exam_round counts ===")
    print(df["exam_round"].value_counts())

    return df


def load_lp_with_ids(path: Path) -> pd.DataFrame:
    """
    Load a merged LogPalette+IDs CSV (2023 or 2024) which already has:
      - student_no, student_id, raw_score, exam_round, year, grade, subject
    """
    print(f"\n📥 Loading merged LP+IDs: {path.name}")
    df = pd.read_csv(path)

    # Normalise ID and basic types
    df["student_id"] = df["student_id"].astype(str).str.strip()
    df["raw_score"]  = df["raw_score"].astype(float)
    df["exam_round"] = df["exam_round"].astype(int)

    print("Rows:", len(df))
    print("Unique students:", df["student_id"].nunique())
    print("exam_round counts:")
    print(df["exam_round"].value_counts())
    print("Years present:", df["year"].unique())
    print("Grades present:", df["grade"].unique())

    return df


# ======================================
# MAIN
# ======================================

def main():
    # 1) 2022 Benesse (Chu1)
    df2022 = query_db_for_2022_math_chu1()

    # 2) 2023 LP (Chu2) + IDs
    df2023 = load_lp_with_ids(CSV_2023)
    # Ensure metadata is what we expect
    df2023["year"]  = 2023
    df2023["grade"] = "中2"

    # 3) 2024 LP (Chu3) + IDs
    df2024 = load_lp_with_ids(CSV_2024)
    df2024["year"]  = 2024
    df2024["grade"] = "中3"

    # 4) Intersection of students across all 3 years
    ids_2022 = set(df2022["student_id"])
    ids_2023 = set(df2023["student_id"])
    ids_2024 = set(df2024["student_id"])

    common_ids = sorted(list(ids_2022 & ids_2023 & ids_2024))

    print("\n=== Intersection diagnostics (cohort) ===")
    print("2022 IDs:", len(ids_2022))
    print("2023 IDs:", len(ids_2023))
    print("2024 IDs:", len(ids_2024))
    print("Common across all 3 years:", len(common_ids))

    # Filter to common students
    df2022_c = df2022[df2022["student_id"].isin(common_ids)].copy()
    df2023_c = df2023[df2023["student_id"].isin(common_ids)].copy()
    df2024_c = df2024[df2024["student_id"].isin(common_ids)].copy()

    # 5) Combine into one long dataframe
    combined = pd.concat([df2022_c, df2023_c, df2024_c], ignore_index=True)

    # 6) Create time label
    def make_label(r):
        return f"{r['year']}年度 {r['grade']} 第{r['exam_round']}回"

    combined["time_label"] = combined.apply(make_label, axis=1)

    # 7) Keep only students with all 6 points
    counts = combined.groupby("student_id")["time_label"].nunique()
    full_ids = counts[counts == 6].index

    combined_full = combined[combined["student_id"].isin(full_ids)].copy()

    print("\n=== Cohort completeness check (6 points) ===")
    print("Students with any data:", combined["student_id"].nunique())
    print("Students with all 6 points:", combined_full["student_id"].nunique())

    # 8) Save
    combined_full.to_csv(OUTPUT_CSV, index=False)
    print(f"\n💾 Saved 6-point Math cohort CSV to:\n{OUTPUT_CSV}")


if __name__ == "__main__":
    main()
