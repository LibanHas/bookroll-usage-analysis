from pathlib import Path
import pandas as pd
import mysql.connector
import numpy as np
from db_config import DB_CONFIG 
# ======================================
# CONFIG
# ======================================
BASE_DIR = Path(__file__).resolve().parents[1]

# 2021 LogPalette CSVs (Chu1)
CSV_2021_FIRST  = BASE_DIR / "logpal_2021_first_maths_scores_chu1.csv"
CSV_2021_SECOND = BASE_DIR / "logpal_2021_second_maths_scores_chu1.csv"

# Mapping
MAPPING_CSV = BASE_DIR / "student_id_mapping.csv"

# Output
OUTPUT_CSV = BASE_DIR / "math_2021cohort_4points_long.csv"


# ======================================
# HELPERS
# ======================================
def load_lp(path, exam_round, year, grade):
    print(f"📥 Loading LogPalette CSV: {path.name}")
    df = pd.read_csv(path)

    df = df.rename(columns={
        "student_no": "student_no",
        "raw_score": "raw_score",
    })

    df["student_no"] = df["student_no"].astype(str).str.strip()
    df["raw_score"]  = df["raw_score"].astype(float)
    df["exam_round"] = exam_round
    df["year"]       = year
    df["grade"]      = grade
    df["subject"]    = "Math"

    return df


def query_db_for_2022_math():
    print("\n🔌 Querying MySQL for 2022 Math...")

    conn = mysql.connector.connect(**DB_CONFIG)

    q = """
        SELECT student_id,
               scaled,
               course_name,
               name
        FROM course_student_scores
        WHERE name IN ('Benesse模試1回', 'Benesse模試2回')
          AND course_name LIKE '%2022年度%'
          AND course_name LIKE '%数学%'
          AND scaled > 0
    """

    df = pd.read_sql(q, conn)
    conn.close()
    df["student_id"] = df["student_id"].astype(str).str.strip()
    print(f"\nRaw 2022 rows from DB: {len(df)}")
    print("Unique `name` values:", df["name"].unique())

    # Map exam_round from `name`
    round_map = {
        "Benesse模試1回": 1,
        "Benesse模試2回": 2,
    }
    df["exam_round"] = df["name"].map(round_map)

    # Keep only rows with a known exam_round
    df = df.dropna(subset=["exam_round"])

    # Make sure it's int, not float
    df["exam_round"] = df["exam_round"].astype(int)

    # Grade/year and raw_score
    df["grade"] = "中2"
    df["year"]  = 2022
    df["raw_score"] = df["scaled"] * 100

    df = df.rename(columns={"student_id": "student_id"})

    print("\n=== 2022 中2 Math DB exam_round counts ===")
    print(df["exam_round"].value_counts())

    return df


# ======================================
# MAIN
# ======================================
def main():
    # 1) Load 2021 LogPalette
    lp_first  = load_lp(CSV_2021_FIRST,  exam_round=1, year=2021, grade="中1")
    lp_second = load_lp(CSV_2021_SECOND, exam_round=2, year=2021, grade="中1")

    df2021 = pd.concat([lp_first, lp_second], ignore_index=True)
    print("\n=== 2021 中1 Math exam_round counts ===")
    print(df2021["exam_round"].value_counts())

    # 2) Load 2022 DB Math
    df2022 = query_db_for_2022_math()

    # 3) Load ID mapping
    print("\n📥 Loading student ID mapping...")
    mapping = pd.read_csv(MAPPING_CSV)
    mapping["student_no"] = mapping["student_no"].astype(str).str.strip()
    mapping["student_id"] = mapping["student_id"].astype(str).str.strip()
    # Merge 2021 LP → student_id
    df2021 = pd.merge(
        df2021,
        mapping[["student_no", "student_id"]],
        on="student_no",
        how="inner"
    )
    df2021["student_id"] = df2021["student_id"].astype(str).str.strip()
    print("\n=== 2021 mapping success ===")
    print("Rows:", len(df2021))
    print("Unique IDs:", df2021["student_id"].nunique())

    # 4) Intersection (students present in both years)
    df2022["student_id"] = df2022["student_id"].astype(str).str.strip()
    ids_2021 = set(df2021["student_id"])
    ids_2022 = set(df2022["student_id"])

    common_ids = sorted(list(ids_2021 & ids_2022))
    print("\n=== Intersection diagnostics ===")
    print("2021 IDs:", len(ids_2021))
    print("2022 IDs:", len(ids_2022))
    print("Common:", len(common_ids))

    # Filter both
    df2021_c = df2021[df2021["student_id"].isin(common_ids)].copy()
    df2022_c = df2022[df2022["student_id"].isin(common_ids)].copy()

    # 5) Combine
    combined = pd.concat([df2021_c, df2022_c], ignore_index=True)

    # 6) Create label
    def make_label(r):
        return f"{r['year']}年度 {r['grade']} 第{r['exam_round']}回"

    combined["time_label"] = combined.apply(make_label, axis=1)

  
    

        # 6) Create label
    def make_label(r):
        return f"{r['year']}年度 {r['grade']} 第{r['exam_round']}回"

    combined["time_label"] = combined.apply(make_label, axis=1)

    # ✅ Keep only students who have all 4 points
    counts = combined.groupby("student_id")["time_label"].nunique()
    full_ids = counts[counts == 4].index

    combined_full = combined[combined["student_id"].isin(full_ids)].copy()

    print("\n=== Cohort completeness check ===")
    print("Total students with any data:", combined["student_id"].nunique())
    print("Students with all 4 points:", combined_full["student_id"].nunique())

    # 7) Save ONLY the complete cohort
    combined_full.to_csv(OUTPUT_CSV, index=False)
    print(f"\n💾 Saved 4-point Math cohort CSV to:\n{OUTPUT_CSV}")

if __name__ == "__main__":
    main()
