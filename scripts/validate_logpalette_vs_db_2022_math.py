from pathlib import Path
import re
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]

LP_CSV = BASE_DIR / "logpalette_2022_chu3_maths_with_ids.csv"
DB_CSV = BASE_DIR / "db_benesse_math_all_years.csv"

# --- same helpers as your main project ---

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

    # 2022-style: "2022年度3年A組数学[中学]"
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

def main():
    # --- LogPalette side ---
    print(f"📥 Loading LogPalette 2022 中3 第1回: {LP_CSV.name}")
    lp = pd.read_csv(LP_CSV)

    lp["student_id"] = lp["student_id"].astype(int)
    lp["raw_score"] = lp["raw_score"].astype(float)
    lp["score_percent_lp"] = lp["raw_score"]  # already 0–100

    print("\n=== LogPalette dataframe summary ===")
    print("Rows:", len(lp))
    print("Unique student_id:", lp["student_id"].nunique())

    # --- DB side (all years) ---
    print(f"\n📥 Loading DB Benesse Math all years: {DB_CSV.name}")
    db = pd.read_csv(DB_CSV)

    db["student_id"] = db["student_id"].astype(int)
    db["scaled"] = db["scaled"].astype(float)
    db["score_percent_db"] = db["scaled"] * 100.0

    # Parse metadata using the same logic as main scripts
    db["year"] = db["course_name"].apply(extract_year_from_course)
    db["grade"] = db["course_name"].apply(extract_grade_jhs)
    db["exam_round"] = db["name"].apply(extract_exam_round_from_name)

    # Filter to 2022 中3 第1回
    db_2022 = db[
        (db["year"] == 2022) &
        (db["grade"] == "中3") &
        (db["exam_round"] == 1)
    ].copy()

    print("\n=== DB 2022 中3 第1回 summary ===")
    print("Rows:", len(db_2022))
    print("Unique student_id:", db_2022["student_id"].nunique())

    # --- Merge on student_id ---
    merged = pd.merge(
        lp[["student_id", "score_percent_lp"]],
        db_2022[["student_id", "score_percent_db"]],
        on="student_id",
        how="inner",
    )

    print("\n=== Merge summary ===")
    print("Rows after merge:", len(merged))
    print("Unique student_id after merge:", merged["student_id"].nunique())

    # Compare scores
    merged["diff"] = merged["score_percent_lp"] - merged["score_percent_db"]
    merged["abs_diff"] = merged["diff"].abs()

    print("\n=== Difference statistics (LogPalette vs DB) ===")
    print(merged["abs_diff"].describe())

    mismatches = merged[merged["abs_diff"] > 0.01].copy()
    if len(mismatches) > 0:
        print(f"\n⚠️ Found {len(mismatches)} students with mismatched scores (> 0.01):")
        print(mismatches.head(20))
    else:
        print("\n✅ All matched students have identical scores (within 0.01).")

    # ID coverage check
    lp_ids = set(lp["student_id"].unique())
    db_ids = set(db_2022["student_id"].unique())

    only_in_lp = lp_ids - db_ids
    only_in_db = db_ids - lp_ids

    print("\n=== ID coverage check ===")
    print(f"Student IDs only in LogPalette: {len(only_in_lp)}")
    print(f"Student IDs only in DB (2022 中3 第1回): {len(only_in_db)}")

    if only_in_lp:
        print("Example only-in-LP IDs:", sorted(list(only_in_lp))[:10])
    if only_in_db:
        print("Example only-in-DB IDs:", sorted(list(only_in_db))[:10])


if __name__ == "__main__":
    main()
