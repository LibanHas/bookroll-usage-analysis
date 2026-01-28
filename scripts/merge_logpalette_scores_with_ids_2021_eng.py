# scripts/merge_logpalette_scores_with_ids_2021_chu1_eng.py

from pathlib import Path
import pandas as pd

# =========================
# CONFIG
# =========================
BASE_DIR = Path(__file__).resolve().parents[1]

# Two separate CSVs for first / second round (2021 中1 英語)
CSV_FIRST  = BASE_DIR / "logpal_2021_first_eng_scores_chu1.csv"   # 第1回
CSV_SECOND = BASE_DIR / "logpal_2021_second_eng_scores_chu1.csv"  # 第2回

MAPPING_CSV = BASE_DIR / "student_id_mapping.csv"

# Output: merged, with student_id + metadata
OUTPUT_CSV  = BASE_DIR / "logpalette_2021_chu1_english_with_ids.csv"

# Column names inside the LogPalette CSVs
LOGPALETTE_STUDENT_NO_COL = "student_no"   # 生徒番号
LOGPALETTE_SCORE_COL      = "raw_score"    # 得点（生点）

# Column names inside the mapping CSV
MAPPING_STUDENT_NO_COL = "student_no"
MAPPING_STUDENT_ID_COL = "student_id"

# Fixed metadata for this dataset
YEAR = 2021
GRADE = "中1"
SUBJECT = "英語"

# =========================
# Helpers
# =========================
def load_logpalette_csv(path: Path, exam_round: int) -> pd.DataFrame:
    """
    Load a single LogPalette CSV (one exam round), standardise column names,
    and add exam_round + fixed year/grade metadata.
    """
    print(f"📥 Loading {path.name} (exam_round={exam_round})")

    # If you get encoding errors, try encoding='cp932' or 'utf-8-sig'
    df = pd.read_csv(path)

    # Standardise column names
    df = df.rename(columns={
        LOGPALETTE_STUDENT_NO_COL: "student_no",
        LOGPALETTE_SCORE_COL: "raw_score",
    })

    # Clean up types
    df["student_no"] = df["student_no"].astype(str).str.strip()
    df["raw_score"]  = df["raw_score"].astype(float)

    # Add exam_round + metadata
    df["exam_round"] = exam_round
    df["year"]       = YEAR
    df["grade"]      = GRADE
    df["subject"]    = SUBJECT

    return df


def main():
    # 1) Load the two rounds from CSV
    first_df  = load_logpalette_csv(CSV_FIRST,  exam_round=1)
    second_df = load_logpalette_csv(CSV_SECOND, exam_round=2)

    # Combine them into one scores table
    scores = pd.concat([first_df, second_df], ignore_index=True)

    print("\n=== Scores dataframe summary ===")
    print("Rows:", len(scores))
    print("Unique student_no:", scores["student_no"].nunique())
    print("exam_round counts:")
    print(scores["exam_round"].value_counts())

    # 2) Load mapping file
    print(f"\n📥 Loading student ID mapping from {MAPPING_CSV.name}")
    mapping = pd.read_csv(MAPPING_CSV)

    mapping = mapping.rename(columns={
        MAPPING_STUDENT_NO_COL: "student_no",
        MAPPING_STUDENT_ID_COL: "student_id",
    })
    mapping["student_no"] = mapping["student_no"].astype(str).str.strip()

    print("\n=== Mapping dataframe summary ===")
    print("Rows:", len(mapping))
    print("Unique student_no in mapping:", mapping["student_no"].nunique())

    # 3) Merge scores ↔ student_id
    merged = pd.merge(
        scores,
        mapping[["student_no", "student_id"]],
        on="student_no",
        how="inner",
    )

    print("\n=== Merge result ===")
    print("Rows in scores:", len(scores))
    print("Rows in mapping:", len(mapping))
    print("Rows after merge:", len(merged))

    missing = scores[~scores["student_no"].isin(merged["student_no"])]
    if not missing.empty:
        print("\n⚠️ Scores with no matching student_id:")
        print(missing.head())
        print(f"... and {len(missing)} total rows without a match.")
    else:
        print("\n✅ All scores matched to a student_id.")

    # 4) Save merged CSV
    merged.to_csv(OUTPUT_CSV, index=False)
    print(f"\n💾 Saved merged file: {OUTPUT_CSV.resolve()}")


if __name__ == "__main__":
    main()
