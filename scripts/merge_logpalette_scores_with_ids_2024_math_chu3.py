from pathlib import Path
import pandas as pd

# =========================
# CONFIG
# =========================
BASE_DIR = Path(__file__).resolve().parents[1]

# Input CSVs (your downloaded LogPalette files)
CSV_FIRST  = BASE_DIR / "logpal_2024_first_maths_scores_chu3.csv"
CSV_SECOND = BASE_DIR / "logpal_2024_second_maths_scores_chu3.csv"

# Student mapping
MAPPING_CSV = BASE_DIR / "student_id_mapping.csv"

# Output
OUTPUT_CSV  = BASE_DIR / "logpalette_2024_chu3_maths_with_ids.csv"

# Column names inside the LogPalette CSVs
LOGPALETTE_STUDENT_NO_COL = "student_no"
LOGPALETTE_SCORE_COL      = "raw_score"

# Column names inside the mapping CSV
MAPPING_STUDENT_NO_COL = "student_no"
MAPPING_STUDENT_ID_COL = "student_id"

# Metadata fixed for this dataset
YEAR_2024       = 2024
GRADE_2024_CHU3 = "中3"
SUBJECT         = "数学"


# =========================
# Helpers
# =========================
def load_logpalette_csv(path: Path, exam_round: int) -> pd.DataFrame:
    print(f"📥 Loading {path.name} (exam_round={exam_round})")
    df = pd.read_csv(path)

    # Standardise column names
    df = df.rename(columns={
        LOGPALETTE_STUDENT_NO_COL: "student_no",
        LOGPALETTE_SCORE_COL: "raw_score",
    })

    # Clean types
    df["student_no"] = df["student_no"].astype(str).str.strip()
    df["raw_score"]  = df["raw_score"].astype(float)

    # Add metadata
    df["exam_round"] = exam_round
    df["year"]       = YEAR_2024
    df["grade"]      = GRADE_2024_CHU3
    df["subject"]    = SUBJECT

    return df


# =========================
# Main
# =========================
def main():
    first_df  = load_logpalette_csv(CSV_FIRST,  exam_round=1)
    second_df = load_logpalette_csv(CSV_SECOND, exam_round=2)

    scores = pd.concat([first_df, second_df], ignore_index=True)

    print("\n=== Scores dataframe summary ===")
    print("Rows:", len(scores))
    print("Unique student_no:", scores["student_no"].nunique())
    print("exam_round counts:")
    print(scores["exam_round"].value_counts())

    # Load mapping file
    print(f"\n📥 Loading student ID mapping from {MAPPING_CSV.name}")
    mapping = pd.read_csv(MAPPING_CSV).rename(columns={
        MAPPING_STUDENT_NO_COL: "student_no",
        MAPPING_STUDENT_ID_COL: "student_id"
    })
    mapping["student_no"] = mapping["student_no"].astype(str).str.strip()
    mapping["student_id"] = mapping["student_id"].astype(str).str.strip()

    print("\n=== Mapping dataframe summary ===")
    print("Rows:", len(mapping))
    print("Unique student_no in mapping:", mapping["student_no"].nunique())

    # Merge
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
    if len(missing) > 0:
        print("\n⚠️ Scores with no matching student_id:")
        print(missing.head())
        print(f"Missing total: {len(missing)}")
    else:
        print("\n✅ All scores matched successfully.")

    # Save output
    merged.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"\n💾 Saved merged file:\n{OUTPUT_CSV.resolve()}")


if __name__ == "__main__":
    main()
