# scripts/merge_logpalette_scores_with_ids_2022_chu1_eng.py

from pathlib import Path
import pandas as pd

# =========================
# CONFIG
# =========================
BASE_DIR = Path(__file__).resolve().parents[1]

# LogPalette export for 2022 中1 英語 第1回
LOGPALETTE_CSV = BASE_DIR / "logpal_2022_first_eng_scores_chu1.csv"

# Student ID mapping (student_no -> student_id)
MAPPING_CSV = BASE_DIR / "student_id_mapping.csv"

# Output: merged, with student_id + metadata
OUTPUT_CSV = BASE_DIR / "logpalette_2022_chu1_english_with_ids.csv"

# Column names inside the LogPalette CSV
# Change these if your headers are different
LOGPALETTE_STUDENT_NO_COL = "student_no"   # 生徒番号
LOGPALETTE_SCORE_COL      = "raw_score"    # 得点（生点）

# Column names inside the mapping CSV
MAPPING_STUDENT_NO_COL = "student_no"
MAPPING_STUDENT_ID_COL = "student_id"

# Fixed metadata for this dataset
YEAR_2022       = 2022
GRADE_2022_CHU1 = "中1"
SUBJECT_ENG     = "英語"
EXAM_ROUND      = 1   # 第1回


# =========================
# Helpers
# =========================
def load_logpalette_csv(path: Path, exam_round: int) -> pd.DataFrame:
    """
    Load a single LogPalette CSV (one exam round), standardise column names,
    and add exam_round + fixed year/grade/subject metadata.
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
    df["year"]       = YEAR_2022
    df["grade"]      = GRADE_2022_CHU1
    df["subject"]    = SUBJECT_ENG

    return df


def main():
    # 1) Load the scores from LogPalette
    scores = load_logpalette_csv(LOGPALETTE_CSV, exam_round=EXAM_ROUND)

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
