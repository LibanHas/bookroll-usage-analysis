from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]

CSV_FIRST  = BASE_DIR / "logpal_2024_first_eng_scores_chu3.csv"
CSV_SECOND = BASE_DIR / "logpal_2024_second_eng_scores_chu3.csv"
MAPPING_CSV = BASE_DIR / "student_id_mapping.csv"

OUTPUT_CSV  = BASE_DIR / "logpalette_2024_chu3_english_with_ids.csv"

LOGPALETTE_STUDENT_NO_COL = "student_no"
LOGPALETTE_SCORE_COL      = "raw_score"

MAPPING_STUDENT_NO_COL = "student_no"
MAPPING_STUDENT_ID_COL = "student_id"

YEAR  = 2024
GRADE = "中3"


def load_logpalette_csv(path: Path, exam_round: int) -> pd.DataFrame:
    print(f"📥 Loading {path.name} (exam_round={exam_round})")
    df = pd.read_csv(path)

    df = df.rename(columns={
        LOGPALETTE_STUDENT_NO_COL: "student_no",
        LOGPALETTE_SCORE_COL: "raw_score",
    })

    df["student_no"] = df["student_no"].astype(str).str.strip()
    df["raw_score"]  = df["raw_score"].astype(float)

    df["exam_round"] = exam_round
    df["year"]       = YEAR
    df["grade"]      = GRADE
    df["subject"]    = "英語"

    return df


def main():
    first_df  = load_logpalette_csv(CSV_FIRST,  exam_round=1)
    second_df = load_logpalette_csv(CSV_SECOND, exam_round=2)

    scores = pd.concat([first_df, second_df], ignore_index=True)

    print("\n=== Scores dataframe summary (2024 中3) ===")
    print("Rows:", len(scores))
    print("Unique student_no:", scores["student_no"].nunique())
    print("exam_round counts:")
    print(scores["exam_round"].value_counts())

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

    merged = pd.merge(
        scores,
        mapping[["student_no", "student_id"]],
        on="student_no",
        how="inner",
    )

    print("\n=== Merge result (2024 中3) ===")
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

    merged.to_csv(OUTPUT_CSV, index=False)
    print(f"\n💾 Saved merged file: {OUTPUT_CSV.resolve()}")


if __name__ == "__main__":
    main()
