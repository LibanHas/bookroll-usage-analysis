# scripts/build_english_2022cohort_long_csv.py

from pathlib import Path
import re
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]

CSV_2022 = BASE_DIR / "logpalette_2022_chu1_english_with_ids.csv"
CSV_2023 = BASE_DIR / "logpalette_2023_chu2_english_with_ids.csv"
CSV_2024 = BASE_DIR / "logpalette_2024_chu3_english_with_ids.csv"

OUT_CSV = BASE_DIR / "english_2022cohort_long.csv"


def to_round(x):
    """
    Normalise exam_round to int if it isn't already.
    Accepts values like 1, "1", "第1回", etc.
    """
    s = str(x)
    m = re.search(r"(\d)", s)
    return int(m.group(1)) if m else None


def load_logpalette_year_csv(path: Path, expected_year: int, expected_grade: str) -> pd.DataFrame:
    """
    Generic loader for the with_ids CSVs.

    Assumes columns like:
      - student_no
      - raw_score
      - exam_round
      - year
      - grade
      - subject (should be '英語')
      - student_id
    """
    print(f"\n📥 Loading English LogPalette CSV: {path}")
    df = pd.read_csv(path)

    print("\n=== Columns in CSV ===")
    print(df.columns.tolist())
    print("Rows:", len(df))

    # Basic checks
    if "student_id" not in df.columns:
        raise ValueError(f"student_id column not found in {path.name}")
    if "raw_score" not in df.columns and "score_raw" not in df.columns:
        raise ValueError(f"No raw score column found in {path.name} (expected 'raw_score' or 'score_raw')")
    if "exam_round" not in df.columns:
        raise ValueError(f"exam_round column not found in {path.name}")

    # Normalise score column
    if "raw_score" in df.columns:
        score_col = "raw_score"
    else:
        score_col = "score_raw"

    # Normalise exam_round
    df["exam_round"] = df["exam_round"].apply(to_round)

    # Ensure year / grade / subject present (overwrite if missing)
    if "year" not in df.columns:
        df["year"] = expected_year
    else:
        df["year"] = df["year"].fillna(expected_year)

    if "grade" not in df.columns:
        df["grade"] = expected_grade
    else:
        df["grade"] = df["grade"].fillna(expected_grade)

    if "subject" not in df.columns:
        df["subject"] = "英語"
    else:
        df["subject"] = df["subject"].fillna("英語")

    # Convert to percent (raw score out of 100)
    df["score_percent"] = df[score_col].astype(float)
    df["source"] = "logpalette"

    print("\n=== exam_round counts ===")
    print(df["exam_round"].value_counts())

    # Keep only the columns we care about
    return df[["student_id", "year", "grade", "subject", "exam_round", "score_percent", "source"]]


def main():
    # 1) 2022 中1 English from LogPalette
    df_2022 = load_logpalette_year_csv(CSV_2022, expected_year=2022, expected_grade="中1")

    # Cohort is everyone who appears in 2022 中1 English
    cohort_ids = set(df_2022["student_id"].dropna().astype(int).unique())
    print(f"\n👥 2022 中1 English cohort size (distinct IDs): {len(cohort_ids)}")

    # 2) 2023 中2 English from LogPalette
    df_2023 = load_logpalette_year_csv(CSV_2023, expected_year=2023, expected_grade="中2")
    df_2023["student_id"] = df_2023["student_id"].astype(int)

    # 3) 2024 中3 English from LogPalette
    df_2024 = load_logpalette_year_csv(CSV_2024, expected_year=2024, expected_grade="中3")
    df_2024["student_id"] = df_2024["student_id"].astype(int)

    # IDs present in each year
    ids_2022 = cohort_ids
    ids_2023 = set(df_2023["student_id"].unique())
    ids_2024 = set(df_2024["student_id"].unique())

    # Intersection across all three grades (stay with the same students throughout)
    common_ids = ids_2022 & ids_2023 & ids_2024

    print("\n=== Intersection diagnostics (2022/2023/2024) ===")
    print(f"IDs in 2022: {len(ids_2022)}")
    print(f"IDs in 2023: {len(ids_2023)}")
    print(f"IDs in 2024: {len(ids_2024)}")
    print(f"Intersection size (present in all 3 years): {len(common_ids)}")
    print("Sample common IDs:", list(sorted(common_ids))[:10])

    # Restrict each year to the common cohort
    df_2022_cohort = df_2022[df_2022["student_id"].isin(common_ids)].copy()
    df_2023_cohort = df_2023[df_2023["student_id"].isin(common_ids)].copy()
    df_2024_cohort = df_2024[df_2024["student_id"].isin(common_ids)].copy()

    print(f"\n2022 rows for common cohort: {len(df_2022_cohort)} (IDs: {df_2022_cohort['student_id'].nunique()})")
    print(f"2023 rows for common cohort: {len(df_2023_cohort)} (IDs: {df_2023_cohort['student_id'].nunique()})")
    print(f"2024 rows for common cohort: {len(df_2024_cohort)} (IDs: {df_2024_cohort['student_id'].nunique()})")

    # 4) Combine into a single long-format table
    combined = pd.concat(
        [df_2022_cohort, df_2023_cohort, df_2024_cohort],
        ignore_index=True,
    )

    # Human-readable labels
    def make_label(row):
        y = int(row["year"])
        g = str(row["grade"])
        r = int(row["exam_round"])
        return f"{y}年度 {g} 第{r}回"

    if not combined.empty:
        combined["time_label"] = combined.apply(make_label, axis=1)
    else:
        combined["time_label"] = []

    print("\n=== Combined counts per time_label ===")
    if not combined.empty:
        print(combined.groupby("time_label").size())
    else:
        print("No rows in combined table.")

    # Sort nicely and save
    combined.sort_values(["student_id", "year", "exam_round"], inplace=True)
    combined.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print(f"\n💾 Saved 2022 cohort long CSV to:\n{OUT_CSV.resolve()}")


if __name__ == "__main__":
    main()
