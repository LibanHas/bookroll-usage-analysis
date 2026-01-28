# scripts/postprocess_logpalette_2021_chu2_english.py

import pandas as pd
from pathlib import Path

# === CONFIG: change these per exam ===
YEAR       = 2021
GRADE      = "中2"       # 2021年度 中2 英語
SUBJECT    = "英語"
EXAM_ROUND = 2          # 1 → 第1回, 2 → 第2回 etc.

# Input/output paths
BASE_DIR = Path(".").resolve()
MERGED_CSV = BASE_DIR / "logpalette_2021_english_with_ids.csv"
OUT_CSV    = BASE_DIR / "benesse_2021_chu2_english_round2_with_ids.csv"

def main():
    print("📥 Loading merged LogPalette+IDs file...")
    df = pd.read_csv(MERGED_CSV)

    # Sanity check
    print("\n=== Columns in merged file ===")
    print(df.columns.tolist())
    print(f"Rows: {len(df)}")

    # Make sure student_id exists
    if "student_id" not in df.columns:
        raise ValueError("student_id column not found in merged CSV")

    # Add metadata columns for later analysis
    df["year"]       = YEAR
    df["grade"]      = GRADE
    df["subject"]    = SUBJECT
    df["exam_round"] = EXAM_ROUND

    # (Optional) normalise score column name
    if "score_raw" in df.columns:
        df.rename(columns={"score_raw": "score"}, inplace=True)
    # If your column is named differently, adjust here.

    # Save
    df.to_csv(OUT_CSV, index=False)
    print(f"\n💾 Saved post-processed file: {OUT_CSV}")

if __name__ == "__main__":
    main()
