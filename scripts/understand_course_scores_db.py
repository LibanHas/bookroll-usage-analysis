#!/usr/bin/env python3
"""
Data Understanding Script for `course_student_scores` (direct from MySQL).

Usage (from leaf_school folder):

    python3 scripts/understand_course_scores_db.py
"""

import mysql.connector
import pandas as pd


# ---------- 1. Database connection settings ----------
# ❗ Replace the strings below with your real credentials.
#    (Leave database name as 'analysis_development' if that is correct.)
DB_CONFIG = {
    "host": "10.236.173.145",
    "port": 33308,  # or your custom port
    "user": "readonly_user",
    "password": "P3FXDdNAehkLiAWNEbTWDLrRngBZYWScCWD8ZDeXLJ",
    "database": "analysis_development",
}


def load_course_scores() -> pd.DataFrame:
    """Connect to MySQL and load the full course_student_scores table."""
    print("🔌 Connecting to MySQL...")
    conn = mysql.connector.connect(**DB_CONFIG)

    try:
        query = """
            SELECT
                id,
                course_student_id,
                quiz,
                response,
                user_id,
                created_at,
                updated_at,
                sort,
                name,
                student_id,
                moodle_url,
                course_id,
                min,
                max,
                scaled,
                course_name,
                date_at,
                consumer_key
            FROM course_student_scores
        """
        print("📥 Running query: SELECT * FROM course_student_scores ...")
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
        print("🔌 Connection closed.")

    return df


def main():
    # --------------------------------------------------
    # 1. Load data from DB
    # --------------------------------------------------
    df = load_course_scores()

    # --------------------------------------------------
    # 2. Basic shape and columns
    # --------------------------------------------------
    print("\n=== BASIC SHAPE ===")
    print(f"Rows:    {len(df)}")
    print(f"Columns: {len(df.columns)}\n")

    print("Columns:")
    for col in df.columns:
        print(f"  - {col}")
    print()

    # Non-null counts
    print("=== NON-NULL COUNTS (top 20 columns) ===")
    print(df.count().sort_values(ascending=False).head(20))
    print()

    # --------------------------------------------------
    # 3. Unique counts for key identifiers
    # --------------------------------------------------
    print("=== UNIQUE COUNTS ===")
    for col in ["student_id", "user_id", "course_id", "course_name", "name"]:
        if col in df.columns:
            n_unique = df[col].nunique(dropna=True)
            print(f"{col:12s}: {n_unique}")
        else:
            print(f"{col:12s}: (not present in result)")
    print()

    # --------------------------------------------------
    # 4. Top courses by number of records
    # --------------------------------------------------
    if "course_name" in df.columns:
        print("=== TOP 20 COURSES BY NUMBER OF RECORDS ===")
        course_counts = (
            df["course_name"]
            .value_counts(dropna=False)
            .head(20)
        )
        print(course_counts)
        print()
    else:
        print("course_name not present; skipping course frequency table.\n")

    # --------------------------------------------------
    # 5. Missingness in key fields
    # --------------------------------------------------
    print("=== MISSINGNESS IN KEY FIELDS ===")

    def missing_info(col):
        if col not in df.columns:
            return f"{col:12s}: (not present)"
        total = len(df)
        missing = df[col].isna().sum()
        return f"{col:12s}: {missing:6d} missing ({missing / total * 100:5.2f}%)"

    for col in ["response", "date_at", "scaled", "quiz", "course_name"]:
        print(missing_info(col))
    print()

    # --------------------------------------------------
    # 6. Numeric summary: quiz & scaled
    # --------------------------------------------------
    numeric_cols = [c for c in ["quiz", "scaled", "min", "max"] if c in df.columns]
    if numeric_cols:
        print("=== NUMERIC SUMMARY (quiz / scaled / min / max) ===")
        print(df[numeric_cols].describe())
        print()
    else:
        print("No numeric quiz/score columns found; skipping numeric summary.\n")

    # --------------------------------------------------
    # 7. Check relationship: scaled vs quiz/max
    # --------------------------------------------------
    if all(c in df.columns for c in ["quiz", "min", "max", "scaled"]):
        print("=== CHECK: scaled ≈ (quiz - min) / (max - min) ? ===")

        # Avoid division by zero / NaN
        valid_mask = df["max"].notna() & (df["max"] != df["min"])
        tmp = df.loc[valid_mask, ["quiz", "min", "max", "scaled"]].copy()

        # Compute expected scaled score
        tmp["scaled_expected"] = (tmp["quiz"] - tmp["min"]) / (tmp["max"] - tmp["min"])

        # Difference
        tmp["scaled_diff"] = tmp["scaled"] - tmp["scaled_expected"]

        # Basic stats of the difference
        diff_desc = tmp["scaled_diff"].describe()
        print(diff_desc)

        # How many are "close enough" (within 0.001)
        close_enough = (tmp["scaled_diff"].abs() < 1e-3).mean() * 100
        print(f"\nPercentage where |scaled - expected| < 0.001: {close_enough:.2f}%")
        print()
    else:
        print("Not all of quiz/min/max/scaled present; skipping scaled-vs-quiz check.\n")

    # --------------------------------------------------
    # 8. Example: one course’s score distribution (preview)
    # --------------------------------------------------
    if "course_name" in df.columns and "scaled" in df.columns:
        course_counts = df["course_name"].value_counts()
        example_course = course_counts.idxmax()
        n_rows = course_counts.max()

        print("=== EXAMPLE COURSE ===")
        print(f"Course with most records: {example_course} ({n_rows} rows)")

        example = df[df["course_name"] == example_course]
        valid_mask = (example["scaled"] > 0) & (example["scaled"] <= 1)
        example_valid = example.loc[valid_mask, "scaled"] * 100
        print("\nScore summary for this course (0 < scaled ≤ 1, in %):")
        if len(example_valid) > 0:
            print(example_valid.describe())
        else:
            print("No valid scores for this course.")
        print()

    print("✅ Data understanding summary complete.")


if __name__ == "__main__":
    main()
