from pathlib import Path
import sys
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector

# --------------------------------------
# Ensure we can import db_config
# --------------------------------------
CURRENT_DIR = Path(__file__).resolve().parent      # .../scripts/bookroll_analysis
PARENT_DIR = CURRENT_DIR.parent                    # .../scripts
ROOT_DIR = PARENT_DIR.parent                       # .../leaf_school

for p in (PARENT_DIR, ROOT_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from db_config import DB_CONFIG


# ======================================
# CONFIG
# ======================================

BASE_DIR = ROOT_DIR  # project root


def build_query(year: int, grade: str, subject_en: str) -> str:
    """
    Build SQL to get weekly out-of-school BookRoll usage (Apr–Oct)
    for a given calendar year, middle-school grade (1–3), and subject.

    Handles three naming eras for JHS courses:

      MATH:
        - old:  「数学[中1]」「数学[中2]」「数学[中3]」
        - mid:  「1年A組数学[中学]」「2年B組数学[中学]」(2022)
        - new:  「中学1年A組[数学]」「中学2年B組[数学]」(2023–)

      ENGLISH:
        - old:  「英語[中1]」「英語[中2]」「英語[中3]」
        - mid:  「1年A組英語[中学]」… (2022)
        - new:  「中学1年A組[英語]」… (2023–)
    """
    subject_en_lower = subject_en.lower()

    if subject_en_lower == "math":
        subject_ja = "数学"
    elif subject_en_lower == "english":
        subject_ja = "英語"
    else:
        raise ValueError(f"Unknown subject: {subject_en}. Use 'Math' or 'English'.")

    grade_str = str(grade).strip()
    if grade_str not in {"1", "2", "3"}:
        raise ValueError(f"Grade must be 1, 2, or 3 (for 中1, 中2, 中3). Got: {grade}")

    # -----------------------------
    # Build grade+subject patterns
    # -----------------------------
    if subject_ja == "数学":
        # JHS Math – 3 naming families across years
        if grade_str == "1":
            like_old = "%数学[中1%"          # 2019–2021
            like_mid = "%1年%数学[中学%"     # 2022
            like_new = "%中学1年%[数学]%"    # 2023–
        elif grade_str == "2":
            like_old = "%数学[中2%"
            like_mid = "%2年%数学[中学%"
            like_new = "%中学2年%[数学]%"
        else:  # "3"
            like_old = "%数学[中3%"
            like_mid = "%3年%数学[中学%"
            like_new = "%中学3年%[数学]%"

        course_condition = (
            "("
            f"course_title LIKE '{like_old}' "
            f"OR course_title LIKE '{like_mid}' "
            f"OR course_title LIKE '{like_new}'"
            ")"
        )

    else:
        # JHS English – analogous patterns
        if grade_str == "1":
            like_old = "%英語[中1%"
            like_mid = "%1年%英語[中学%"
            like_new = "%中学1年%[英語]%"
        elif grade_str == "2":
            like_old = "%英語[中2%"
            like_mid = "%2年%英語[中学%"
            like_new = "%中学2年%[英語]%"
        else:  # "3"
            like_old = "%英語[中3%"
            like_mid = "%3年%英語[中学%"
            like_new = "%中学3年%[英語]%"

        course_condition = (
            "("
            f"course_title LIKE '{like_old}' "
            f"OR course_title LIKE '{like_mid}' "
            f"OR course_title LIKE '{like_new}'"
            ")"
        )

    query = f"""
SELECT
    ssokid                        AS user_id,
    YEAR(operationdate)           AS year,
    WEEK(operationdate, 1)        AS week_of_year,
    MIN(DATE(operationdate))      AS week_start,
    SUM(diftime) / 3600.0         AS total_hours
FROM artsci_bookroll_difftimes
WHERE
    {course_condition}
    AND YEAR(operationdate) = {year}
    AND diftime IS NOT NULL
    AND diftime > 0

    -- Outside school hours
    AND NOT (
        DAYOFWEEK(operationdate) BETWEEN 2 AND 6
        AND TIME(operationdate) >= '08:00:00'
        AND TIME(operationdate) <  '16:00:00'
    )

    -- April–October only
    AND MONTH(operationdate) BETWEEN 4 AND 10

GROUP BY
    ssokid,
    YEAR(operationdate),
    WEEK(operationdate, 1)
ORDER BY
    ssokid,
    year,
    week_of_year;
"""
    return query


def main():
    # ==============================
    # 0. Parse CLI arguments
    # ==============================
    if len(sys.argv) != 4:
        print("Usage: python3 plot_weekly_by_student.py <year> <grade> <subject>")
        print("Example: python3 plot_weekly_by_student.py 2020 1 Math")
        sys.exit(1)

    year = int(sys.argv[1])      # e.g. 2020
    grade = sys.argv[2]          # "1", "2", or "3"
    subject = sys.argv[3]        # "Math" or "English"

    subject_lower = subject.lower()
    if subject_lower not in {"math", "english"}:
        raise ValueError(f"Subject must be 'Math' or 'English'. Got: {subject}")

    # Build query for this configuration
    query = build_query(year, grade, subject)

    # Output filename (e.g. math_2020_chu1_weekly_by_student_all.png)
    subject_slug = subject_lower
    output_name = f"{subject_slug}_{year}_chu{grade}_weekly_by_student_all.png"
    output_png = BASE_DIR / output_name

    # For title
    subject_label = "Math" if subject_lower == "math" else "English"
    grade_label = f"中{grade}"

    # ==============================
    # 1. Run query, load into pandas
    # ==============================
    print("🔌 Connecting to MySQL...")
    conn = mysql.connector.connect(**DB_CONFIG)

    try:
        print(f"📥 Running query for {year} {grade_label} {subject_label} weekly usage...")
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
        print("🔌 Connection closed.")

    if df.empty:
        print("No data returned. Check the course_title pattern or filters.")
        return

    # Ensure dates are proper datetimes
    df["week_start"] = pd.to_datetime(df["week_start"])

    # Sort nicely
    df = df.sort_values(["user_id", "week_start"])

    print(f"Total students in cohort (with any usage): {df['user_id'].nunique()}")

    # Cohort-average curve (all students)
    df_mean = (
        df.groupby("week_start")["total_hours"]
          .mean()
          .reset_index()
          .sort_values("week_start")
    )

    # ==============================
    # 3. Plot: one line per student (all) + cohort mean
    # ==============================
    plt.figure(figsize=(12, 7))
    ax = plt.gca()

    # Individual students: thin, same colour, transparent
    for user_id, sub in df.groupby("user_id"):
        ax.plot(
            sub["week_start"],
            sub["total_hours"],
            linestyle="-",
            linewidth=0.5,
            alpha=0.2,
            color="lightgray",
        )

    # Cohort mean line: thicker, different colour
    ax.plot(
        df_mean["week_start"],
        df_mean["total_hours"],
        linestyle="--",
        linewidth=3,
        alpha=0.9,
        color="black",
        label="Cohort mean",
    )

    title = (
        f"Weekly out-of-school BookRoll usage\n"
        f"{subject_label}, {year} {grade_label} cohort (all students + mean)"
    )
    ax.set_title(title)
    ax.set_xlabel("Week (start date)")
    ax.set_ylabel("Total hours per week")
    plt.xticks(rotation=45)

    # Legend: only the mean needs a label now
    ax.legend(fontsize=9)

    plt.tight_layout()

    # Save + show
    output_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_png, dpi=200)
    print(f"📊 Plot saved to: {output_png}")
    plt.show()


if __name__ == "__main__":
    main()
