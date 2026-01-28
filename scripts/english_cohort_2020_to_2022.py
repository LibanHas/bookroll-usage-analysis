import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import seaborn as sns
import pandas as pd
import re
from pathlib import Path
import mysql.connector

# ---- HARD-SET JAPANESE FONT HERE ----
JP_FONT_NAME = "Hiragino Sans"

matplotlib.rcParams["font.family"] = JP_FONT_NAME
matplotlib.rcParams["font.sans-serif"] = [JP_FONT_NAME]
matplotlib.rcParams["axes.unicode_minus"] = False

print("matplotlib font.family =", matplotlib.rcParams["font.family"])
print("matplotlib font.sans-serif =", matplotlib.rcParams["font.sans-serif"])
# -------------------------------------


# -----------------------------
# DB loader (ENGLISH ONLY, 2020 & 2022)
# -----------------------------
def load_benesse_english_from_db() -> pd.DataFrame:
    """
    Load Benesse English rows for years 2020 and 2022 from course_student_scores,
    and convert to 'raw_score' using scaled * max.
    """
    print("🔌 Connecting to MySQL...")

    conn = mysql.connector.connect(
        host="10.236.173.145",
        port=33308,
        user="readonly_user",
        password="P3FXDdNAehkLiAWNEbTWDLrRngBZYWScCWD8ZDeXLJ",
        database="analysis_development",
    )

    query = """
        SELECT
            id,
            student_id,
            course_id,
            course_name,
            name,
            quiz,
            min,
            max,
            scaled,
            created_at
        FROM course_student_scores
        WHERE (name LIKE '%Benesse%' OR name LIKE '%ベネッセ%')
          AND course_name LIKE '%英語%'
          AND scaled IS NOT NULL
          AND scaled > 0
    """

    df = pd.read_sql(query, conn)
    conn.close()
    print(f"📥 Loaded {len(df):,} Benesse English rows from DB (scaled > 0)")

    return df


# -----------------------------
# Helpers to parse course_name / name
# -----------------------------
def extract_year_from_course(course_name: str):
    """From '2020年度英語[中1]A組' etc. → 2020."""
    m = re.search(r"(\d{4})年度", str(course_name))
    return int(m.group(1)) if m else None


def extract_grade_jhs(course_name: str):
    """
    Normalize grade to '中1', '中2', '中3' for JHS.

    Handles patterns like:
      - '2020年度英語[中1]A組'
      - '2022年度1年A組英語[中学]'
      - '2023年度中学2年A組[英語]'
    """
    s = str(course_name)

    # Pattern [中1] / [中2] / [中3]
    m = re.search(r"\[中([123])\]", s)
    if m:
        return f"中{m.group(1)}"

    # Pattern '1年A組英語[中学]' etc. (2022 style)
    m2 = re.search(r"([123])年.*英語\[中学\]", s)
    if m2:
        return f"中{m2.group(1)}"

    # Pattern '中学1年A組[英語]' etc. (2023+ style)
    m3 = re.search(r"中学([123])年", s)
    if m3:
        return f"中{m3.group(1)}"

    return None


def extract_exam_round(exam_name: str):
    """
    From:
      - '2020_中学_1年_英語_Benesse模試2回' → 2
      - '2021年度 2A ベネッセ模試 第1回 英語' → 1
    If missing, return 1 as fallback.
    """
    s = str(exam_name)

    m = re.search(r"模試(\d)回", s)  # 'Benesse模試2回'
    if m:
        return int(m.group(1))

    m2 = re.search(r"第(\d)回", s)    # '第1回 英語'
    if m2:
        return int(m2.group(1))

    return 1


# -----------------------------
# Load 2021 raw English from LogPalette CSV
# -----------------------------
def load_logpalette_2021_english_raw(csv_path: Path) -> pd.DataFrame:
    """
    Load 2021 English raw scores from the merged LogPalette CSV.

    Accepts both English and Japanese column names, e.g.:

      - student_id
      - year / 年度
      - grade / 学年
      - exam_round / 回
      - raw_score / score / 得点 / 点数

    If year/grade/exam_round are missing, we fall back to defaults:
      year = 2021, grade = '中2', exam_round = 1
    (you can change these defaults below)
    """
    print(f"📥 Loading LogPalette 2021 English raw from: {csv_path}")
    df = pd.read_csv(csv_path)

    cols = set(df.columns)

    # ---- student_id must exist ----
    if "student_id" not in cols:
        raise ValueError("CSV must contain a 'student_id' column (matching course_student_scores).")

    # ---- raw score column detection ----
    raw_candidates = ["raw_score", "score", "得点", "点数"]
    raw_col = next((c for c in raw_candidates if c in cols), None)
    if raw_col is None:
        raise ValueError(f"Could not find a raw score column. Tried: {raw_candidates}")
    
    # ---- year / grade / round detection ----
    year_candidates  = ["year", "年度"]
    grade_candidates = ["grade", "学年"]
    round_candidates = ["exam_round", "回", "round"]

    year_col  = next((c for c in year_candidates  if c in cols), None)
    grade_col = next((c for c in grade_candidates if c in cols), None)
    round_col = next((c for c in round_candidates if c in cols), None)

    # Defaults if missing (edit these if needed)
    DEFAULT_YEAR = 2021
    DEFAULT_GRADE = "中2"
    DEFAULT_ROUND = 1  # if this file is e.g. 第1回; for 第2回 change to 2

    if year_col is None:
        df["year"] = DEFAULT_YEAR
        year_col = "year"
    if grade_col is None:
        df["grade"] = DEFAULT_GRADE
        grade_col = "grade"
    if round_col is None:
        df["exam_round"] = DEFAULT_ROUND
        round_col = "exam_round"

    # ---- Normalize to standard column names ----
    rename_map = {
        year_col: "year",
        grade_col: "grade",
        round_col: "exam_round",
        raw_col: "raw_score",
    }
    df = df.rename(columns=rename_map)

    # ---- Type cleanup ----
    df["student_id"] = df["student_id"].astype(int)
    df["year"] = df["year"].astype(int)
    df["grade"] = df["grade"].astype(str).str.strip()
    df["exam_round"] = df["exam_round"].astype(int)
    df["raw_score"] = df["raw_score"].astype(float)

    # We only need 2021 rows here
    df = df[df["year"] == 2021]

    print(f"✅ Loaded {len(df)} rows from LogPalette 2021 English")
    return df[["student_id", "year", "grade", "exam_round", "raw_score"]]


# -----------------------------
# Analysis / plotting
# -----------------------------
def analyze_english_2020_to_2022_cohort_raw(df_db: pd.DataFrame,
                                            df_2021: pd.DataFrame,
                                            out_dir: Path):
    """
    2020 中1 cohort (English) 4-point trajectory using RAW scores:

        1. 2020 中1 Benesse英語 第2回   (DB)
        2. 2021 中2 Benesse英語 第1回 (LogPalette CSV)
        3. 2021 中2 Benesse英語 第2回 (LogPalette CSV)
        4. 2022 中3 Benesse英語 第1回 (DB)
    """
    # ---- Prepare DB part (2020 & 2022) ----
    df = df_db.copy()
    df["year"] = df["course_name"].apply(extract_year_from_course)
    df["grade"] = df["course_name"].apply(extract_grade_jhs)
    df["exam_round"] = df["name"].apply(extract_exam_round)

    # Keep only rows with parsed year/grade/student
    df = df[df["year"].notna()]
    df = df[df["grade"].notna()]
    df = df[df["student_id"].notna()]

    df["year"] = df["year"].astype(int)
    df["grade"] = df["grade"].astype(str)

    # Convert scaled → raw score using max (usually 100)
    df["raw_score"] = (df["scaled"] * df["max"]).astype(float)

    # Keep only 2020 & 2022 from DB
    df_db_subset = df[df["year"].isin([2020, 2022])]
    df_db_subset = df_db_subset[["student_id", "year", "grade", "exam_round", "raw_score"]]

    print("\n=== DB subset (2020 & 2022) ===")
    print(df_db_subset["year"].value_counts().sort_index())

    # ---- Combine with 2021 LogPalette part ----
    df_all = pd.concat([df_db_subset, df_2021], ignore_index=True)

    print("\n=== Combined year counts (2020, 2021, 2022) ===")
    print(df_all["year"].value_counts().sort_index())
    print("\n=== Combined grade counts ===")
    print(df_all["grade"].value_counts())

    # ---- 1) Identify 2020 中1 cohort (English) ----
    cohort_mask_2020_chu1 = (df_all["year"] == 2020) & (df_all["grade"] == "中1")
    cohort_students = df_all.loc[cohort_mask_2020_chu1, "student_id"].dropna().unique()
    print(f"\n2020 中1 English cohort: {len(cohort_students)} students found")

    if len(cohort_students) == 0:
        print("⚠️ No 2020 中1 cohort students found. Aborting analysis.")
        return

    # ---- 2) Restrict to those students and the 4 target tests ----
    time_points = [
        (2020, "中1", 2, "2020年度 中1 第2回"),
        (2021, "中2", 1, "2021年度 中2 第1回"),
        (2021, "中2", 2, "2021年度 中2 第2回"),
        (2022, "中3", 1, "2022年度 中3 第1回"),
    ]
    key_to_label = {(y, g, r): label for (y, g, r, label) in time_points}

    cond_2020 = (df_all["year"] == 2020) & (df_all["grade"] == "中1") & (df_all["exam_round"] == 2)
    cond_2021_1 = (df_all["year"] == 2021) & (df_all["grade"] == "中2") & (df_all["exam_round"] == 1)
    cond_2021_2 = (df_all["year"] == 2021) & (df_all["grade"] == "中2") & (df_all["exam_round"] == 2)
    cond_2022 = (df_all["year"] == 2022) & (df_all["grade"] == "中3") & (df_all["exam_round"] == 1)

    target_mask = cond_2020 | cond_2021_1 | cond_2021_2 | cond_2022

    coh = df_all[(df_all["student_id"].isin(cohort_students)) & target_mask].copy()

    if coh.empty:
        print("⚠️ No English Benesse rows for the cohort in the 4 target tests.")
        return

    coh["time_label"] = [
        key_to_label.get((int(y), str(g), int(r)), None)
        for y, g, r in zip(coh["year"], coh["grade"], coh["exam_round"])
    ]
    coh = coh[coh["time_label"].notna()]

    # ---- 3) Average raw score per student per time point ----
    summary = (
        coh.groupby(["student_id", "time_label"], as_index=False)["raw_score"]
        .mean()
    )

    # ---- 4) Pivot to wide format ----
    pivot = summary.pivot(index="student_id",
                          columns="time_label",
                          values="raw_score")

    ordered_labels = [tp[3] for tp in time_points]
    pivot = pivot.reindex(columns=ordered_labels)

    # Keep only students who have all four scores
    pivot = pivot.dropna()

    print(f"\nStudents with ALL 4 English raw scores: {len(pivot)}")
    if pivot.empty:
        print("⚠️ No students with complete 4-point records. Nothing to plot.")
        return

    print("\n=== Basic stats (English, raw points) ===")
    for label in ordered_labels:
        print(f"{label}: mean = {pivot[label].mean():.2f}")

    sns.set_style("whitegrid")
    jp_font = fm.FontProperties(family=JP_FONT_NAME)

    # -----------------------------
    # Line plot: individual trajectories across 4 tests (raw scores)
    # -----------------------------
    x_positions = list(range(len(ordered_labels)))

    plt.figure(figsize=(12, 7))
    ax = plt.gca()

    # Individual students
    for sid, row in pivot.iterrows():
        ax.plot(
            x_positions,
            row.values,
            marker="o",
            linewidth=1.0,
            alpha=0.25,
        )

    # Median line
    medians = pivot.median(axis=0)
    ax.plot(
        x_positions,
        medians.values,
        marker="o",
        linewidth=3.0,
        alpha=0.95,
        color="black",
        label="中央値",
    )

    ax.set_xticks(x_positions)
    ax.set_xticklabels(ordered_labels, fontproperties=jp_font, rotation=20)

    ax.set_ylabel("得点（点）", fontproperties=jp_font)
    for lbl in ax.get_yticklabels():
        lbl.set_fontproperties(jp_font)

    ax.set_title(
        "2020年度 中1コホートのBenesse英語スコア推移（生得点：2020第2回→2021第1・第2回→2022第1回）",
        fontproperties=jp_font,
    )

    # Assuming 0–100 points; adjust if needed
    ax.set_ylim(0, 100)
    ax.legend(prop=jp_font)

    plt.tight_layout()
    out_path = out_dir / "english_cohort_2020chu1_4points_raw_lines.png"
    plt.savefig(out_path, dpi=200)
    plt.close()
    print(f"💾 Saved English 4-point RAW line plot: {out_path.resolve()}")
    

# -----------------------------
# Main
# -----------------------------
def main():
    # 1) Load DB data (2020 & 2022 English)
    df_db = load_benesse_english_from_db()

    # 2) Load 2021 English raw from LogPalette CSV
    csv_2021 = Path("logpalette_2021_english_with_ids.csv")
    df_2021 = load_logpalette_2021_english_raw(csv_2021)

    # 3) Analysis + plot
    out_dir = Path(".")
    analyze_english_2020_to_2022_cohort_raw(df_db, df_2021, out_dir)


if __name__ == "__main__":
    main()
