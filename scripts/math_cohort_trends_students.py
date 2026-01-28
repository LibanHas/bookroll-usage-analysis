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
# DB loader
# -----------------------------
def load_benesse_math_from_db() -> pd.DataFrame:
    """
    Load only Benesse Math rows from course_student_scores.
    """
    print("🔌 Connecting to MySQL...")

    conn = mysql.connector.connect(
        host="10.236.173.145",
        port=33308,
        user="readonly_user",
        password="P3FXDdNAehkLiAWNEbTWDLrRngBZYWScCWD8ZDeXLJ",
        database="analysis_development",
    )

    # Only Benesse + Math + scaled > 0 (real scores only)
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
          AND course_name LIKE '%数学%'
          AND scaled IS NOT NULL
          AND scaled > 0
    """

    df = pd.read_sql(query, conn)
    conn.close()
    print(f"📥 Loaded {len(df):,} Benesse Math rows from DB (scaled > 0)")
    return df


# -----------------------------
# Helpers to parse course_name / name
# -----------------------------
def extract_year_from_course(course_name: str):
    """From '2020年度数学[中1]A組' → 2020."""
    m = re.search(r"(\d{4})年度", str(course_name))
    return int(m.group(1)) if m else None


def extract_grade_jhs(course_name: str):
    """
    Normalize grade to '中1', '中2', '中3' for JHS.

    Handles patterns like:
      - '2019年度前期数学[中1]A組前半'
      - '2020年度数学[中2]B組'
      - '2022年度2年A組数学[中学]'
      - '2023年度中学1年A組[数学]'
      - '2024年度中学3年C組[英語]'
    """
    s = str(course_name)

    # 1) [中1] / [中2] / [中3] (2019–2021, plus some 2020)
    m = re.search(r"\[中([123])\]", s)
    if m:
        return f"中{m.group(1)}"

    # 2) '中学1年A組[数学]' / '中学2年B組[英語]' (2023–2024)
    m = re.search(r"中学([123])年", s)
    if m:
        return f"中{m.group(1)}"

    # 3) '1年A組数学[中学]' etc. (2022)
    m = re.search(r"([123])年.*数学\[中学\]", s)
    if m:
        return f"中{m.group(1)}"

    # 4) If needed, you can add a similar pattern for 英語/国語:
    m = re.search(r"([123])年.*英語\[中学\]", s)
    if m:
        return f"中{m.group(1)}"
    m = re.search(r"([123])年.*国語\[中学\]", s)
    if m:
        return f"中{m.group(1)}"

    return None



def extract_exam_round(exam_name: str):
    """
    From '...Benesse模試1回' → 1, '...模試2回' → 2.
    If missing, return 1 as fallback.
    """
    s = str(exam_name)
    m = re.search(r"模試(\d)回", s)
    if m:
        return int(m.group(1))
    return 1


# -----------------------------
# Analysis / plotting
# -----------------------------
def analyze_2020_to_2022_cohort(df: pd.DataFrame, out_dir: Path):
    """
    Take the 2020 中1 cohort and compare their
    Benesse Math scores in:
      - 2020 (中1) 第1回 & 第2回
      - 2022 (中3) 第1回

    Outputs:
      1) Line plot with three time points per student
      2) Scatter plot: 2020 vs 2022 yearly averages
    """
    df = df.copy()
    df["year"] = df["course_name"].apply(extract_year_from_course)
    df["grade"] = df["course_name"].apply(extract_grade_jhs)
    df["exam_round"] = df["name"].apply(extract_exam_round)
    df["score_percent"] = df["scaled"] * 100

    df = df[df["year"].notna()]
    df = df[df["grade"].notna()]
    df = df[df["student_id"].notna()]

    print("\n=== Year counts (after parsing) ===")
    print(df["year"].value_counts().sort_index())
    print("\n=== Grade counts (after parsing) ===")
    print(df["grade"].value_counts())

    # -----------------------------
    # Identify 2020 中1 cohort
    # -----------------------------
    cohort_mask_2020_chu1 = (df["year"] == 2020) & (df["grade"] == "中1")
    cohort_students = df.loc[cohort_mask_2020_chu1, "student_id"].dropna().unique()

    print(f"\n2020 中1 cohort: {len(cohort_students)} students found")

    if len(cohort_students) == 0:
        print("⚠️ No 2020 中1 cohort students found. Aborting analysis.")
        return

    jp_font = fm.FontProperties(family=JP_FONT_NAME)
    sns.set_style("whitegrid")

    # =====================================================
    # (A) Line plot: 2020 第1回 → 2020 第2回 → 2022 第1回
    # =====================================================
    coh_line = df[
    (df["student_id"].isin(cohort_students)) &
    (
        # 2020: 中1, 第1回・第2回
        (
            (df["year"] == 2020) &
            (df["grade"] == "中1") &
            (df["exam_round"].isin([1, 2]))
        )
        |
        # 2022: same cohort, 第1回 （grade check removed）
        (
            (df["year"] == 2022) &
            (df["exam_round"] == 1)
        )
    )
].copy()


    if coh_line.empty:
        print("⚠️ No rows for 2020中1 (第1回/第2回) or 2022中3 (第1回). Skipping line plot.")
    else:
        # average per (student, year, exam_round)
        summary_line = (
            coh_line.groupby(["student_id", "year", "exam_round"], as_index=False)["score_percent"]
            .mean()
        )

        # create time_key to pivot on
        def make_time_key(row):
            return f"{int(row['year'])}_r{int(row['exam_round'])}"

        summary_line["time_key"] = summary_line.apply(make_time_key, axis=1)

        # We want exactly these three time points in this order
        time_order = ["2020_r1", "2020_r2", "2022_r1"]
        label_map = {
            "2020_r1": "2020年度 中1 第1回",
            "2020_r2": "2020年度 中1 第2回",
            "2022_r1": "2022年度 中3 第1回",
        }

        pivot_line = summary_line.pivot(
            index="student_id", columns="time_key", values="score_percent"
        )

        # Keep only the columns we care about (some may be missing)
        pivot_line = pivot_line.reindex(columns=time_order)

        print(f"\nLine-plot students (at least one of the 3 points): {len(pivot_line)}")

        x_positions = list(range(len(time_order)))
        x_labels = [label_map[k] for k in time_order]

        plt.figure(figsize=(11, 7))
        ax = plt.gca()

        # draw each student's trajectory (NaNs will break the line)
        for sid, row in pivot_line.iterrows():
            ax.plot(
                x_positions,
                row.values,
                marker="o",
                linewidth=1.0,
                alpha=0.25,
            )

        # median line at each time point
        medians = [pivot_line[col].median(skipna=True) for col in time_order]
        ax.plot(
            x_positions,
            medians,
            marker="o",
            linewidth=3.0,
            alpha=0.95,
            color="black",
            label="中央値",
        )

        ax.set_xticks(x_positions)
        ax.set_xticklabels(x_labels, fontproperties=jp_font, rotation=0)

        ax.set_ylabel("得点（％）", fontproperties=jp_font)
        for lbl in ax.get_yticklabels():
            lbl.set_fontproperties(jp_font)

        ax.set_title(
            "2020年度 中1コホートのBenesse数学スコア推移（2020第1回→2020第2回→2022第1回）",
            fontproperties=jp_font,
        )
        ax.set_ylim(35, 100)
        ax.legend(prop=jp_font)

        plt.tight_layout()
        out_path_lines = out_dir / "math_cohort_2020chu1_2020r1_2020r2_2022r1_lines.png"
        plt.savefig(out_path_lines, dpi=200)
        plt.close()
        print(f"💾 Saved line plot: {out_path_lines.resolve()}")

    # =====================================================
    # (B) Scatter plot: 2020 vs 2022 yearly averages
    # =====================================================
    coh_year = df[
        (df["student_id"].isin(cohort_students)) &
        (df["year"].isin([2020, 2022]))
    ].copy()

    if coh_year.empty:
        print("⚠️ No 2020/2022 rows for cohort. Skipping scatter.")
        return

    summary_year = (
        coh_year.groupby(["student_id", "year"], as_index=False)["score_percent"]
        .mean()
    )

    pivot_year = summary_year.pivot(
        index="student_id", columns="year", values="score_percent"
    )

    pivot_year = pivot_year.dropna(subset=[2020, 2022])
    print(f"\nStudents with BOTH 2020 and 2022 yearly scores: {len(pivot_year)}")

    if pivot_year.empty:
        print("⚠️ No students with both 2020 and 2022 scores. Skipping scatter.")
        return

    print("\n=== Basic stats (yearly averages) ===")
    print("2020 中1 mean:", pivot_year[2020].mean())
    print("2022 中3 mean:", pivot_year[2022].mean())
    print("Mean change (2022 - 2020):", (pivot_year[2022] - pivot_year[2020]).mean())

    plt.figure(figsize=(7, 7))
    ax = plt.gca()

    ax.scatter(pivot_year[2020], pivot_year[2022], alpha=0.5)

    # y = x reference line
    min_score = min(pivot_year[2020].min(), pivot_year[2022].min())
    max_score = max(pivot_year[2020].max(), pivot_year[2022].max())
    ax.plot(
        [min_score, max_score],
        [min_score, max_score],
        linestyle="--",
        linewidth=1.0,
        color="gray",
        label="y = x",
    )

    ax.set_xlabel("2020年度 中1 Benesse数学（％）", fontproperties=jp_font)
    ax.set_ylabel("2022年度 中3 Benesse数学（％）", fontproperties=jp_font)

    for lbl in ax.get_xticklabels() + ax.get_yticklabels():
        lbl.set_fontproperties(jp_font)

    ax.set_title(
        "2020年度 中1コホートのBenesse数学：2020 vs 2022（年平均）",
        fontproperties=jp_font,
    )
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.legend(prop=jp_font)

    plt.tight_layout()
    out_path_scatter = out_dir / "math_cohort_2020chu1_2022chu3_scatter.png"
    plt.savefig(out_path_scatter, dpi=200)
    plt.close()
    print(f"💾 Saved scatter plot: {out_path_scatter.resolve()}")


# -----------------------------
# Main
# -----------------------------
def main():
    df = load_benesse_math_from_db()
    out_dir = Path(".")
    analyze_2020_to_2022_cohort(df, out_dir)


if __name__ == "__main__":
    main()
