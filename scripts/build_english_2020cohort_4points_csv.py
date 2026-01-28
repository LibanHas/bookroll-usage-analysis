# scripts/plot_english_2020cohort_4points.py

from pathlib import Path
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import pandas as pd

# --------------------------
# Font (Mac, Hiragino Sans)
# --------------------------
JP_FONT_NAME = "Hiragino Sans"

matplotlib.rcParams["font.family"] = JP_FONT_NAME
matplotlib.rcParams["font.sans-serif"] = [JP_FONT_NAME]
matplotlib.rcParams["axes.unicode_minus"] = False

BASE_DIR = Path(".").resolve()
IN_CSV   = BASE_DIR / "english_2020cohort_4points_long.csv"
OUT_PNG  = BASE_DIR / "english_2020cohort_4points_lines.png"


def main():
    print(f"📥 Loading cohort CSV from: {IN_CSV}")
    df = pd.read_csv(IN_CSV)

    print("\n=== Head of data ===")
    print(df.head())

    # Just in case: ensure the types are sensible
    df["student_id"] = df["student_id"].astype(int)
    df["year"]       = df["year"].astype(int)
    df["exam_round"] = df["exam_round"].astype(int)
    df["score_percent"] = df["score_percent"].astype(float)

    # We expect exactly these 4 labels, but let's assert/check:
    print("\n=== time_label value counts ===")
    print(df["time_label"].value_counts())

    # Pivot to wide: one row per student, one column per time point
    pivot = df.pivot(
        index="student_id",
        columns="time_label",
        values="score_percent",
    )

    # Desired order of time points (must match your labels in CSV)
    ordered_labels = [
        "2020年度 中1 第2回",
        "2021年度 中2 第1回",
        "2021年度 中2 第2回",
        "2022年度 中3 第1回",
    ]

    # Keep only the columns we care about (in this order)
    pivot = pivot.reindex(columns=ordered_labels)

    # Keep only students who have ALL 4 scores
    before_drop = len(pivot)
    pivot = pivot.dropna()
    after_drop = len(pivot)

    print(f"\nRows (students) before dropna: {before_drop}")
    print(f"Rows (students) after  dropna: {after_drop}")

    if pivot.empty:
        print("⚠️ No students with complete 4-point data. Abort plot.")
        return

    # Basic stats
    print("\n=== Basic stats ===")
    for lbl in ordered_labels:
        print(f"{lbl}: mean = {pivot[lbl].mean():.2f}, median = {pivot[lbl].median():.2f}")

    # --------------------------
    # Plotting
    # --------------------------
    jp_font = fm.FontProperties(family=JP_FONT_NAME)
    x_positions = list(range(len(ordered_labels)))

    plt.figure(figsize=(12, 7))
    ax = plt.gca()

    # Individual student trajectories
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
    ax.set_xticklabels(ordered_labels, fontproperties=jp_font, rotation=15)

    ax.set_ylabel("得点（％）", fontproperties=jp_font)
    for lbl in ax.get_yticklabels():
        lbl.set_fontproperties(jp_font)

    ax.set_title(
        "2020年度 中1コホートのBenesse英語スコア推移\n"
        "（2020中1第2回 → 2021中2第1・第2回 → 2022中3第1回）",
        fontproperties=jp_font,
    )

    # Adjust y-limits if you like; scores are 0–100
    ax.set_ylim(0, 100)

    ax.legend(prop=jp_font)
    plt.tight_layout()
    plt.savefig(OUT_PNG, dpi=200)
    plt.close()

    print(f"\n💾 Saved line plot to: {OUT_PNG.resolve()}")


if __name__ == "__main__":
    main()
