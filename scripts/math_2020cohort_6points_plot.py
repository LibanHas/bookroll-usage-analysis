# scripts/math_2020cohort_6points_plot.py

from pathlib import Path
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import pandas as pd

# ---- HARD-SET JAPANESE FONT HERE ----
JP_FONT_NAME = "Hiragino Sans"

matplotlib.rcParams["font.family"] = JP_FONT_NAME
matplotlib.rcParams["font.sans-serif"] = [JP_FONT_NAME]
matplotlib.rcParams["axes.unicode_minus"] = False

BASE_DIR = Path(".").resolve()
INPUT_CSV = BASE_DIR / "math_2020cohort_6points_long.csv"
OUT_PNG   = BASE_DIR / "math_2020cohort_6points_lines.png"


def main():
    print(f"📥 Loading cohort CSV: {INPUT_CSV}")
    df = pd.read_csv(INPUT_CSV)

    # Sanity check
    print("\n=== Head of data ===")
    print(df.head())
    print("\n=== time_label counts ===")
    print(df["time_label"].value_counts())

    # Ensure numeric
    df["score_percent"] = df["score_percent"].astype(float)

    # Desired time order (x-axis)
    ordered_labels = [
        "2020年度 中1 第1回",
        "2020年度 中1 第2回",
        "2021年度 中2 第1回",
        "2021年度 中2 第2回",
        "2022年度 中3 第1回",
        "2022年度 中3 第2回",
    ]

    # Filter to just those labels in case there’s any extra noise
    df = df[df["time_label"].isin(ordered_labels)].copy()

    # Pivot: one row per student, columns = time points
    pivot = df.pivot_table(
        index="student_id",
        columns="time_label",
        values="score_percent",
        aggfunc="mean",
    )

    # Ensure correct column order
    pivot = pivot.reindex(columns=ordered_labels)

    print("\n=== Pivot shape (students x timepoints) ===")
    print(pivot.shape)

    # Basic stats
    print("\n=== Basic stats (Math) ===")
    for label in ordered_labels:
        col = pivot[label]
        mean_val = col.mean()
        count_val = col.notna().sum()
        print(f"{label}: n = {count_val}, mean = {mean_val:.2f}")

    # Plot
    jp_font = fm.FontProperties(family=JP_FONT_NAME)
    x_positions = list(range(len(ordered_labels)))

    plt.figure(figsize=(12, 7))
    ax = plt.gca()

    # Individual trajectories
    for sid, row in pivot.iterrows():
        ax.plot(
            x_positions,
            row.values,
            marker="o",
            linewidth=1.0,
            alpha=0.25,
        )

    # Median line (ignoring NaNs)
    medians = pivot.median(axis=0, skipna=True)
    ax.plot(
        x_positions,
        medians.values,
        marker="o",
        linewidth=3.0,
        alpha=0.95,
        color="black",
        label="中央値",
    )

    # X axis: ticks, labels, no side gaps
    ax.set_xticks(x_positions)
    ax.set_xticklabels(ordered_labels, fontproperties=jp_font, rotation=20, ha="right")
    ax.set_xlim(x_positions[0], x_positions[-1])

    # Y axis: fixed range and ticks
    ax.set_ylim(20, 100)                  # percent scale
    ax.set_yticks(range(20, 101, 10))     # 20, 30, ..., 100

    ax.set_ylabel("得点（％）", fontproperties=jp_font)
    for lbl in ax.get_yticklabels():
        lbl.set_fontproperties(jp_font)

    # Right-hand mirrored y-axis
    ax_right = ax.twinx()
    ax_right.set_ylim(ax.get_ylim())
    ax_right.set_yticks(ax.get_yticks())
    for lbl in ax_right.get_yticklabels():
        lbl.set_fontproperties(jp_font)
    ax_right.set_ylabel("得点（％）", fontproperties=jp_font)

    ax.set_title(
        "2020年度 中1コホートのベネッセ数学スコア推移（中1第1回〜中3第2回）",
        fontproperties=jp_font,
    )

    ax.grid(True, alpha=0.3)
    ax.legend(prop=jp_font)

    plt.tight_layout()
    plt.savefig(OUT_PNG, dpi=200)
    plt.close()
    print(f"\n💾 Saved Math 6-point line plot: {OUT_PNG.resolve()}")


if __name__ == "__main__":
    main()
