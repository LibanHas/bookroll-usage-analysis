# scripts/plot_math_2021cohort_4points.py

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

BASE_DIR   = Path(".").resolve()
INPUT_CSV  = BASE_DIR / "math_2021cohort_4points_long.csv"
OUT_PNG    = BASE_DIR / "math_2021cohort_4points_lines.png"


def main():
    print(f"📥 Loading cohort CSV: {INPUT_CSV}")
    df = pd.read_csv(INPUT_CSV)

    print("\n=== Basic info ===")
    print("Rows:", len(df))
    print("Students (unique):", df["student_id"].nunique())
    print("Years:", df["year"].unique())
    print("Grades:", df["grade"].unique())
    print("Exam rounds:", df["exam_round"].unique())
    print("Time labels:", df["time_label"].unique())

    # If raw_score is already a percentage, leave as is.
    # If it's 0–1, uncomment the next line:
    # df["raw_score"] = df["raw_score"] * 100.0

    # Work out the intended time order
    order = (
        df[["year", "grade", "exam_round", "time_label"]]
        .drop_duplicates()
        .sort_values(["year", "grade", "exam_round"])
    )
    ordered_labels = order["time_label"].tolist()
    print("\nTime order:", ordered_labels)

    # Filter just in case and enforce categorical ordering
    df = df[df["time_label"].isin(ordered_labels)].copy()
    df["time_label"] = pd.Categorical(
        df["time_label"], categories=ordered_labels, ordered=True
    )

    # Pivot: one row per student, columns = time points
    pivot = df.pivot_table(
        index="student_id",
        columns="time_label",
        values="raw_score",
        aggfunc="mean",
    )

    # Ensure correct column order
    pivot = pivot.reindex(columns=ordered_labels)

    print("\n=== Pivot shape (students x timepoints) ===")
    print(pivot.shape)

    # Basic stats
    print("\n=== Basic stats (Math, raw_score) ===")
    for label in ordered_labels:
        col = pivot[label]
        mean_val = col.mean()
        count_val = col.notna().sum()
        print(f"{label}: n = {count_val}, mean = {mean_val:.2f}")

    # ---- Plot ----
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
    ax.set_xticklabels(
        ordered_labels,
        fontproperties=jp_font,
        rotation=20,
        ha="right",
    )
    ax.set_xlim(x_positions[0], x_positions[-1])

    # Y axis: fixed range and ticks
    ax.set_ylim(0, 100)                  # tweak if needed
    ax.set_yticks(range(0, 101, 10))     # 30, 40, ..., 100

    ax.set_ylabel("得点", fontproperties=jp_font)
    for lbl in ax.get_yticklabels():
        lbl.set_fontproperties(jp_font)

    # Mirror y-axis on the right
    ax_right = ax.twinx()
    ax_right.set_ylim(ax.get_ylim())
    ax_right.set_yticks(ax.get_yticks())
    for lbl in ax_right.get_yticklabels():
        lbl.set_fontproperties(jp_font)
    ax_right.set_ylabel("得点", fontproperties=jp_font)

    ax.set_title(
        "2021年度 中1コホートのベネッセ数学スコア推移（中1第1回〜中2第2回）",
        fontproperties=jp_font,
    )

    ax.grid(True, alpha=0.3)
    ax.legend(prop=jp_font)

    plt.tight_layout()
    plt.savefig(OUT_PNG, dpi=200)
    plt.close()
    print(f"\n💾 Saved Math 4-point line plot: {OUT_PNG.resolve()}")


if __name__ == "__main__":
    main()
