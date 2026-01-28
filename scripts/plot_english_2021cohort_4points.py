# scripts/plot_english_2021cohort_4points.py

from pathlib import Path
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import pandas as pd
import seaborn as sns

# ---- HARD-SET JAPANESE FONT HERE ----
JP_FONT_NAME = "Hiragino Sans"

matplotlib.rcParams["font.family"] = JP_FONT_NAME
matplotlib.rcParams["font.sans-serif"] = [JP_FONT_NAME]
matplotlib.rcParams["axes.unicode_minus"] = False

print("matplotlib font.family =", matplotlib.rcParams["font.family"])
print("matplotlib font.sans-serif =", matplotlib.rcParams["font.sans-serif"])
# -------------------------------------

BASE_DIR = Path(".").resolve()
DATA_CSV = BASE_DIR / "english_2021cohort_4points_long.csv"
OUT_PNG  = BASE_DIR / "english_2021cohort_4points_lines.png"

# Time points in the order we want on the x-axis
ORDERED_LABELS = [
    "2021年度 中1 第1回",
    "2021年度 中1 第2回",
    "2022年度 中2 第1回",
    "2022年度 中2 第2回",
]


def main():
    print(f"📥 Loading {DATA_CSV.name} ...")
    df = pd.read_csv(DATA_CSV)

    # Just in case, filter to the four labels we care about
    df = df[df["time_label"].isin(ORDERED_LABELS)].copy()

    # One average score per student per time point (defensive)
    summary = (
        df.groupby(["student_id", "time_label"], as_index=False)["score_percent"]
          .mean()
    )

    # Pivot: rows = students, cols = time points
    pivot = summary.pivot(
        index="student_id",
        columns="time_label",
        values="score_percent"
    )

    # Re-order columns to our desired axis order
    pivot = pivot.reindex(columns=ORDERED_LABELS)

    # If you want *only* students with all 4 data points:
    pivot_full = pivot.dropna()
    print(f"Students with all 4 points: {len(pivot_full)}")

    if pivot_full.empty:
        print("⚠️ No students with complete 4-point records. Nothing to plot.")
        return

    # Basic stats printout
    print("\n=== Basic stats (English 2021 cohort) ===")
    for label in ORDERED_LABELS:
        print(f"{label}: mean = {pivot_full[label].mean():.2f}")

    sns.set_style("whitegrid")
    jp_font = fm.FontProperties(family=JP_FONT_NAME)

    x_positions = list(range(len(ORDERED_LABELS)))

    plt.figure(figsize=(12, 7))
    ax = plt.gca()

    # Individual trajectories
    for sid, row in pivot_full.iterrows():
        ax.plot(
            x_positions,
            row.values,
            marker="o",
            linewidth=1.0,
            alpha=0.25,
        )

    # Median line
    medians = pivot_full.median(axis=0)
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
        ORDERED_LABELS,
        fontproperties=jp_font,
        rotation=20,
        ha="right",
    )
    ax.set_xlim(x_positions[0], x_positions[-1])

    # Y axis: fixed range and ticks
    ax.set_ylim(0, 100)
    ax.set_yticks(range(0, 101, 10))

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
        "2021年度 中1コホートのBenesse英語スコア推移（2021第1・第2回→2022中2第1・第2回）",
        fontproperties=jp_font,
    )

    ax.grid(True, alpha=0.3)
    ax.legend(prop=jp_font)

    plt.tight_layout()
    plt.savefig(OUT_PNG, dpi=200)
    plt.close()
    print(f"💾 Saved plot to: {OUT_PNG.resolve()}")


if __name__ == "__main__":
    main()
