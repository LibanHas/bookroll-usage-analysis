# scripts/plot_english_2022cohort_lines.py

from pathlib import Path
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import seaborn as sns
import pandas as pd

# ---------- CONFIG ----------
JP_FONT_NAME = "Hiragino Sans"
BASE_DIR = Path(__file__).resolve().parents[1]
IN_CSV  = BASE_DIR / "english_2022cohort_long.csv"
OUT_PNG = BASE_DIR / "english_2022cohort_5points_lines.png"

# Set Japanese font
matplotlib.rcParams["font.family"] = JP_FONT_NAME
matplotlib.rcParams["font.sans-serif"] = [JP_FONT_NAME]
matplotlib.rcParams["axes.unicode_minus"] = False


def main():
    print(f"📥 Loading cohort CSV: {IN_CSV}")
    df = pd.read_csv(IN_CSV)

    print("\n=== First few rows ===")
    print(df.head())

    # Make sure we have what we expect
    needed_cols = [
        "student_id",
        "year",
        "grade",
        "subject",
        "exam_round",
        "score_percent",
        "time_label",
    ]
    missing = [c for c in needed_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in CSV: {missing}")

    # We expect exactly these 5 labels, in this order
    ordered_labels = [
        "2022年度 中1 第1回",
        "2023年度 中2 第1回",
        "2023年度 中2 第2回",
        "2024年度 中3 第1回",
        "2024年度 中3 第2回",
    ]

    # Filter to only those labels (in case something extra sneaks in)
    df = df[df["time_label"].isin(ordered_labels)].copy()

    # Average per student per time point (should mostly be 1 row each)
    summary = (
        df.groupby(["student_id", "time_label"], as_index=False)["score_percent"]
          .mean()
    )

    # Pivot to wide: one row per student, 5 columns for the 5 tests
    pivot = summary.pivot(
        index="student_id",
        columns="time_label",
        values="score_percent"
    )

    # Ensure column order
    pivot = pivot.reindex(columns=ordered_labels)

    print("\n=== Pivot shape before dropping NaNs ===")
    print(pivot.shape)

    # Keep students who have *all 5* data points
    pivot_complete = pivot.dropna()
    print("Rows after requiring all 5 points:", len(pivot_complete))

    if pivot_complete.empty:
        print("⚠️ No students with complete 5-point data. Nothing to plot.")
        return

    # Basic stats
    print("\n=== Basic stats (mean by time_point) ===")
    for label in ordered_labels:
        print(f"{label}: mean = {pivot_complete[label].mean():.2f}")

    # -------------------
    # Plot
    # -------------------
    sns.set_style("whitegrid")
    jp_font = fm.FontProperties(family=JP_FONT_NAME)

    x_positions = list(range(len(ordered_labels)))

    plt.figure(figsize=(12, 7))
    ax = plt.gca()

    # Individual trajectories
    for sid, row in pivot_complete.iterrows():
        ax.plot(
            x_positions,
            row.values,
            marker="o",
            linewidth=1.0,
            alpha=0.25,
        )

    # Median line
    medians = pivot_complete.median(axis=0)
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
        "2022年度 中1コホートのBenesse英語スコア推移\n"
        "（2022中1第1回 → 2023中2第1・第2回 → 2024中3第1・第2回）",
        fontproperties=jp_font,
    )

    ax.grid(True, alpha=0.3)
    ax.legend(prop=jp_font)

    plt.tight_layout()
    plt.savefig(OUT_PNG, dpi=200)
    plt.close()
    print(f"\n💾 Saved 5-point line plot to: {OUT_PNG.resolve()}")


if __name__ == "__main__":
    main()
