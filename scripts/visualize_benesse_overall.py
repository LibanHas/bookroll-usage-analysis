#!/usr/bin/env python3
"""
visualize_benesse_overall.py

Overall score distribution for Benesse tests.
- Uses benesse_scores.csv in the project root.
- Creates:
    1) benesse_overall_hist.png   (histogram + KDE)
    2) benesse_overall_boxplot.png (boxplot)
"""

from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def main():
    # ------------------------------------------------------------------
    # 1) Load data
    # ------------------------------------------------------------------
    project_root = Path(__file__).resolve().parents[1]
    csv_path = project_root / "benesse_scores.csv"

    print(f"📄 Loading: {csv_path.name}")
    df = pd.read_csv(csv_path)

    # We know Benesse rows are clean: quiz 0–100, scaled 0–1
    # Convert scaled → percentage for easier interpretation
    df["score_percent"] = df["scaled"] * 100

    # Basic summary in the terminal
    print("\n=== BASIC SUMMARY (Benesse scores) ===")
    print(df["score_percent"].describe())

    # ------------------------------------------------------------------
    # 2) Plot settings
    # ------------------------------------------------------------------
    sns.set(style="whitegrid")  # use Seaborn styling

    # ------------------------------------------------------------------
    # 3) Histogram + KDE
    # ------------------------------------------------------------------
    plt.figure(figsize=(10, 6))
    sns.histplot(
        df["score_percent"],
        bins=20,
        kde=True,
        stat="count",
    )

    mean_val = df["score_percent"].mean()
    median_val = df["score_percent"].median()

    # Add vertical lines for mean & median
    plt.axvline(mean_val, linestyle="--", linewidth=1.5, label=f"Mean = {mean_val:.1f}")
    plt.axvline(
        median_val,
        linestyle=":",
        linewidth=1.5,
        label=f"Median = {median_val:.1f}",
    )

    plt.title("Benesse Scores – Overall Distribution")
    plt.xlabel("Score (%)")
    plt.ylabel("Number of students")
    plt.legend()

    out_path_hist = project_root / "benesse_overall_hist.png"
    plt.tight_layout()
    plt.savefig(out_path_hist, dpi=150)
    plt.close()
    print(f"💾 Saved: {out_path_hist.name}")

    # ------------------------------------------------------------------
    # 4) Boxplot
    # ------------------------------------------------------------------
    plt.figure(figsize=(10, 2.5))
    sns.boxplot(x=df["score_percent"])

    plt.title("Benesse Scores – Boxplot")
    plt.xlabel("Score (%)")

    out_path_box = project_root / "benesse_overall_boxplot.png"
    plt.tight_layout()
    plt.savefig(out_path_box, dpi=150)
    plt.close()
    print(f"💾 Saved: {out_path_box.name}")

    print("\n✅ Visualization complete.")


if __name__ == "__main__":
    main()
