#!/usr/bin/env python3
"""
Quick visualizations for course_student_scores (scores-only CSV).

Usage:
    python3 scripts/visualize_scores.py
    # or, with a different file:
    python3 scripts/visualize_scores.py my_scores.csv
"""

import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def main():
    # Nice default look
    sns.set_theme(style="whitegrid")

    # ----------------------------
    # 1. Decide which CSV to load
    # ----------------------------
    if len(sys.argv) > 1:
        csv_path = Path(sys.argv[1])
    else:
        csv_path = Path("analysis_development_scores_only.csv")

    if not csv_path.exists():
        print(f"❌ CSV file not found: {csv_path}")
        sys.exit(1)

    print(f"📄 Loading: {csv_path}")
    df = pd.read_csv(csv_path)

    if "scaled" not in df.columns:
        print("❌ CSV does not contain a 'scaled' column.")
        print(f"Columns found: {list(df.columns)}")
        sys.exit(1)

    # ----------------------------
    # 2. Basic counts / data quality
    # ----------------------------
    total = len(df)
    zeros = (df["scaled"] == 0).sum()
    negatives = (df["scaled"] < 0).sum()
    over_one = (df["scaled"] > 1).sum()
    valid_mask = (df["scaled"] > 0) & (df["scaled"] <= 1)
    valid = valid_mask.sum()

    print("\n=== Data quality summary ===")
    print(f"Total rows:                      {total}")
    print(f"Zeros (scaled == 0):             {zeros} ({zeros / total * 100:.2f}%)")
    print(f"Negatives (scaled < 0):          {negatives} ({negatives / total * 100:.2f}%)")
    print(f"> 1.0 (scaled > 1):              {over_one} ({over_one / total * 100:.2f}%)")
    print(f"Valid (0 < scaled <= 1):         {valid} ({valid / total * 100:.2f}%)")

    # ----------------------------
    # 3. Prepare cleaned scores
    # ----------------------------
    scores_clean = df[valid_mask].copy()
    scores_clean["score_percent"] = scores_clean["scaled"] * 100

    print("\n=== Valid score summary (0 < scaled ≤ 1) ===")
    print(scores_clean["score_percent"].describe())

    # ----------------------------
    # 4. Plot 1: Data quality breakdown (seaborn barplot)
    # ----------------------------
    data_quality = pd.DataFrame({
        "category": ["Zero", "Negative", "> 1.0", "Valid (0–1)"],
        "count": [zeros, negatives, over_one, valid],
    })

    plt.figure(figsize=(6, 4))
    ax = sns.barplot(data=data_quality, x="category", y="count")
    ax.set_ylabel("Number of records")
    ax.set_title("course_student_scores: Data Quality Breakdown")

    # Show counts on top of bars
    for container in ax.containers:
        ax.bar_label(container, fmt="%d", padding=3)

    plt.tight_layout()
    plt.savefig("scores_data_quality.png", dpi=150)
    print("\n💾 Saved: scores_data_quality.png")

    # ----------------------------
    # 5. Plot 2: Distribution of valid scores (seaborn histplot + KDE)
    # ----------------------------
    plt.figure(figsize=(8, 5))
    sns.histplot(scores_clean["score_percent"], bins=20, kde=True)
    plt.xlabel("Score (%)")
    plt.ylabel("Number of students")
    plt.title("Distribution of Valid Scores (0 < scaled ≤ 1)")
    plt.tight_layout()
    plt.savefig("scores_distribution.png", dpi=150)
    print("💾 Saved: scores_distribution.png")

    # If you want to see the plots interactively as well, uncomment:
    # plt.show()


if __name__ == "__main__":
    main()
