#!/usr/bin/env python3
"""
Benesse Boxplots by Course

Reads benesse_scores.csv from the project root and creates a
horizontal boxplot per course, sorted by median score.
"""

import os
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import font_manager, rcParams


# ---------------------------------------------------------------------
# 0. Optional: try to use a Japanese-capable font
# ---------------------------------------------------------------------
def set_japanese_font():
    """
    Try to set a Japanese-capable font if available.
    Adjust the list below based on what you have installed.
    """
    candidate_fonts = [
        "Hiragino Sans",        # macOS
        "Hiragino Kaku Gothic ProN",
        "Noto Sans CJK JP",
        "Yu Gothic",
        "IPAexGothic",
    ]

    available = {f.name for f in font_manager.fontManager.ttflist}
    for name in candidate_fonts:
        if name in available:
            rcParams["font.family"] = name
            print(f"🈶 Using font: {name}")
            return

    print("⚠ No Japanese font from candidate list found; labels may show tofu.")


set_japanese_font()

sns.set(style="whitegrid")

# ---------------------------------------------------------------------
# 1. Load data
# ---------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
csv_path = ROOT / "benesse_scores.csv"

print(f"📄 Loading {csv_path} ...")
df = pd.read_csv(csv_path)

# We assume:
#   - quiz: 0–100
#   - scaled: 0–1 and scaled == quiz / 100
# If you prefer, you can switch x="scaled" and multiply by 100 in the plot.
df["score_percent"] = df["quiz"].astype(float)

# Sanity check: drop any impossible values just in case
df = df[(df["score_percent"] >= 0) & (df["score_percent"] <= 100)].copy()

# ---------------------------------------------------------------------
# 2. Order courses by median score
# ---------------------------------------------------------------------
course_medians = (
    df.groupby("course_name")["score_percent"]
    .median()
    .sort_values(ascending=False)   # highest → lowest
)

ordered_courses = course_medians.index.tolist()

df["course_name_ordered"] = pd.Categorical(
    df["course_name"], categories=ordered_courses, ordered=True
)

print("\nTop 5 courses by median score:")
print(course_medians.head())

print("\nBottom 5 courses by median score:")
print(course_medians.tail())

# ---------------------------------------------------------------------
# 3. Plot boxplots (one per course)
# ---------------------------------------------------------------------
n_courses = len(ordered_courses)
# Height scales with number of courses so labels remain readable
fig_height = max(6, 0.3 * n_courses)

plt.figure(figsize=(10, fig_height))

ax = sns.boxplot(
    data=df,
    x="score_percent",
    y="course_name_ordered",
    orient="h",
    showfliers=True,   # show outliers
    linewidth=0.8,
)

ax.set_title("Benesse Scores – Course Distributions (Boxplots)", fontsize=14)
ax.set_xlabel("Score (%)", fontsize=12)
ax.set_ylabel("Course Name", fontsize=12)

ax.set_xlim(0, 100)

plt.tight_layout()

out_path = ROOT / "benesse_boxplots_by_course.png"
plt.savefig(out_path, dpi=200)
print(f"\n💾 Saved: {out_path}")
