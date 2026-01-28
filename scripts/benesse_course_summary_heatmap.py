#!/usr/bin/env python3
"""
benesse_course_summary_heatmap.py

Summarise Benesse scores per course and make a heatmap:
- mean score
- median score
- std deviation
- % of students below 50%

Input : benesse_scores.csv  (in project root)
Output: benesse_course_summary.csv
        benesse_course_summary_heatmap.png
"""

import os
import math
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import font_manager as fm

# -----------------------------------------------------------
# 1. Paths & loading
# -----------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_DIR, "benesse_scores.csv")
OUT_CSV = os.path.join(BASE_DIR, "benesse_course_summary.csv")
OUT_PNG = os.path.join(BASE_DIR, "benesse_course_summary_heatmap.png")

print(f"📄 Loading {CSV_PATH} ...")
df = pd.read_csv(CSV_PATH)

# -----------------------------------------------------------
# 2. Basic preparation
# -----------------------------------------------------------

# Ensure we have a percentage column (0–100)
if "score_percent" not in df.columns:
    df["score_percent"] = df["scaled"] * 100

# Keep only rows with non-null score_percent and a course_name
df = df[df["score_percent"].notna() & df["course_name"].notna()].copy()

print(f"Rows after filtering non-null score_percent & course_name: {len(df)}")

# -----------------------------------------------------------
# 3. Aggregate per course
# -----------------------------------------------------------

def agg_low_rate(x, threshold=50.0):
    """Percentage of scores < threshold."""
    if len(x) == 0:
        return math.nan
    return (x < threshold).mean() * 100.0

grouped = df.groupby("course_name")["score_percent"].agg(
    mean_score="mean",
    median_score="median",
    std_score="std",
    n_students="count",
    low_rate=lambda x: agg_low_rate(x, 50.0),
)

# Sort courses by median score (hard → easy)
grouped_sorted = grouped.sort_values(by="median_score", ascending=True)

print("\n=== Course summary (head) ===")
print(grouped_sorted.head(10))

# Save summary to CSV
grouped_sorted.to_csv(OUT_CSV, encoding="utf-8-sig")
print(f"\n💾 Saved summary CSV: {OUT_CSV}")

# -----------------------------------------------------------
# 4. Set Japanese-capable font (macOS-friendly)
# -----------------------------------------------------------

jp_candidates = [
    "Hiragino Sans",
    "Hiragino Kaku Gothic ProN",
    "Yu Gothic",
    "YuGothic",
    "Noto Sans CJK JP",
]

chosen_font = None
for name in jp_candidates:
    try:
        font_path = fm.findfont(name, fallback_to_default=False)
        if font_path and os.path.exists(font_path):
            chosen_font = name
            break
    except Exception:
        continue

if chosen_font is None:
    print("⚠️ No Japanese font from candidate list found. Using default font.")
else:
    print(f"🈶 Using font: {chosen_font}")
    plt.rcParams["font.family"] = chosen_font

# -----------------------------------------------------------
# 5. Prepare data for heatmap
# -----------------------------------------------------------

# We only heatmap the main numeric stats
heatmap_cols = ["mean_score", "median_score", "std_score", "low_rate"]
heatmap_df = grouped_sorted[heatmap_cols].copy()

# Optionally scale std so it isn't "tiny" compared to percentages
# but for now we leave raw values; colour scale will adapt.

# -----------------------------------------------------------
# 6. Plot heatmap
# -----------------------------------------------------------

plt.figure(figsize=(10, max(6, len(heatmap_df) * 0.35)))

sns.heatmap(
    heatmap_df,
    annot=True,
    fmt=".1f",
    cmap="viridis",
    cbar_kws={"label": "Value"},
)

plt.title("Benesse模試コース別スコア要約（中央値順）", pad=12)
plt.xlabel("指標")
plt.ylabel("コース名")

plt.tight_layout()
plt.savefig(OUT_PNG, dpi=200)
print(f"💾 Saved heatmap: {OUT_PNG}")
plt.close()

print("\n✅ Done.")
