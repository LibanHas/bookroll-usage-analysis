# scripts/benesse_course_difficulty.py

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import matplotlib as mpl

# ---- Set a Japanese-capable font ----
# Try these in order; comment/uncomment depending on what works on your Mac.
mpl.rcParams["font.family"] = "Hiragino Sans"        # good default on macOS
# mpl.rcParams["font.family"] = "Hiragino Kaku Gothic ProN"
# mpl.rcParams["font.family"] = "YuGothic"
mpl.rcParams["axes.unicode_minus"] = False

# --- File path ---
CSV_FILE = "benesse_scores.csv"

# --- Output folder ---
OUT_DIR = "benesse_outputs"
os.makedirs(OUT_DIR, exist_ok=True)

print(f"📄 Loading {CSV_FILE} ...")
df = pd.read_csv(CSV_FILE)

# Ensure consistency
df["score_percent"] = df["scaled"] * 100

print("✔️ Loaded. Now generating visualizations...")

# -------------------------------------
# 1) BOX PLOTS PER COURSE
# -------------------------------------
plt.figure(figsize=(18, 10))
sns.boxplot(
    data=df,
    x="course_name",
    y="score_percent",
    palette="Blues"
)
plt.xticks(rotation=90)
plt.xlabel("Course Name")
plt.ylabel("Score (%)")
plt.title("Benesse Scores – Distribution by Course")
plt.tight_layout()

output_path = f"{OUT_DIR}/benesse_boxplots_by_course.png"
plt.savefig(output_path, dpi=300)
plt.close()
print("💾 Saved boxplots:", output_path)


# -------------------------------------
# 2) MEAN SCORE BAR CHART (RANKED)
# -------------------------------------
course_means = (
    df.groupby("course_name")["score_percent"]
    .mean()
    .sort_values(ascending=False)
)

plt.figure(figsize=(12, 10))
sns.barplot(
    x=course_means.values,
    y=course_means.index,
    palette="Blues_r"
)
plt.xlabel("Mean Score (%)")
plt.ylabel("Course Name")
plt.title("Benesse Scores – Course Averages (Ranked Highest to Lowest)")
plt.tight_layout()

output_path = f"{OUT_DIR}/benesse_course_means_ranked.png"
plt.savefig(output_path, dpi=300)
plt.close()
print("💾 Saved ranked bar chart:", output_path)


# -------------------------------------
# 3) SUMMARY TABLE -> CSV
# -------------------------------------
summary = df.groupby("course_name")["score_percent"].agg(
    ["count", "mean", "median", "std", "min", "max"]
)

summary_path = f"{OUT_DIR}/benesse_course_summary.csv"
summary.to_csv(summary_path)
print("💾 Saved summary CSV:", summary_path)


print("\n✅ All Benesse course difficulty visualizations completed.\n")
