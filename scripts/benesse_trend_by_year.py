# scripts/benesse_trend_by_year.py

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import re

# Load CSV
df = pd.read_csv("benesse_scores.csv")

# Extract the academic year (年度)
def extract_year(text):
    match = re.search(r'(\d{4})年度', str(text))
    return int(match.group(1)) if match else None

df["year"] = df["course_name"].apply(extract_year)

# Drop anything without a year
df = df.dropna(subset=["year"])

# Compute yearly stats
year_stats = df.groupby("year").agg(
    mean_score = ("quiz", "mean"),
    median_score = ("quiz", "median"),
    std_score = ("quiz", "std"),
    count = ("quiz", "count")
).reset_index()

print("\n=== YEARLY SUMMARY ===")
print(year_stats)

# ---- Plot ----
sns.set(style="whitegrid", font="Hiragino Sans")

plt.figure(figsize=(12, 6))

# Mean Line
sns.lineplot(
    data=year_stats, x="year", y="mean_score",
    marker="o", label="Mean Score", linewidth=2
)

# Median Line
sns.lineplot(
    data=year_stats, x="year", y="median_score",
    marker="o", label="Median Score", linewidth=2, linestyle="--"
)

plt.title("Benesse Scores – Trend by Academic Year", fontsize=14)
plt.xlabel("Academic Year (年度)")
plt.ylabel("Score (%)")

plt.xticks(year_stats["year"])
plt.ylim(0, 100)

plt.legend()
plt.tight_layout()

plt.savefig("benesse_trend_by_year.png", dpi=200)
print("\n💾 Saved: benesse_trend_by_year.png")
plt.show()
