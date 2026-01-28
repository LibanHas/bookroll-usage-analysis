# scripts/benesse_trend_by_subject.py
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import re
from pathlib import Path

from jp_font_setup import setup_japanese_font


# ===========================
# 1. Font (shared JP setup)
# ===========================
setup_japanese_font()  # chooses a Japanese-capable font if available

# ===========================
# 2. Load the data
# ===========================
csv_path = Path("benesse_scores.csv")
print(f"📄 Loading {csv_path.resolve()} ...")
df = pd.read_csv(csv_path)

# scaled is 0.0–1.0 → convert to %
df["score_percent"] = df["scaled"] * 100

# ===========================
# 3. Extract SUBJECT and YEAR
# ===========================
# Examples:
#   2020年度数学[中1]A組  → subject = 数学 , year = 2020
#   2022年度英語[中学]C組 → subject = 英語 , year = 2022

def extract_year(name: str):
    m = re.match(r"(\d{4})年度", str(name))
    return int(m.group(1)) if m else None

def extract_subject(name: str):
    name = str(name)
    if "数学" in name:
        return "数学"
    if "英語" in name:
        return "英語"
    return None  # unexpected / other subjects

df["year"] = df["course_name"].apply(extract_year)
df["subject"] = df["course_name"].apply(extract_subject)

# keep only rows with recognised subject & year
df = df[df["subject"].notna() & df["year"].notna()]

print("\n=== Subject counts ===")
print(df["subject"].value_counts())

print("\n=== Year counts ===")
print(df["year"].value_counts())

# ===========================
# 4. Aggregate: mean & median per subject per year
# ===========================
trend = (
    df.groupby(["year", "subject"])["score_percent"]
      .agg(["mean", "median"])
      .reset_index()
)

print("\n=== Trend Table ===")
print(trend)

# ===========================
# 5. Plot
# ===========================
sns.set_style("whitegrid")

# Re-apply Japanese font AFTER seaborn touches rcParams
setup_japanese_font()
# And just to be extra explicit on this machine:
mpl.rcParams["font.family"] = "Hiragino Sans"
mpl.rcParams["font.sans-serif"] = ["Hiragino Sans"]

plt.figure(figsize=(12, 6))

subjects = sorted(df["subject"].unique())

for subj in subjects:
    subdf = trend[trend["subject"] == subj]

    # 平均（solid line）
    plt.plot(
        subdf["year"],
        subdf["mean"],
        marker="o",
        linewidth=2,
        label=f"{subj}（平均）",
    )

    # 中央値（dashed line）
    plt.plot(
        subdf["year"],
        subdf["median"],
        marker="o",
        linestyle="--",
        linewidth=2,
        label=f"{subj}（中央値）",
    )

plt.title("Benesse模試スコア推移（教科別・平均／中央値）")
plt.xlabel("年度")
plt.ylabel("得点（％）")
plt.xticks(sorted(df["year"].unique()))
plt.ylim(0, 100)
plt.legend()
plt.tight_layout()

out_path = Path("benesse_trend_by_subject.png")
plt.savefig(out_path, dpi=200)
print(f"\n💾 Saved: {out_path.resolve()}")
