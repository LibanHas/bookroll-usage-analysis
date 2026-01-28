# scripts/benesse_subject_gap.py

import re
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from jp_font_setup import setup_japanese_font


def extract_year(name: str):
    """Extract 4-digit year from course_name like '2020年度数学[中1]A組'."""
    m = re.match(r"(\d{4})年度", str(name))
    return int(m.group(1)) if m else None


def extract_subject(name: str):
    """Return '数学' or '英語' if present, otherwise None."""
    if "数学" in name:
        return "数学"
    if "英語" in name:
        return "英語"
    return None


def main():
    # 1) Font – get the FontProperties object
    font_prop = setup_japanese_font()

    # 2) Load
    csv_path = Path("benesse_scores.csv")
    print(f"📄 Loading {csv_path.resolve()} ...")
    df = pd.read_csv(csv_path)

    df["score_percent"] = df["scaled"] * 100

    # 3) Year & subject
    df["year"] = df["course_name"].apply(extract_year)
    df["subject"] = df["course_name"].apply(extract_subject)

    df = df[df["subject"].notna()]
    df = df[df["year"].notna()]

    print("\n=== Subject counts ===")
    print(df["subject"].value_counts())

    print("\n=== Year counts ===")
    print(df["year"].value_counts())

    # 4) Aggregate: mean per subject per year
    agg = (
        df.groupby(["year", "subject"])["score_percent"]
          .mean()
          .reset_index()
    )

    print("\n=== Mean score per subject per year ===")
    print(agg)

    # 5) Pivot to year index, subjects as columns
    pivot = agg.pivot(index="year", columns="subject", values="score_percent")

    # Ensure both subjects exist
    missing_cols = [col for col in ["数学", "英語"] if col not in pivot.columns]
    if missing_cols:
        print("\n⚠️ Missing subjects in data, cannot compute gap:", missing_cols)
        return

    pivot["英語_−_数学"] = pivot["英語"] - pivot["数学"]

    print("\n=== 英語 − 数学 の得点差（平均, ポイント） ===")
    print(pivot[["英語", "数学", "英語_−_数学"]])

    # 6) Plot
    sns.set_style("whitegrid")
    fig, ax = plt.subplots(figsize=(10, 5))

    years = pivot.index.to_list()
    gaps = pivot["英語_−_数学"].to_list()

    ax.axhline(0, color="gray", linewidth=1, linestyle="--")
    ax.plot(years, gaps, marker="o", linewidth=2)

    # Label each point
    for x, y in zip(years, gaps):
        ax.text(
            x, y + 0.4, f"{y:.1f}",
            ha="center", va="bottom", fontsize=9,
            fontproperties=font_prop,
        )

    # Apply JP font to all JP text explicitly
    ax.set_title(
        "Benesse模試スコア差：英語−数学（平均）",
        fontproperties=font_prop,
    )
    ax.set_xlabel("年度", fontproperties=font_prop)
    ax.set_ylabel("英語 − 数学 の得点差（ポイント）", fontproperties=font_prop)

    ax.set_xticks(years)
    for label in ax.get_xticklabels():
        label.set_fontproperties(font_prop)
    for label in ax.get_yticklabels():
        label.set_fontproperties(font_prop)

    fig.tight_layout()

    out_path = Path("benesse_subject_gap.png")
    fig.savefig(out_path, dpi=200)
    print(f"\n💾 Saved: {out_path.resolve()}")


if __name__ == "__main__":
    main()
