import pandas as pd

print("📄 Loading benesse_scores.csv...")
df = pd.read_csv("benesse_scores.csv")

print("\n=== BASIC SHAPE ===")
print(df.shape)

print("\n=== COLUMNS ===")
print(list(df.columns))

print("\n=== NON-NULL COUNTS ===")
print(df.notna().sum())

print("\n=== UNIQUE COURSES ===")
print("course_name:", df["course_name"].nunique())
print("course_id:", df["course_id"].nunique())

print("\n=== SCORE SUMMARY (scaled and quiz) ===")
print(df[["quiz", "scaled"]].describe())

print("\n=== CHECK: scaled = quiz/100 ===")
df["scaled_expected"] = df["quiz"] / df["max"]
df["diff"] = (df["scaled"] - df["scaled_expected"]).abs()

print(df["diff"].describe())

bad = df[df["diff"] > 0.0001]
print("\nRows where scaled != expected:", len(bad))

print("\n=== DATE RANGE ===")
print(df["date_at"].min(), "→", df["date_at"].max())
