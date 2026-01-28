import mysql.connector
import pandas as pd
from datetime import datetime

# -----------------------------
# DB connection settings
# (fill these with your real values)
# -----------------------------
DB_CONFIG = {
    "host": "10.236.173.145",
    "port": 33308,  # or your custom port
    "user": "readonly_user",
    "password": "P3FXDdNAehkLiAWNEbTWDLrRngBZYWScCWD8ZDeXLJ",
    "database": "analysis_development",
}

print("🔌 Connecting to MySQL...")

conn = mysql.connector.connect(**DB_CONFIG)

# -----------------------------
# Load the full course_student_scores table
# -----------------------------
query = """
SELECT
  id,
  course_student_id,
  quiz,
  response,
  user_id,
  created_at,
  updated_at,
  sort,
  name,
  student_id,
  moodle_url,
  course_id,
  min,
  max,
  scaled,
  course_name,
  date_at,
  consumer_key
FROM course_student_scores;
"""

print("📥 Running query: SELECT * FROM course_student_scores ...")
df = pd.read_sql(query, conn)
conn.close()
print("🔌 Connection closed.\n")

print("=== BASIC SHAPE ===")
print("Rows:", len(df))
print("Columns:", len(df.columns))
print("Columns:", list(df.columns))

# -------------------------------
# 1) min > max  OR min = max with scaled outside [0,1]
# -------------------------------
print("\n=== 1) min/max anomalies ===")

min_gt_max = df[df["min"] > df["max"]]
min_eq_max_scaled_bad = df[(df["min"] == df["max"]) & (~df["scaled"].between(0, 1))]

print("min > max:", len(min_gt_max))
print("min == max but scaled outside [0,1]:", len(min_eq_max_scaled_bad))

# -------------------------------
# 2) inconsistent scores for the same student_id + course_id
#    (very large spread in quiz)
# -------------------------------
print("\n=== 2) inconsistent scores per student+course ===")

group = df.groupby(["student_id", "course_id"])["quiz"]
spread = group.max() - group.min()
inconsistent = spread[spread > 50]   # heuristic threshold

print("Student+Course with >50 point spread:", len(inconsistent))
print("\nTop inconsistent student+course combinations (spread):")
print(inconsistent.sort_values(ascending=False).head(20))

# -------------------------------
# 3) impossible or strange dates in date_at
# -------------------------------
print("\n=== 3) date anomalies ===")

df["date_at_parsed"] = pd.to_datetime(df["date_at"], errors="coerce")

future_dates = df[df["date_at_parsed"] > datetime.now()]
very_old_dates = df[df["date_at_parsed"] < datetime(1990, 1, 1)]

print("Future dates:", len(future_dates))
print("Dates before 1990:", len(very_old_dates))

# -------------------------------
# 4) courses with almost all zeros
# -------------------------------
print("\n=== 4) courses dominated by zeros (scaled == 0) ===")

course_zero_ratio = df.groupby("course_id")["scaled"].apply(lambda x: (x == 0).mean())
zero_heavy = course_zero_ratio[course_zero_ratio > 0.90].sort_values(ascending=False)

print("Courses with >90% zeros:", len(zero_heavy))
print(zero_heavy.head(20))

# -------------------------------
# 5) scaled == 0 but quiz unusually high (> 50)
# -------------------------------
print("\n=== 5) scaled==0 but quiz unusually high (quiz > 50) ===")

scaled_zero_high_quiz = df[(df["scaled"] == 0) & (df["quiz"] > 50)]
print("scaled = 0 and quiz > 50:", len(scaled_zero_high_quiz))

# -------------------------------
# 6) scaled values outside [0, 1]
# -------------------------------
print("\n=== 6) scaled outside [0,1] ===")

scaled_bad = df[(df["scaled"] < 0) | (df["scaled"] > 1)]
print("Rows with invalid scaled:", len(scaled_bad))

print("\n=== Anomaly scan complete ===")
