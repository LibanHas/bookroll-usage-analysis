#!/usr/bin/env python
"""
Example script demonstrating how to use the keyword ranking functionality.

Usage:
    python keyword_ranking_example.py [--context_id CONTEXT_ID] [--limit LIMIT] [--top_n TOP_N]

For example, to get top 20 keywords from course ID 705:
    python keyword_ranking_example.py --context_id 705 --top_n 20
"""

import os
import sys
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Django setup to use models
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'leaf_school.settings')
django.setup()

# Import the TopKeywords model
from core.models import TopKeywords


def main():
    """Main function to demonstrate keyword ranking."""
    parser = argparse.ArgumentParser(description='Get top keywords from student highlights')
    parser.add_argument('--context_id', type=str, help='Course context ID to filter by')
    parser.add_argument('--limit', type=int, default=1000, help='Limit number of highlights to process')
    parser.add_argument('--top_n', type=int, default=20, help='Number of top keywords to return')
    args = parser.parse_args()

    print(f"Getting top {args.top_n} keywords for context_id={args.context_id} (limit={args.limit})...")

    # Get top keywords
    keywords = TopKeywords.get_top_keywords(
        context_id=args.context_id,
        limit=args.limit,
        top_n=args.top_n
    )

    if not keywords:
        print("No keywords found. Make sure there are student highlights available.")
        return

    # Convert to DataFrame for easier handling
    df = pd.DataFrame(keywords)

    # Display results
    print(f"\nTop {len(df)} keywords from student highlights:")
    print(df)

    # Plot top 10 keywords
    plot_keywords(df.head(min(10, len(df))))

    # Save results to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    context_str = f"_context{args.context_id}" if args.context_id else ""
    filename = f"top_keywords{context_str}_{timestamp}.csv"
    df.to_csv(filename, index=False)
    print(f"\nResults saved to {filename}")


def plot_keywords(df):
    """Plot top keywords with their frequencies."""
    try:
        plt.figure(figsize=(10, 6))
        plt.barh(df['keyword'][::-1], df['frequency'][::-1])
        plt.xlabel('Frequency')
        plt.ylabel('Keyword')
        plt.title('Top Keywords from Student Highlights')
        plt.tight_layout()

        # Save the plot
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"top_keywords_plot_{timestamp}.png"
        plt.savefig(filename)
        print(f"Plot saved to {filename}")

        # Show the plot if running in interactive mode
        plt.show()
    except Exception as e:
        print(f"Error creating plot: {e}")


if __name__ == "__main__":
    main()