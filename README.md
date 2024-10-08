# Elasticsearch ILM Policy and Index Usage Report Generator

This script analyzes Elasticsearch cluster diagnostic files to generate a report on Index Lifecycle Management (ILM) policies, index usage, and estimated daily ingestion rates.

## Table of Contents

1. [Overview](#overview)
2. [Methodology](#methodology)
3. [Formulas and Concepts](#formulas-and-concepts)
4. [Usage](#usage)
5. [Output](#output)

## Overview

The script processes Elasticsearch cluster diagnostic files to provide insights into:

- ILM policies in use
- Index usage statistics
- Estimated daily ingestion rates

It uses data from the following files:
- `commercial/ilm_policies.json`
- `commercial/ilm_explain.json`
- `cat/cat_indices.txt`

## Methodology

### 1. Parsing Index Information

The script reads the `cat_indices.txt` file to gather information about each index, including:
- Index name
- Document count
- Store size

### 2. ILM Policy Analysis

Using the `ilm_policies.json` and `ilm_explain.json` files, the script:
- Identifies which indices are managed by ILM policies
- Maps indices to their respective ILM policies

### 3. Size Calculation

The script converts all reported sizes to bytes for consistent calculations. It handles various size units (B, KB, MB, GB, TB) and their abbreviations.

### 4. Daily Ingestion Rate Estimation

To estimate the daily ingestion rate:
1. The script identifies indices with dates in their names using a regular expression.
2. It groups these indices by date and sums their sizes.
3. The average daily ingestion rate is calculated over all days and for the most recent 30 days.

## Formulas and Concepts

### Total Index Usage

```
Total Index Usage = Σ (Size of each index in bytes)
```

### Daily Ingestion Rate

```
Average Daily Ingestion Rate = Total size of date-based indices / Number of unique dates
```

```
Recent 30-Day Average = Total size of indices in the last 30 days / 30
```

### Concepts

1. **Date-based Indices**: The script assumes that indices with dates in their names (format: YYYY.MM.DD) represent data ingested on those dates.

2. **Size Normalization**: All sizes are converted to bytes for calculations and then formatted for human-readable output.

3. **ILM Policy Usage**: The script identifies which indices are managed by ILM policies and groups them accordingly.

4. **Recent vs. Overall Average**: By calculating both an overall average and a recent 30-day average, the script provides insights into both long-term trends and recent ingestion patterns.

### Limitations and Assumptions

- The script assumes that index names with dates accurately reflect the ingestion date of the data.
- It doesn't account for data that might have been deleted or compressed over time.
- The estimation doesn't consider potential variations in ingestion rates (e.g., weekdays vs. weekends).

## Usage

1. Ensure you have Python installed on your system.
2. Place the script in a directory with the Elasticsearch cluster diagnostic files.
3. Run the script:
   ```
   python elastic_ilm_report.py
   ```
4. When prompted, enter the path to the directory containing the diagnostic files.

## Output

The script generates a report that includes:

1. Estimated Daily Ingestion Rate
   - Average over all days
   - Average over the last 30 days
   - Total data ingested
   - Number of days analyzed

2. ILM Policies in Use
   - Policy names
   - Number of indices using each policy
   - List of indices with their sizes and document counts

3. Total Index Usage
   - Sum of all index sizes

4. Policy Details
   - Phases defined for each policy

The report is displayed in the console and saved to a file named `ilm_report.txt`.

---

This README provides a comprehensive overview of the script's functionality, methodology, and the concepts behind the calculations. It should help users understand how the estimations are made and interpret the results accurately.
