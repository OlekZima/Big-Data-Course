"""
UK Property Sales Insights — Data Product Consumption Analysis
Product by: Dominika Lutek (readmebooks/projekt-nieruchomosci)
Consumer:   BGD project

This script reproduces the gold-layer aggregation from the data product
using a sample of the UK Land Registry pp-complete.csv dataset,
then produces charts and a visual summary as required by task.md.

Data source: http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv
Schema (from data_product_contract.yaml):
  id, price, sale_date, city, county, sale_year
"""

import io
import urllib.request
import duckdb
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.ticker as mticker
import seaborn as sns
import numpy as np
from pathlib import Path

# ── output dir ──────────────────────────────────────────────────────────────
OUT = Path(__file__).parent / "charts"
OUT.mkdir(exist_ok=True)

# ── palette ─────────────────────────────────────────────────────────────────
PALETTE = sns.color_palette("Blues_r", 20)
sns.set_theme(style="whitegrid", font_scale=1.1)

# ── 1. Fetch data from UK Land Registry ─────────────────────────────────────
# The file is chronological (oldest first). To get a multi-decade sample we
# download the first 200k rows (1995-early) AND the last ~5 MB (recent years)
# by using HTTP Range requests, then combine them.
URL = (
    "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"
    "/pp-complete.csv"
)
CHUNK = 65536

print("Fetching HEAD to get file size …")
req = urllib.request.Request(URL, method="HEAD")
with urllib.request.urlopen(req, timeout=30) as r:
    total_size = int(r.headers.get("Content-Length", 0))
print(f"File size: {total_size/1e9:.2f} GB")

def fetch_range(url: str, start: int, end: int) -> list[bytes]:
    """Fetch a byte range and return complete lines."""
    req = urllib.request.Request(url, headers={"Range": f"bytes={start}-{end}"})
    with urllib.request.urlopen(req, timeout=60) as r:
        data = r.read()
    lines = data.split(b"\n")
    # drop first line (may be partial) unless start==0
    return lines[1:] if start > 0 else lines

print("Fetching early slice (1995-2000) …")
early = fetch_range(URL, 0, 30 * 1024 * 1024)          # first 30 MB
print(f"  {len(early):,} lines")

print("Fetching mid slice (≈2007-2010) …")
mid_start = total_size // 3
mid = fetch_range(URL, mid_start, mid_start + 20 * 1024 * 1024)
print(f"  {len(mid):,} lines")

print("Fetching recent slice (2018-2025) …")
late_start = max(0, total_size - 30 * 1024 * 1024)
late = fetch_range(URL, late_start, total_size - 1)
print(f"  {len(late):,} lines")

all_lines = early + mid + late
raw_csv = b"\n".join(l for l in all_lines if l.strip())
print(f"Combined sample: {len(all_lines):,} rows  ({len(raw_csv)/1e6:.1f} MB)")

# ── 2. Load into DuckDB and apply the silver-layer transformation ────────────
con = duckdb.connect(":memory:")

# Write sample to a temp file so DuckDB can read it
import tempfile, os
tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
tmp.write(raw_csv)
tmp.close()

con.execute(f"CREATE TABLE raw AS SELECT * FROM read_csv_auto('{tmp.name}', all_varchar=True, ignore_errors=true)")
os.unlink(tmp.name)

# Silver: cast types, rename columns, deduplicate (mirrors 02_elt_proces.py)
con.execute("""
    CREATE TABLE silver AS
    SELECT DISTINCT
        column00  AS id,
        TRY_CAST(column01 AS INTEGER)                        AS price,
        TRY_CAST(substring(column02, 1, 10) AS DATE)         AS sale_date,
        column11                                             AS city,
        column13                                             AS county,
        column04                                             AS property_type,
        YEAR(TRY_CAST(substring(column02, 1, 10) AS DATE))  AS sale_year
    FROM raw
    WHERE TRY_CAST(column01 AS INTEGER) > 0
""")

row_count = con.execute("SELECT COUNT(*) FROM silver").fetchone()  # type: ignore[index]
row_count = row_count[0] if row_count else 0
print(f"Silver layer: {row_count:,} clean rows")

# Gold: avg price + transaction count by city (min 200 sales in sample)
con.execute("""
    CREATE TABLE gold_city AS
    SELECT
        city,
        COUNT(*)                    AS total_sales,
        ROUND(AVG(price), 0)        AS avg_price,
        ROUND(MEDIAN(price), 0)     AS median_price,
        ROUND(MIN(price), 0)        AS min_price,
        ROUND(MAX(price), 0)        AS max_price
    FROM silver
    WHERE city IS NOT NULL AND city <> ''
    GROUP BY city
    HAVING total_sales >= 200
    ORDER BY avg_price DESC
""")

# Gold: avg price by year
con.execute("""
    CREATE TABLE gold_year AS
    SELECT
        sale_year,
        COUNT(*)                    AS total_sales,
        ROUND(AVG(price), 0)        AS avg_price,
        ROUND(MEDIAN(price), 0)     AS median_price
    FROM silver
    WHERE sale_year BETWEEN 1995 AND 2025
    GROUP BY sale_year
    ORDER BY sale_year
""")

# Gold: transactions by property type
con.execute("""
    CREATE TABLE gold_type AS
    SELECT
        property_type,
        COUNT(*)                    AS total_sales,
        ROUND(AVG(price), 0)        AS avg_price
    FROM silver
    WHERE property_type IS NOT NULL AND property_type <> ''
    GROUP BY property_type
    ORDER BY total_sales DESC
""")

df_city  = con.execute("SELECT * FROM gold_city").df()
df_year  = con.execute("SELECT * FROM gold_year").df()
df_type  = con.execute("SELECT * FROM gold_type").df()

print(f"\nGold city rows : {len(df_city)}")
print(f"Gold year rows : {len(df_year)}")
print(f"Gold type rows : {len(df_type)}")
print("\nTop 10 cities by avg price:")
print(df_city.head(10).to_string(index=False))

# ── 3. Charts ────────────────────────────────────────────────────────────────

# --- Chart 1: Top 20 cities by average price (horizontal bar) ---------------
top20 = df_city.head(20).sort_values("avg_price")
fig, ax = plt.subplots(figsize=(10, 8))
bars = ax.barh(top20["city"], top20["avg_price"] / 1_000,
               color=sns.color_palette("Blues_r", len(top20)))
ax.set_xlabel("Average Sale Price (£ thousands)", labelpad=10)
ax.set_title("Top 20 UK Cities by Average Property Price", fontsize=14, fontweight="bold")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"£{x:,.0f}k"))
for bar, val in zip(bars, top20["avg_price"]):
    ax.text(bar.get_width() + 5, bar.get_y() + bar.get_height() / 2,
            f"£{val/1000:,.0f}k", va="center", fontsize=8.5)
ax.set_xlim(0, top20["avg_price"].max() / 1_000 * 1.18)
plt.tight_layout()
fig.savefig(OUT / "01_top20_cities_avg_price.png", dpi=150)
plt.close()
print("Saved chart 1")

# --- Chart 2: Transaction volume by year (line + area) ----------------------
fig, ax1 = plt.subplots(figsize=(12, 5))
color_line = "#1565C0"
color_area = "#90CAF9"
ax1.fill_between(df_year["sale_year"], df_year["total_sales"] / 1_000,
                 alpha=0.3, color=color_area)
ax1.plot(df_year["sale_year"], df_year["total_sales"] / 1_000,
         color=color_line, linewidth=2.5, marker="o", markersize=4)
ax1.set_xlabel("Year")
ax1.set_ylabel("Transactions (thousands)", color=color_line)
ax1.tick_params(axis="y", labelcolor=color_line)

ax2 = ax1.twinx()
ax2.plot(df_year["sale_year"], df_year["avg_price"] / 1_000,
         color="#E53935", linewidth=2, linestyle="--", marker="s", markersize=4)
ax2.set_ylabel("Avg Price (£ thousands)", color="#E53935")
ax2.tick_params(axis="y", labelcolor="#E53935")
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"£{x:,.0f}k"))

ax1.set_title("UK Property Market: Transaction Volume & Average Price by Year",
              fontsize=13, fontweight="bold")
ax1.legend(["Transaction Volume"], loc="upper left")
ax2.legend(["Average Price"], loc="upper right")
plt.tight_layout()
fig.savefig(OUT / "02_volume_and_price_by_year.png", dpi=150)
plt.close()
print("Saved chart 2")

# --- Chart 3: Property type breakdown (pie + bar side by side) --------------
type_labels = {
    "D": "Detached", "S": "Semi-detached",
    "T": "Terraced", "F": "Flat/Maisonette", "O": "Other"
}
df_type["label"] = df_type["property_type"].map(type_labels).fillna(df_type["property_type"])

fig, (ax_pie, ax_bar) = plt.subplots(1, 2, figsize=(13, 6))

wedge_colors = sns.color_palette("Blues", len(df_type))
wedges, texts, autotexts = ax_pie.pie(
    df_type["total_sales"], labels=df_type["label"],
    autopct="%1.1f%%", colors=wedge_colors,
    startangle=140, pctdistance=0.82,
    wedgeprops=dict(edgecolor="white", linewidth=1.5)
)
for at in autotexts:
    at.set_fontsize(9)
ax_pie.set_title("Transactions by Property Type", fontweight="bold")

bars2 = ax_bar.bar(df_type["label"], df_type["avg_price"] / 1_000,
                   color=wedge_colors, edgecolor="white", linewidth=1.2)
ax_bar.set_ylabel("Average Price (£ thousands)")
ax_bar.set_title("Average Price by Property Type", fontweight="bold")
ax_bar.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"£{x:,.0f}k"))
for bar, val in zip(bars2, df_type["avg_price"]):
    ax_bar.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 2,
                f"£{val/1000:,.0f}k", ha="center", va="bottom", fontsize=9)
ax_bar.set_ylim(0, df_type["avg_price"].max() / 1_000 * 1.2)
plt.tight_layout()
fig.savefig(OUT / "03_property_type_breakdown.png", dpi=150)
plt.close()
print("Saved chart 3")

# --- Chart 4: Price distribution (box plot top 10 cities) -------------------
top10_cities = df_city.head(10)["city"].tolist()
df_silver = con.execute("""
    SELECT city, price FROM silver
    WHERE city IN (SELECT city FROM gold_city ORDER BY avg_price DESC LIMIT 10)
      AND price BETWEEN 10000 AND 5000000
""").df()

fig, ax = plt.subplots(figsize=(13, 6))
order: list[str] = (
    df_silver.groupby("city")["price"]
    .median()
    .pipe(lambda s: s.iloc[(-s.values).argsort()].index.tolist())  # type: ignore[union-attr]
)
sns.boxplot(data=df_silver, x="city", y="price", order=order,
            palette="Blues_r", flierprops=dict(marker=".", markersize=2, alpha=0.3),
            ax=ax)
ax.set_xlabel("City")
ax.set_ylabel("Sale Price (£)")
ax.set_title("Price Distribution — Top 10 Most Expensive Cities", fontsize=13, fontweight="bold")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"£{x/1e3:,.0f}k"))
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
fig.savefig(OUT / "04_price_distribution_top10.png", dpi=150)
plt.close()
print("Saved chart 4")

# --- Chart 5: Summary dashboard (2×2 grid) ----------------------------------
fig = plt.figure(figsize=(16, 12))
fig.suptitle("UK Property Sales Insights — Data Product Summary Dashboard",
             fontsize=16, fontweight="bold", y=0.98)
gs = gridspec.GridSpec(2, 2, figure=fig, hspace=0.45, wspace=0.35)

# 5a — top 10 cities bar
ax_a = fig.add_subplot(gs[0, 0])
t10 = df_city.head(10).sort_values("avg_price")
ax_a.barh(t10["city"], t10["avg_price"] / 1_000,
          color=sns.color_palette("Blues_r", 10))
ax_a.set_title("Top 10 Cities — Avg Price", fontweight="bold")
ax_a.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"£{x:,.0f}k"))
ax_a.set_xlabel("£ thousands")

# 5b — avg price trend
ax_b = fig.add_subplot(gs[0, 1])
ax_b.plot(df_year["sale_year"], df_year["avg_price"] / 1_000,
          color="#1565C0", linewidth=2.5, marker="o", markersize=4)
ax_b.fill_between(df_year["sale_year"], df_year["avg_price"] / 1_000,
                  alpha=0.15, color="#1565C0")
ax_b.set_title("Average Price Trend by Year", fontweight="bold")
ax_b.set_xlabel("Year")
ax_b.set_ylabel("£ thousands")
ax_b.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"£{x:,.0f}k"))

# 5c — transaction volume trend
ax_c = fig.add_subplot(gs[1, 0])
ax_c.bar(df_year["sale_year"], df_year["total_sales"] / 1_000,
         color="#42A5F5", edgecolor="white", linewidth=0.5)
ax_c.set_title("Transaction Volume by Year", fontweight="bold")
ax_c.set_xlabel("Year")
ax_c.set_ylabel("Transactions (thousands)")

# 5d — property type avg price
ax_d = fig.add_subplot(gs[1, 1])
ax_d.bar(df_type["label"], df_type["avg_price"] / 1_000,
         color=sns.color_palette("Blues", len(df_type)), edgecolor="white")
ax_d.set_title("Avg Price by Property Type", fontweight="bold")
ax_d.set_ylabel("£ thousands")
ax_d.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"£{x:,.0f}k"))
plt.xticks(rotation=20, ha="right")

fig.savefig(OUT / "05_dashboard.png", dpi=150, bbox_inches="tight")
plt.close()
print("Saved chart 5 (dashboard)")

# ── 4. Summary stats table ───────────────────────────────────────────────────
print("\n" + "="*60)
print("SUMMARY STATISTICS")
print("="*60)
_r = con.execute("SELECT COUNT(*) FROM silver").fetchone(); total_txn = _r[0] if _r else 0  # type: ignore[index]
_r = con.execute("SELECT ROUND(AVG(price),0) FROM silver").fetchone(); avg_price = _r[0] if _r else 0  # type: ignore[index]
_r = con.execute("SELECT ROUND(MEDIAN(price),0) FROM silver").fetchone(); med_price = _r[0] if _r else 0  # type: ignore[index]
max_city  = df_city.iloc[0]
min_city  = df_city.iloc[-1]
years     = f"{int(df_year['sale_year'].min())} – {int(df_year['sale_year'].max())}"

print(f"  Sample rows (silver)  : {total_txn:>12,}")
print(f"  Average price         : £{avg_price:>11,.0f}")
print(f"  Median price          : £{med_price:>11,.0f}")
print(f"  Most expensive city   : {max_city['city']} (£{max_city['avg_price']:,.0f})")
print(f"  Least expensive city  : {min_city['city']} (£{min_city['avg_price']:,.0f})")
print(f"  Year range            : {years}")
print(f"  Cities analysed       : {len(df_city)}")
print("="*60)
print(f"\nAll charts saved to: {OUT.resolve()}")
