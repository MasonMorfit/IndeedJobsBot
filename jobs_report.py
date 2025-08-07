#!/usr/bin/env python3
"""
jobs_report.py  ·  Weekly Indeed Job-Postings snapshot (chat-only)

* Downloads aggregate and sector index CSVs from Indeed Hiring-Lab.
* Calculates:
    – National w/w and 4-week changes (percentage-point moves vs Feb-2020 baseline)
    – Breadth (% of sectors expanding w/w)
    – Seasonal averages (Summer 2024, Fall 2024, Winter 2024-25, Spring 2025)
    – Top gainer & decliner sectors with same long-view stats
* Prints a markdown report to stdout.
"""

import datetime as dt
import pandas as pd

# ---------- Config ----------------------------------------------------------

SEASONS = {
    "Summer 2024": (dt.date(2024, 6, 21), dt.date(2024, 9, 20)),
    "Fall 2024":   (dt.date(2024, 9, 21), dt.date(2024, 12, 20)),
    "Winter 2024-25": (dt.date(2024, 12, 21), dt.date(2025, 3, 19)),
    "Spring 2025": (dt.date(2025, 3, 20), dt.date(2025, 6, 20)),
}

AGG_URL    = "https://hiring-lab.github.io/job_postings_tracker/aggregate_job_postings_us.csv"
SECTOR_URL = "https://hiring-lab.github.io/job_postings_tracker/sector_job_postings_us.csv"

# ---------- Data fetch ------------------------------------------------------

def fetch_aggregate() -> pd.DataFrame:
    df = pd.read_csv(AGG_URL, parse_dates=["date"])[["date", "index"]]
    return df.sort_values("date").rename(columns={"index": "value"})

def fetch_sectors() -> pd.DataFrame:
    df = pd.read_csv(SECTOR_URL, parse_dates=["date"])
    return df.sort_values(["sector", "date"])

# ---------- Helpers ---------------------------------------------------------

def seasonal_means(series: pd.Series, dates: pd.Series) -> dict:
    """Return dict of {season name: mean value}."""
    out = {}
    for name, (start, end) in SEASONS.items():
        mask = (dates.dt.date >= start) & (dates.dt.date <= end)
        if mask.any():
            out[name] = series[mask].mean()
    return out

def pct_point_change(new: float, old: float) -> float:
    """Percentage-point change (values are already % above baseline)."""
    return new - old

# ---------- Analysis --------------------------------------------------------

def compute_breadth(sector_df: pd.DataFrame, today: pd.Timestamp) -> tuple[float,int]:
    last_week = today - pd.Timedelta(days=7)
    gains = sector_df[sector_df.date == today].merge(
        sector_df[sector_df.date == last_week],
        on="sector",
        suffixes=("_new", "_old"),
    )
    pct = (gains.index_new > gains.index_old).mean() * 100
    return pct, len(gains)

def identify_leaders_laggards(sector_df: pd.DataFrame, today: pd.Timestamp):
    last_week = today - pd.Timedelta(days=7)
    latest = sector_df[sector_df.date == today]
    prior  = sector_df[sector_df.date == last_week]
    merged = latest.merge(prior, on="sector", suffixes=("_new", "_old"))
    merged["delta_pp"] = merged.index_new - merged.index_old
    leader  = merged.sort_values("delta_pp", ascending=False).iloc[0]
    laggard = merged.sort_values("delta_pp").iloc[0]
    return leader, laggard

def sector_long_view(sector_df: pd.DataFrame, sector_name: str, today: pd.Timestamp):
    df = sector_df[sector_df.sector == sector_name]
    one_month_ago = today - pd.Timedelta(days=28)
    current     = df.loc[df.date == today, "index"].iat[0]
    prev_month  = df.loc[df.date == one_month_ago, "index"].iat[0]
    seasonal    = seasonal_means(df.index, df.date)
    delta_month = pct_point_change(current, prev_month)
    return current, delta_month, seasonal

def build_summary(agg_df: pd.DataFrame, sector_df: pd.DataFrame) -> str:
    today         = agg_df.date.max()
    last_week     = today - pd.Timedelta(days=7)
    one_month_ago = today - pd.Timedelta(days=28)

    # National stats
    latest       = agg_df.loc[agg_df.date == today, "value"].iat[0]
    prior        = agg_df.loc[agg_df.date == last_week, "value"].iat[0]
    prev_month   = agg_df.loc[agg_df.date == one_month_ago, "value"].iat[0]
    delta_pp     = pct_point_change(latest, prior)
    delta_month  = pct_point_change(latest, prev_month)
    breadth_pct, n_sector = compute_breadth(sector_df, today)
    nat_seasonal = seasonal_means(agg_df.value, agg_df.date)

    # Sector leaders / laggards
    leader, laggard = identify_leaders_laggards(sector_df, today)
    lead_curr, lead_mo, lead_seas = sector_long_view(sector_df, leader.sector,  today)
    lag_curr,  lag_mo,  lag_seas  = sector_long_view(sector_df, laggard.sector, today)

    def season_lines(seas: dict) -> str:
        return " | ".join(f"{n.split()[0]} {v:0.0f}" for n, v in seas.items())

    return f"""
**Indeed Job-Postings Index (U.S.) – {today:%b %d %Y}**

* National index: **{latest:0.1f}** (Δ {delta_pp:+0.2f} pp w/w, {delta_month:+0.2f} pp vs 4 wks)
* **Breadth:** {breadth_pct:0.0f}% of {n_sector} sectors expanded w/w
* Seasonal means → {season_lines(nat_seasonal)}

---
**Sector movers** (w/w):

* **Top gainer – {leader.sector}**  
  Level **{lead_curr:0.1f}** (Δ {leader.delta_pp:+0.2f} pp w/w, {lead_mo:+0.2f} pp vs 4 wks)  
  Seasonal → {season_lines(lead_seas)}

* **Top decliner – {laggard.sector}**  
  Level **{lag_curr:0.1f}** (Δ {laggard.delta_pp:+0.2f} pp w/w, {lag_mo:+0.2f} pp vs 4 wks)  
  Seasonal → {season_lines(lag_seas)}

*All changes are percentage-point (pp) moves relative to the Feb 2020 baseline.*
"""

# ---------- Main ------------------------------------------------------------

if __name__ == "__main__":
    agg_df = fetch_aggregate()
    sector_df = fetch_sectors()
    print(build_summary(agg_df, sector_df))
