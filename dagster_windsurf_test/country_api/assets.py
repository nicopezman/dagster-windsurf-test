from typing import Dict, Any, List
from dagster import asset, AssetIn, AssetExecutionContext
import pandas as pd
from pathlib import Path
import os

@asset(required_resource_keys={"country_api"})
def raw_countries(context: AssetExecutionContext) -> List[Dict[str, Any]]:
    """Fetch raw country data from the REST Countries API"""
    context.log.info("Fetching country data from REST Countries API")
    countries = context.resources.country_api.get_all_countries()
    context.log.info(f"Fetched data for {len(countries)} countries")
    return countries

@asset(
    ins={"raw_countries": AssetIn("raw_countries")},
    required_resource_keys={"country_api"}
)
def processed_countries(context: AssetExecutionContext, raw_countries: List[Dict[str, Any]]) -> pd.DataFrame:
    """Process raw country data into a structured DataFrame, keeping only top 30 largest countries by area"""
    context.log.info("Processing country data")
    
    # Extract relevant fields
    processed = []
    for country in raw_countries:
        processed.append({
            "name": country.get("name", {}).get("common", ""),
            "official_name": country.get("name", {}).get("official", ""),
            "cca2": country.get("cca2", ""),
            "cca3": country.get("cca3", ""),
            "region": country.get("region", ""),
            "subregion": country.get("subregion", ""),
            "population": country.get("population", 0),
            "area": country.get("area", 0.0),  # Area in square kilometers
            "languages": ", ".join(country.get("languages", {}).values()),
            "capital": ", ".join(country.get("capital", [])),
            "borders": ", ".join(country.get("borders", [])),
            "timezones": ", ".join(country.get("timezones", [])),
            "currencies": ", ".join(country.get("currencies", {}).keys()),
            "independent": country.get("independent", False),
            "un_member": country.get("unMember", False),
            "landlocked": country.get("landlocked", False),
            "flag": country.get("flag", ""),
            "flag_emoji": country.get("flag", "")
        })
    
    # Convert to DataFrame
    df = pd.DataFrame(processed)
    
    # Sort by area in descending order and keep top 30
    df = df.sort_values('area', ascending=False).head(30)
    
    # Reset index after sorting
    df = df.reset_index(drop=True)
    
    # Add rank column
    df['area_rank'] = df.index + 1
    
    context.log.info(f"Processed data for top {len(df)} largest countries by area")
    return df

@asset(
    ins={"countries": AssetIn("processed_countries")}
)
def country_stats(context: AssetExecutionContext, countries: pd.DataFrame) -> Dict[str, Any]:
    """Generate statistics from country data"""
    context.log.info("Generating country statistics")
    
    stats = {
        "total_countries": len(countries),
        "total_population": int(countries["population"].sum()),
        "average_area": float(countries["area"].mean()),
        "regions": countries["region"].nunique(),
        "most_populous_country": countries.loc[countries["population"].idxmax()]["name"],
        "largest_country": countries.loc[countries["area"].idxmax()]["name"],
        "regions_distribution": countries["region"].value_counts().to_dict(),
        "languages_count": len(set(", ".join(countries["languages"]).split(", ")))
    }
    
    context.log.info(f"Generated statistics: {stats}")
    return stats
