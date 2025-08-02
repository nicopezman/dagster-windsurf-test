import pandas as pd
import requests
from dagster import asset, AssetExecutionContext


@asset
def raw_data(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Fetch raw data from a public API as an example.
    This asset demonstrates how to fetch external data.
    """
    context.log.info("Fetching raw data from JSONPlaceholder API")
    
    # Fetch sample data from a public API
    response = requests.get("https://jsonplaceholder.typicode.com/posts")
    data = response.json()
    
    df = pd.DataFrame(data)
    context.log.info(f"Fetched {len(df)} records")
    
    return df


@asset
def processed_data(context: AssetExecutionContext, raw_data: pd.DataFrame) -> pd.DataFrame:
    """
    Process the raw data by adding some computed columns.
    This asset depends on the raw_data asset.
    """
    context.log.info("Processing raw data")
    
    # Add some computed columns
    processed_df = raw_data.copy()
    processed_df['title_length'] = processed_df['title'].str.len()
    processed_df['body_length'] = processed_df['body'].str.len()
    processed_df['word_count'] = processed_df['body'].str.split().str.len()
    
    context.log.info(f"Processed {len(processed_df)} records with new computed columns")
    
    return processed_df


@asset
def data_summary(context: AssetExecutionContext, processed_data: pd.DataFrame) -> dict:
    """
    Create a summary of the processed data.
    This asset provides insights into the dataset.
    """
    context.log.info("Creating data summary")
    
    summary = {
        'total_records': len(processed_data),
        'avg_title_length': processed_data['title_length'].mean(),
        'avg_body_length': processed_data['body_length'].mean(),
        'avg_word_count': processed_data['word_count'].mean(),
        'unique_users': processed_data['userId'].nunique(),
    }
    
    context.log.info(f"Summary created: {summary}")
    
    return summary