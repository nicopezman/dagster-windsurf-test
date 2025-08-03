from dagster import asset, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from elasticsearch import Elasticsearch as es
from dagster_windsurf_test.resources import get_es_conn, duckdb
import json
from pathlib import Path

@asset(config_schema={"index_name": str}, required_resource_keys={"get_es_conn"}) # Add config schema
def es_index(context: AssetExecutionContext) -> dict:
    """
    Pull an index from Elasticsearch.
    
    Args:
        context (AssetExecutionContext): The context for the asset.
        auth (tuple): The authentication tuple for the Elasticsearch client.
        url (str): The URL of the Elasticsearch instance.
        index_name (str): The name of the index to pull.
    
    returns:
        dict: The index data.

    """
    
    es_conn = context.resources.get_es_conn
    index_name = context.asset_config["index_name"]
    context.log.info(f"Searching index {index_name} from Elasticsearch")
    return es_conn.search(index=index_name, query={"match_all": {}}, size=10)

@asset
def dict_to_file(context: AssetExecutionContext, es_index: dict):
    """
    Convert a dictionary to a file.
    
    Args:
        context (AssetExecutionContext): The context for the asset.
        es_index (dict): The index data to convert.
    
    Returns:
        None
    """
    
    context.log.info("Converting dictionary to file")
    file_path = Path(__file__).parent / "es_index_nyc_taxi.json"
    with open(file_path, "w") as f:
        json.dump(es_index, f)
    
    return str(file_path)


@asset
def dict_to_duckdb_table(context: AssetExecutionContext, dict_to_file: str, database: DuckDBResource):
    """
    Convert a dictionary to a DuckDB Table.
    
    Args:
        context (AssetExecutionContext): The context for the asset.
        dict_to_file (str): The path to the file containing the dictionary.
    
    Returns:
        duckdb.DataFrame: The converted DataFrame.
    """
    
    table_name = "nyc_taxi"
    context.log.info(f"Converting file to DuckDB table {table_name}")
    database = context.resources.database

    with database.get_connection() as conn:
        table_query = f"""
            create table if not exists {table_name} 
            as
            SELECT * FROM read_csv('{dict_to_file}');
        """
        # run create table statement
        conn.execute(table_query)

    
    