from dagster import resource, InitResourceContext
from dagster_duckdb import DuckDBResource
from elasticsearch import Elasticsearch as es
from pathlib import Path

project_root = Path(__file__).resolve().parents[1]  # Adjust depending on depth
duckdb_path = project_root / "dagster_windsurf_test/data/staging/data.duckdb"

@resource
def duckdb(context: InitResourceContext) -> DuckDBResource:
    return DuckDBResource(
        database=f"{duckdb_path}",
    )


@resource
def get_es_conn(context: InitResourceContext) -> es:
    """
    Initialize an Elasticsearch connection.

    Args:
        context (InitResourceContext): The context for the resource.
    
    Returns:
        es: The Elasticsearch connection.
    
    Raises:
        ValueError: If the authentication tuple is not provided.
        ConnectionError: If the connection to Elasticsearch fails.
    """

    context.log.info("Initializing Elasticsearch connection")
    auth_creds = context.resource_config.get("auth", None)
    url = context.resource_config.get("url", None)
    if not auth_creds or not isinstance(auth_creds, tuple) or not url:
        raise ValueError("Authentication tuple and URL are required")
    try:
        return es(url, basic_auth=auth_creds, verify_certs=False)
    except ConnectionError as e:
        raise ConnectionError("Failed to connect to Elasticsearch") from e
    except Exception as e:
        raise Exception("Failed to create Elasticsearch connection") from e
   