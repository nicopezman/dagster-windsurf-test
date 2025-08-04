from typing import Dict, Any
from dagster import resource
import requests

class CountryApiClient:
    def __init__(self, base_url: str = "https://restcountries.com/v3.1"):
        self.base_url = base_url
    
    def get_all_countries(self) -> list[Dict[str, Any]]:
        """Fetch all countries data from the API"""
        response = requests.get(f"{self.base_url}/all")
        response.raise_for_status()
        return response.json()
    
    def get_country(self, country_code: str) -> Dict[str, Any]:
        """Fetch data for a specific country by alpha code"""
        response = requests.get(f"{self.base_url}/alpha/{country_code}")
        response.raise_for_status()
        return response.json()[0]

@resource
def country_api_client(init_context) -> CountryApiClient:
    """Resource that provides a Country API client"""
    return CountryApiClient()
