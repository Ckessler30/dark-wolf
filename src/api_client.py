import requests
from src.config import API_KEY, BASE_URL
import logging

class SportsDataIOClient:
    def __init__(self, api_key: str = API_KEY):
        self.api_key = api_key
        self.headers = {"Ocp-Apim-Subscription-Key": self.api_key}

    def get(self, endpoint: str) -> dict:
        url = BASE_URL + endpoint
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            logging.error(f"Failed to fetch data from {url}, status code: {response.status_code}")
            response.raise_for_status()
        return response.json()
