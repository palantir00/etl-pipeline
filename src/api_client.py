"""
OpenSky Network API client.
Oddziela logikę HTTP od logiki ETL w notebookach.
"""

import requests
from typing import Optional


OPENSKY_BASE_URL = "https://opensky-network.org/api"


def get_states_all(
    timeout: int = 30,
    bbox: Optional[tuple] = None,
) -> dict:
    """
    Pobiera live state vectors z endpointu /api/states/all.

    Args:
        timeout: Timeout HTTP w sekundach.
        bbox:    Opcjonalny bounding box (min_lat, max_lat, min_lon, max_lon)
                 do filtrowania samolotów po obszarze geograficznym.

    Returns:
        Dict z kluczami 'time' (int) i 'states' (list of lists).

    Raises:
        requests.HTTPError:  API zwróciło status 4xx/5xx.
        requests.Timeout:    Żądanie przekroczyło `timeout` sekund.
    """
    params: dict = {}
    if bbox:
        min_lat, max_lat, min_lon, max_lon = bbox
        params = {
            "lamin": min_lat,
            "lamax": max_lat,
            "lomin": min_lon,
            "lomax": max_lon,
        }

    response = requests.get(
        f"{OPENSKY_BASE_URL}/states/all",
        params=params,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()
