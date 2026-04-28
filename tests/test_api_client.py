"""
Testy dla src/api_client.py.

Używamy `requests_mock` żeby NIE robić prawdziwych żądań HTTP podczas testów.
Dzięki temu testy działają offline, są szybkie i deterministyczne
(nie zależą od dostępności API OpenSky).
"""

import pytest
import requests
from src.api_client import get_states_all


# Przykładowa odpowiedź API — dokładnie taki kształt zwraca OpenSky
MOCK_RESPONSE = {
    "time": 1700000000,
    "states": [
        ["a1b2c3", "LOT123  ", "Poland",    1700000000, 1700000000,  21.01,  52.22, 10000.0, False, 230.5, 88.0,  0.5, None, 10100.0, "7700", False, 0],
        ["x9y8z7", "RYR456  ", "Ireland",   1700000000, 1700000000,  -6.25,  53.33,  9500.0, False, 245.0, 270.0, 0.0, None,  9600.0, "1234", False, 0],
        ["b3c4d5", "DLH789  ", "Germany",   1700000000, 1700000000,  13.40,  52.52, 11000.0, False, 260.0, 90.0, -1.0, None, 11100.0, "5678", False, 0],
    ],
}


class TestGetStatesAll:

    def test_zwraca_poprawny_ksztalt_odpowiedzi(self, requests_mock):
        """Happy path: API zwraca 200 OK → funkcja zwraca dict z 'time' i 'states'."""
        # requests_mock przechwytuje każde żądanie GET do tego URL
        # i odpowiada naszymi fałszywymi danymi zamiast prawdziwego API
        requests_mock.get(
            "https://opensky-network.org/api/states/all",
            json=MOCK_RESPONSE,
        )

        result = get_states_all()

        assert "time" in result
        assert "states" in result
        assert result["time"] == 1700000000
        assert len(result["states"]) == 3

    def test_kazdy_state_vector_ma_17_pol(self, requests_mock):
        """Każdy wiersz w 'states' musi mieć dokładnie 17 wartości."""
        requests_mock.get(
            "https://opensky-network.org/api/states/all",
            json=MOCK_RESPONSE,
        )

        result = get_states_all()

        for i, state in enumerate(result["states"]):
            assert len(state) == 17, (
                f"State vector #{i} ma {len(state)} pól zamiast 17"
            )

    def test_blad_http_rzuca_wyjatek(self, requests_mock):
        """Jeśli API zwróci 500, funkcja powinna rzucić HTTPError."""
        requests_mock.get(
            "https://opensky-network.org/api/states/all",
            status_code=500,
        )

        # pytest.raises sprawdza że dany wyjątek JEST rzucony
        # jeśli wyjątek NIE zostanie rzucony → test nie przechodzi
        with pytest.raises(requests.HTTPError):
            get_states_all()

    def test_blad_429_too_many_requests(self, requests_mock):
        """OpenSky limituje anonimowe zapytania — 429 też powinien rzucić HTTPError."""
        requests_mock.get(
            "https://opensky-network.org/api/states/all",
            status_code=429,
        )

        with pytest.raises(requests.HTTPError):
            get_states_all()

    def test_bbox_przekazuje_parametry_do_url(self, requests_mock):
        """Gdy podamy bounding box, URL powinien zawierać parametry lamin/lamax/lomin/lomax."""
        requests_mock.get(
            "https://opensky-network.org/api/states/all",
            json={"time": 1700000000, "states": []},
        )

        get_states_all(bbox=(49.0, 55.0, 14.0, 24.0))

        # requests_mock zapamiętuje jakie żądanie zostało wysłane
        sent_qs = requests_mock.last_request.qs  # query string jako dict

        assert "lamin" in sent_qs
        assert "lamax" in sent_qs
        assert "lomin" in sent_qs
        assert "lomax" in sent_qs
        assert sent_qs["lamin"] == ["49.0"]
        assert sent_qs["lamax"] == ["55.0"]

    def test_brak_bbox_nie_dodaje_parametrow(self, requests_mock):
        """Bez bbox URL nie powinien mieć parametrów filtrowania."""
        requests_mock.get(
            "https://opensky-network.org/api/states/all",
            json=MOCK_RESPONSE,
        )

        get_states_all()

        sent_qs = requests_mock.last_request.qs
        assert "lamin" not in sent_qs
        assert "lomin" not in sent_qs

    def test_pusta_lista_states_jest_poprawna(self, requests_mock):
        """API może zwrócić pustą listę states (np. w nocy) — nie powinno rzucać błędu."""
        requests_mock.get(
            "https://opensky-network.org/api/states/all",
            json={"time": 1700000000, "states": []},
        )

        result = get_states_all()

        assert result["states"] == []
