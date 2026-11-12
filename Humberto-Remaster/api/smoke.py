"""Run the HTTP checks with the API and built web app listening locally."""
import json
from urllib.error import HTTPError
from urllib.request import Request, urlopen

BASE = "http://127.0.0.1:5180"


def get(path):
    with urlopen(BASE + path, timeout=10) as response:
        return json.load(response)


def compare(origin, destination, preference):
    payload = json.dumps({
        "origin": origin, "destination": destination, "preference": preference,
    }).encode()
    request = Request(BASE + "/api/routes/compare", payload, {"Content-Type": "application/json"})
    with urlopen(request, timeout=10) as response:
        return json.load(response)


def main():
    health = get("/api/health")
    assert get("/api/stations")["items"]
    assert get("/api/lines")["items"]
    assert get("/api/stations/42")["item"]["name"] == "LUZ"
    assert get("/api/lines/1")["item"]["stations"] > 0
    forward = compare("Luz", "Paraíso", "fewer_transfers")
    reverse = compare("Paraíso", "Luz", "fewer_stops")
    assert forward["version"] == reverse["version"] == health["version"]
    assert [(route["stops"], route["transfers"]) for route in forward["options"]] == [(6, 0), (6, 1)]
    assert reverse["options"][0]["legs"][0]["stations"][0]["name"] == "PARAÍSO"
    try:
        compare("Sé", "Luz", "fewer_transfers")
    except HTTPError as error:
        assert error.code == 400
    else:
        raise AssertionError("Route outside the pilot must return HTTP 400")
    with urlopen(BASE + "/", timeout=10) as response:
        page = response.read().decode()
    assert "<title>Humberto" in page and "assets/" in page
    print("API, ida e volta, cobertura e app compilado verificados. Gold", health["version"])


if __name__ == "__main__":
    main()
