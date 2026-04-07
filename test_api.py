import requests
key = "COLOQUE_SUA_CHAVE_OPENWEATHER_AQUI"
lat, lon = -32.035, -52.0986

url2 = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid={key}"
r2 = requests.get(url2).json()
print("KEYS:", list(r2.keys()))
if "hourly" in r2:
    print(r2["hourly"][0])

