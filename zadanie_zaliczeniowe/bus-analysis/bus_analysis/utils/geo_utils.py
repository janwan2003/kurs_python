from math import radians, cos, sin, asin, sqrt


# Funkcja pomocnicza do obliczania odległości między punktami geograficznymi
def haversine(lon1, lat1, lon2, lat2):
    """
    Oblicz odległość między dwoma punktami na kuli ziemskiej określonymi w stopniach.
    """
    # Konwersja stopni na radiany
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # Różnica długości i szerokości geograficznej
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    # Formuła haversine
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Promień Ziemi w kilometrach
    return c * r


# Funkcja do obliczania prędkości autobusu między dwoma punktami czasowymi
def calculate_speed(df):
    speeds = []
    for i in range(1, len(df)):
        dist = haversine(
            df.iloc[i - 1]["Lon"],
            df.iloc[i - 1]["Lat"],
            df.iloc[i]["Lon"],
            df.iloc[i]["Lat"],
        )
        # Czas w godzinach, ponieważ odległość jest w km, a czas w minutach, chcemy prędkość w km/h
        time_diff = 1 / 60
        speed = dist / time_diff
        speeds.append(speed)
    return speeds
