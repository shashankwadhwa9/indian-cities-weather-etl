TOP_CITIES_TO_TAKE = 10

# Urls
CITIES_URL = "https://simplemaps.com/static/data/country-cities/in/in.json"
WEATHER_URL = "https://api.openweathermap.org/data/3.0/onecall/day_summary"

# S3 paths
S3_BUCKET_NAME = "indian-cities-weather-etl"
S3_RAW_PREFIX = "raw"
S3_REFINED_PREFIX = "refined"

# Refined dataframe final columns list
CITIES_DF_COL_LIST = ["city_name", "latitude", "longitude", "country"]
WEATHER_DF_COL_LIST = [
    "date",
    "city_name",
    "min_temperature",
    "max_temperature",
    "total_precipitation",
]

# Postgres DB
DB_USER = "avnadmin"
DB_HOST = "indian-cities-weather-indian-cities-weather-etl.j.aivencloud.com"
DB_PORT = "21558"
DB_NAME = "defaultdb"
