import os
import requests

from common.utils import upload_json_to_s3
from common.config import TOP_CITIES_TO_TAKE, CITIES_URL, WEATHER_URL, S3_RAW_PREFIX


class WeatherFetcher:
    """
    The WeatherFetcher is responsible for:
        1. Getting the top Indian cities data from "simplemaps" API
        2. For each city, fetch weather data for the passed date by calling "OpenWeatherMap" API
        3. Put this raw data on S3
    """

    def __init__(self, logger, date, s3_client):
        self.logger = logger
        self.date = date
        self.s3_client = s3_client

    def _get_cities(self):
        try:
            response = requests.get(CITIES_URL)
            if response.status_code == 200:
                self.logger.info("[✓] Cities fetch successful")
                return response.json()
            else:
                self.logger.error(
                    f"[✕] Failed to fetch cities. "
                    f"Status code: {response.status_code}. Response: {response.json()}"
                )
                return None
        except Exception as e:
            self.logger.error(f"[x] Error fetching cities: {str(e)}")
            return None

    def _get_weather(self, latitude, longitude):
        try:
            params = {
                "lat": latitude,
                "lon": longitude,
                "date": self.date,
                "units": "metric",
                "appid": os.environ.get("OPENWEATHERMAP_API_KEY"),
            }

            response = requests.get(WEATHER_URL, params=params)
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(
                    f"[x] Failed to fetch weather data for {latitude}, {longitude}. "
                    f"Status code: {response.status_code}. Response: {response.json()}"
                )
                return None
        except Exception as e:
            self.logger.error(
                f"[x] Error fetching weather data for {latitude}, {longitude}: {str(e)}"
            )
            return None

    def fetch_raw_data(self):
        """
        This is the main function
        """
        self.logger.info("[->] Starting raw data fetch")

        # Get all the cities
        cities_data = self._get_cities()
        if cities_data:
            # Limit to the top "x" cities
            cities_data = cities_data[:TOP_CITIES_TO_TAKE]

            self.logger.info("[->] Starting upload of raw cities data to S3")
            # Upload cities data to S3
            upload_json_to_s3(
                self.s3_client,
                self.logger,
                cities_data,
                f"{S3_RAW_PREFIX}/date={self.date}/city_data.json",
            )

            # Get the weather for each city
            for city in cities_data:
                city_name = city["city"]
                weather_data = self._get_weather(
                    latitude=city["lat"], longitude=city["lng"]
                )
                if weather_data:
                    # Upload weather data to S3
                    self.logger.info(
                        f"[->] Starting upload of {city_name} weather to S3"
                    )
                    upload_json_to_s3(
                        self.s3_client,
                        self.logger,
                        weather_data,
                        f"{S3_RAW_PREFIX}/date={self.date}/weather/{city_name}.json",
                    )
