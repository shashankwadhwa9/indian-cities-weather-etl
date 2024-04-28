import os
import json
import requests
import boto3

from common.config import CITIES_URL, WEATHER_URL, S3_BUCKET_NAME, S3_RAW_PREFIX

OPENWEATHERMAP_API_KEY = os.environ.get("OPENWEATHERMAP_API_KEY")
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_ACCESS_SECRET = os.environ.get("AWS_ACCESS_SECRET")


class WeatherFetcher:
    """
    The WeatherFetcher is responsible for:
        1. Getting the top Indian cities data from "simplemaps" API
        2. For each city, fetch yesterday's weather by calling "OpenWeatherMap" API
        3. Put this raw data on S3
    """

    def __init__(self, logger, date):
        self.logger = logger
        self.date = date
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_ACCESS_SECRET,
        )

    def _get_cities(self):
        try:
            response = requests.get(CITIES_URL)
            if response.status_code == 200:
                self.logger.info("[✓] Cities fetch successful")
                return response.json()
            else:
                self.logger.error(
                    f"[✕] Failed to fetch cities. Status code: {response.status_code}"
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
                "appid": OPENWEATHERMAP_API_KEY,
            }

            response = requests.get(WEATHER_URL, params=params)
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(
                    f"[x] Failed to fetch weather data for {latitude}, {longitude}. Status code: {response.status_code}"
                )
                return None
        except Exception as e:
            self.logger.error(
                f"[x] Error fetching weather data for {latitude}, {longitude}: {str(e)}"
            )
            return None

    def _upload_to_s3(self, data, path):
        try:
            self.s3_client.put_object(
                Body=json.dumps(data), Bucket=S3_BUCKET_NAME, Key=path
            )
            self.logger.info(f"[✓] Data uploaded to S3 at {path}")
        except Exception as e:
            self.logger.error(f"[x] Error uploading data to S3: {str(e)}")

    def fetch_raw_data(self):
        """
        This is the main function
        """
        self.logger.info("[->] Starting raw data fetch")

        # Get all the cities
        cities_data = self._get_cities()
        if cities_data:
            self.logger.info("[->] Starting upload of raw cities_data to S3")

            # Upload cities data to S3
            self._upload_to_s3(
                cities_data, f"{S3_RAW_PREFIX}/date={self.date}/city_data.json"
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
                    self._upload_to_s3(
                        cities_data,
                        f"{S3_RAW_PREFIX}/date={self.date}/weather_{city_name}.json",
                    )
