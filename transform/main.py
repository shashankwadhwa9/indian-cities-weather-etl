import json
import pandas as pd

from common.config import (
    S3_BUCKET_NAME,
    S3_RAW_PREFIX,
    S3_REFINED_PREFIX,
    CITIES_DF_COL_LIST,
    WEATHER_DF_COL_LIST,
)
from common.utils import write_df_parquet_to_s3


class WeatherTransformer:
    """
    The WeatherTransformer is responsible for:
        1. Cleaning the cities data and uploading the refined cities data to S3
        2. Cleaning the weather data and uploading the refined weather data to S3
    """

    def __init__(self, logger, date, s3_client):
        self.logger = logger
        self.date = date
        self.s3_client = s3_client

    def _get_raw_cities_data(self):
        """
        Get the raw cities data from S3
        """
        obj = self.s3_client.get_object(
            Bucket=S3_BUCKET_NAME,
            Key=f"{S3_RAW_PREFIX}/date={self.date}/city_data.json",
        )
        data = obj["Body"].read().decode("utf-8")

        # Parse JSON data
        json_data = json.loads(data)

        # Convert JSON data to DataFrame
        df = pd.DataFrame(json_data)

        return df

    def _create_cities_refined_data(self):
        """
        Clean the raw cities data and upload the refined data to S3 in parquet format
        """
        # Get the raw cities data
        cities_raw_df = self._get_raw_cities_data()

        # Remove the not needed columns
        cities_refined_df = cities_raw_df[["city", "lat", "lng", "country"]]
        cities_refined_df.columns = CITIES_DF_COL_LIST

        self.logger.info("[->] Starting upload of refined cities data to S3")

        # Upload refined cities data to S3
        write_df_parquet_to_s3(
            self.s3_client,
            self.logger,
            cities_refined_df,
            f"{S3_REFINED_PREFIX}/date={self.date}/city_data.parquet",
        )

    def _get_raw_weather_data(self):
        """
        Get the raw weather data from S3
        """
        # List objects in the folder
        response = self.s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME, Prefix=f"{S3_RAW_PREFIX}/date={self.date}/weather/"
        )

        # Iterate through the objects and read each file
        dfs = []  # List to store DataFrames
        for obj in response["Contents"]:
            file_key = obj["Key"]
            if file_key.endswith(".json"):
                # Read JSON file from S3
                file_obj = self.s3_client.get_object(
                    Bucket=S3_BUCKET_NAME, Key=file_key
                )
                data = file_obj["Body"].read().decode("utf-8")

                # Load JSON data into a pandas DataFrame
                df = pd.json_normalize(json.loads(data))

                # Add a new column for the city name
                df["city_name"] = file_key.split("/")[-1].split(".")[0]

                # Append DataFrame to the list
                dfs.append(df)

        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df

    def _create_weather_refined_data(self):
        """
        Clean the raw weather data and upload the refined data to S3 in parquet format
        """
        # Get the raw weather data
        weather_raw_df = self._get_raw_weather_data()

        # Remove the not needed columns
        weather_refined_df = weather_raw_df[
            ["date", "city_name", "temperature.min", "temperature.max"]
        ]
        weather_refined_df.columns = WEATHER_DF_COL_LIST

        self.logger.info("[->] Starting upload of refined weather data to S3")

        # Upload refined weather data to S3
        write_df_parquet_to_s3(
            self.s3_client,
            self.logger,
            weather_refined_df,
            f"{S3_REFINED_PREFIX}/date={self.date}/weather_data.parquet",
        )

    def create_refined_data(self):
        """
        This is the main function
        """
        self.logger.info("[->] Starting data refinement")

        self._create_cities_refined_data()
        self._create_weather_refined_data()
