import pandas as pd

from .models import Base
from common.utils import read_parquet_from_s3, load_df_to_postgres
from common.config import S3_REFINED_PREFIX


class WeatherLoader:
    """
    The WeatherLoader is responsible for:
        1. Fetching the refined cities data from S3 and inserting the data to Postgres
        2. Fetching the refined weather data from S3 and inserting the data to Postgres
    """

    def __init__(self, logger, date, s3_client, sqlalchemy_engine):
        self.logger = logger
        self.date = date
        self.s3_client = s3_client
        self.sqlalchemy_engine = sqlalchemy_engine

    def _create_tables_if_not_exists(self):
        """
        Create the city and weather tables in the database if they are not there
        """
        existing_tables = self.sqlalchemy_engine.table_names()

        if "dim_city" not in existing_tables:
            self.logger.info("[->] Creating dim_city table...")
            Base.metadata.tables["dim_city"].create(self.sqlalchemy_engine)

        if "fct_weather" not in existing_tables:
            self.logger.info("[->] Creating fct_weather table...")
            Base.metadata.tables["fct_weather"].create(self.sqlalchemy_engine)

    def _load_cities_data(self):
        """
        Fetch the refined cities data from S3 and insert the data to Postgres
        """
        # Get the refined cities data from S3
        cities_df = read_parquet_from_s3(
            f"{S3_REFINED_PREFIX}/date={self.date}/city_data.parquet"
        )

        # Define the SQL statement
        sql = """
            INSERT INTO dim_city (city_name, latitude, longitude, country)
            VALUES (:city_name, :latitude, :longitude, :country)
            ON CONFLICT (city_name) DO UPDATE
            SET latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                country = EXCLUDED.country;
        """

        load_df_to_postgres(cities_df, sql)
        self.logger.info("[✓] Inserted city data to Postgres")

    def _load_weather_data(self):
        """
        Fetch the refined weather data from S3 and insert the data to Postgres
        """
        # Get the refined cities data from S3
        weather_df = read_parquet_from_s3(
            f"{S3_REFINED_PREFIX}/date={self.date}/weather_data.parquet"
        )

        # Fetch city_id for each city_name in fct_weather DataFrame
        city_id_query = """
            SELECT city_id, city_name
            FROM dim_city
            ;
        """
        city_id_df = pd.read_sql_query(city_id_query, con=self.sqlalchemy_engine)

        # Merge city_id_df with df_fct_weather DataFrame to map city_name to city_id
        df_fct_weather_with_city_id = pd.merge(
            weather_df, city_id_df, on="city_name", how="inner"
        )
        df_fct_weather_with_city_id.drop(columns=["city_name"], inplace=True)

        # Define the SQL statement
        sql = """
            INSERT INTO fct_weather (date, city_id, min_temperature, max_temperature)
            VALUES (:date, :city_id, :min_temperature, :max_temperature)
            ON CONFLICT (date, city_id) DO UPDATE
            SET min_temperature = EXCLUDED.min_temperature,
                max_temperature = EXCLUDED.max_temperature;
        """

        load_df_to_postgres(df_fct_weather_with_city_id, sql)
        self.logger.info("[✓] Inserted weather data to Postgres")

    def _get_data(self):
        """
        Read the data of the passed "date" from the Postgres DB
        """
        query = f"""
            SELECT *
            FROM fct_weather
            WHERE date = {self.date}
        """
        output_df = pd.read_sql_query(query, con=self.sqlalchemy_engine)
        return output_df

    def load(self):
        """
        This is the main function
        """
        self.logger.info("[->] Starting data load")

        self._create_tables_if_not_exists()
        self._load_cities_data()
        self._load_weather_data()
        return self._get_data()
