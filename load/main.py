import pandas as pd
from sqlalchemy import inspect

from .models import Base
from common.utils import read_parquet_from_s3
from common.config import S3_BUCKET_NAME, S3_REFINED_PREFIX


class WeatherLoader:
    """
    The WeatherLoader is responsible for:
        1. Fetching the refined cities data from S3 and inserting the data to Postgres
        2. Fetching the refined weather data from S3 and inserting the data to Postgres
    """

    def __init__(self, logger, date, sqlalchemy_engine):
        self.logger = logger
        self.date = date
        self.sqlalchemy_engine = sqlalchemy_engine

    def _create_tables_if_not_exists(self):
        """
        Create the city and weather tables in the database if they are not there
        """
        existing_tables = inspect(self.sqlalchemy_engine).get_table_names()

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
            f"s3://{S3_BUCKET_NAME}/{S3_REFINED_PREFIX}/date={self.date}/city_data.parquet"
        )

        # TODO: move this to common.utils
        def upsert_to_postgres(table, conn, keys, data_iter):
            upsert_args = {"constraint": "dim_city_city_name_key"}
            for data in data_iter:
                data = {k: data[i] for i, k in enumerate(keys)}
                upsert_args["set_"] = data
                from sqlalchemy.dialects.postgresql import insert
                import sqlalchemy as sa

                meta = sa.MetaData()
                meta.bind = self.sqlalchemy_engine
                meta.reflect(bind=self.sqlalchemy_engine, views=True)
                insert_stmt = insert(meta.tables[table.name]).values(**data)
                upsert_stmt = insert_stmt.on_conflict_do_update(**upsert_args)
                conn.execute(upsert_stmt)

        # load_df_to_postgres(cities_df, sql, self.sqlalchemy_engine)
        cities_df.to_sql(
            "dim_city",
            con=self.sqlalchemy_engine,
            if_exists="append",
            method=upsert_to_postgres,
            index=False,
        )
        self.logger.info("[✓] Inserted city data to Postgres")

    def _load_weather_data(self):
        """
        Fetch the refined weather data from S3 and insert the data to Postgres
        """
        # Get the refined cities data from S3
        weather_df = read_parquet_from_s3(
            f"s3://{S3_BUCKET_NAME}/{S3_REFINED_PREFIX}/date={self.date}/weather_data.parquet"
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

        # TODO: move this to common.utils
        def upsert_to_postgres(table, conn, keys, data_iter):
            upsert_args = {"constraint": "fct_weather_date_city_id_key"}
            for data in data_iter:
                data = {k: data[i] for i, k in enumerate(keys)}
                upsert_args["set_"] = data
                from sqlalchemy.dialects.postgresql import insert
                import sqlalchemy as sa

                meta = sa.MetaData()
                meta.bind = self.sqlalchemy_engine
                meta.reflect(bind=self.sqlalchemy_engine, views=True)
                insert_stmt = insert(meta.tables[table.name]).values(**data)
                upsert_stmt = insert_stmt.on_conflict_do_update(**upsert_args)
                conn.execute(upsert_stmt)

        # load_df_to_postgres(cities_df, sql, self.sqlalchemy_engine)
        df_fct_weather_with_city_id.to_sql(
            "fct_weather",
            con=self.sqlalchemy_engine,
            if_exists="append",
            method=upsert_to_postgres,
            index=False,
        )

        self.logger.info("[✓] Inserted weather data to Postgres")

    def _get_data(self):
        """
        Read the data of the passed "date" from the Postgres DB
        """
        query = """
            SELECT *
            FROM dim_city
            ;
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
