import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import text
import sqlalchemy as sa

from .models import DimCity, FctWeather
from common.utils import read_parquet_from_s3
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
        meta = sa.MetaData()
        meta.bind = self.sqlalchemy_engine
        meta.reflect(bind=self.sqlalchemy_engine, views=True)

        # Get table names from the database
        existing_tables = meta.tables.keys()

        models = [DimCity, FctWeather]
        # Loop through each model and compare with existing tables
        for model in models:
            table_name = model.__tablename__

            # Check if the table exists in the database
            if table_name not in existing_tables:
                # If the table doesn't exist, create it
                model.__table__.create(self.sqlalchemy_engine)

                self.logger.info(f"[✓] Table '{table_name}' created in the database")
            else:
                # If the table exists, compare columns
                existing_columns = set(meta.tables[table_name].columns.keys())
                model_columns = set(model.__table__.columns.keys())

                # Find new columns in the model
                new_columns = model_columns - existing_columns

                # Add new columns to the database
                for column_name in new_columns:
                    column = model.__table__.columns[column_name]
                    column_type = column.type  # Get the type of the column
                    alter_query = (
                        f"ALTER TABLE {table_name} "
                        f"ADD COLUMN {column_name} {column_type}"
                    )
                    with self.sqlalchemy_engine.connect() as connection:
                        connection.execute(text(alter_query))
                        connection.commit()

                    print(f"Column '{column_name}' added to table '{table_name}'.")

    def _load_cities_data(self):
        """
        Fetch the refined cities data from S3 and insert the data to Postgres
        """
        # Get the refined cities data from S3
        cities_df = read_parquet_from_s3(
            self.s3_client,
            self.logger,
            f"{S3_REFINED_PREFIX}/date={self.date}/city_data.parquet",
        )

        # TODO: move this to common.utils
        def upsert_to_postgres(table, conn, keys, data_iter):
            upsert_args = {"constraint": "dim_city_city_name_key"}
            for data in data_iter:
                data = {k: data[i] for i, k in enumerate(keys)}
                upsert_args["set_"] = data
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
            self.s3_client,
            self.logger,
            f"{S3_REFINED_PREFIX}/date={self.date}/weather_data.parquet",
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
        self.logger.info(
            f"Weather report for {self.date} (Sorted hottest -> coldest by max_temperature)"
        )
        query = f"""
            SELECT
                dim_city.city_name,
                fct_weather.max_temperature,
                fct_weather.min_temperature
            FROM fct_weather
            INNER JOIN dim_city
                ON fct_weather.city_id = dim_city.city_id
            WHERE fct_weather.date = '{self.date}'
            ORDER BY fct_weather.max_temperature DESC
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
