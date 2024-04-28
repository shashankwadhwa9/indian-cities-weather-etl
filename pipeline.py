import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
import boto3

from common.utils import valid_date
from extract.main import WeatherFetcher

OPENWEATHERMAP_API_KEY = os.environ.get("OPENWEATHERMAP_API_KEY")
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_ACCESS_SECRET = os.environ.get("AWS_ACCESS_SECRET")

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("indian_cities_weather_etl_pipeline")


class PipelineRunner:
    def __init__(self, date):
        """
        :param date: Date for which the weather data has to be fetched
        """
        self.logger = logger
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_ACCESS_SECRET,
        )
        self.date = date

    def _extract(self):
        """
        Get the raw weather data and upload to S3
        """
        fetcher = WeatherFetcher(logger=self.logger, date=self.date)
        fetcher.fetch_raw_data()

    def _transform(self):
        """
        Read the raw weather data, clean it to create refined data and upload to S3
        """
        pass

    def _load(self, transformed_df):
        """
        Read the refined data from S3 and load to Postgres database
        """
        pass

    def run(self):
        """
        Run the ETL pipeline
        """
        self._extract()
        self._transform()
        self._load()


def run_pipeline():
    parser = argparse.ArgumentParser(description="Indian cities weather ETL pipeline")

    # Arguments
    parser.add_argument(
        "--start-date",
        type=valid_date,
        default=datetime.today(),
        help="Start date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--end-date",
        type=valid_date,
        default=datetime.today(),
        help="End date in YYYY-MM-DD format (default: today)",
    )

    args = parser.parse_args()

    # Iterate through each date between start date and end date
    current_date = args.start_date
    while current_date <= args.end_date:
        logger.info(f"[->] Running the pipeline for {current_date}")
        pipeline = PipelineRunner(date=current_date)
        pipeline.run()
        current_date += timedelta(days=1)


if __name__ == "__main__":
    # Check if the OpenWeatherMap API key environment variable is set
    # This is required to get the weather data from their API
    if OPENWEATHERMAP_API_KEY is None:
        logger.error("[x] OpenWeatherMap API key not provided.")
        sys.exit()

    # Check if the AWS environment variables are set
    # These are required to put the raw and refined data on S3
    if AWS_ACCESS_KEY is None or AWS_ACCESS_SECRET is None:
        logger.error("[x] AWS credentials not provided.")
        sys.exit()

    # Run the pipeline
    run_pipeline()
