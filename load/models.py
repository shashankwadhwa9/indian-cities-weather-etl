from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy import Column, Integer, String, Date, Float
from sqlalchemy.ext.declarative import declarative_base

# Define SQLAlchemy base
Base = declarative_base()


# Define dim_city table class
class DimCity(Base):
    __tablename__ = "dim_city"

    city_id = Column(Integer, primary_key=True)
    city_name = Column(String, unique=True)
    latitude = Column(Float)
    longitude = Column(Float)
    country = Column(String)


# Define fct_weather table class
class FctWeather(Base):
    __tablename__ = "fct_weather"

    id = Column(Integer, primary_key=True)
    date = Column(Date)
    city_id = Column(Integer, ForeignKey("dim_city.city_id"))
    min_temperature = Column(Float)
    max_temperature = Column(Float)

    # Define unique constraint on date and city_id
    __table_args__ = (UniqueConstraint("date", "city_id"),)
