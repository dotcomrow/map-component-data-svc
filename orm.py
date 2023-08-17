from sqlalchemy.schema import Table
from sqlalchemy import Column, Integer, String
from geoalchemy2 import Geometry
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy_bigquery import DATETIME
import config

class Base(DeclarativeBase):
    pass

class POIData(Base):
    __table__ = Table(
       config.TABLE_NAME,
        Base.metadata,
        Column("id", Integer, primary_key=True),
        Column("location", Geometry("POINT")),
        Column("data", String),
        Column("account_id", String),
        Column("last_update_datetime", DATETIME(timezone=True))
    )

class POIDeleteData(Base):
    __table__ = Table(
        config.TABLE_NAME + "_deletes",
        Base.metadata,
        Column("id", Integer, primary_key=True),
        Column("account_id", String),
        Column("requested_on", DATETIME(timezone=True)),
        Column("delete_after", DATETIME(timezone=True))
    )