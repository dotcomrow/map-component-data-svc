from sqlalchemy.schema import Table
from sqlalchemy import Column, Integer, String
from geoalchemy2 import Geography
from sqlalchemy import String
from sqlalchemy_bigquery import DATETIME
import config
from sqlalchemy.orm import registry

mapper_registry = registry()

class POIData():
    __table__ = config.TABLE_NAME
    pass

class POIDeleteData():
    __table__ = config.TABLE_NAME + "_deletes"
    pass
    
mapper_registry.map_imperatively(POIData, Table(
       config.TABLE_NAME,
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True),
        Column("location", Geography("POINT")),
        Column("data", String),
        Column("account_id", String),
        Column("last_update_datetime", DATETIME(timezone=True))
))

mapper_registry.map_imperatively(POIDeleteData, Table(
        config.TABLE_NAME + "_deletes",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True),
        Column("account_id", String),
        Column("requested_on", DATETIME(timezone=True)),
        Column("delete_after", DATETIME(timezone=True))
))