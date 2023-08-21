from sqlalchemy.schema import Table
from sqlalchemy import Column, Integer, String
from sqlalchemy_bigquery import GEOGRAPHY
from sqlalchemy import String
from sqlalchemy_bigquery import DATETIME
import config
from sqlalchemy.orm import registry

class Base():
    pass

mapper_registry = registry()

class POIData():
    __table__ = config.TABLE_NAME
    def to_dict(self):
        return {
            "id": self.id,
            "location": self.location,
            "data": self.data,
            "account_id": self.account_id,
            "last_update_datetime": self.last_update_datetime
        }
    
mapper_registry.map_imperatively(POIData, Table(
       config.TABLE_NAME,
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True),
        Column("location", GEOGRAPHY),
        Column("data", String),
        Column("account_id", String),
        Column("last_update_datetime", DATETIME(timezone=True))
))