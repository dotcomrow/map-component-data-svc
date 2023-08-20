from sqlalchemy.schema import Table
from sqlalchemy import Column, Integer, String
from sqlalchemy_bigquery import GEOGRAPHY
from sqlalchemy import String, ForeignKey
from sqlalchemy_bigquery import DATETIME
import config
from sqlalchemy.orm import registry
from sqlalchemy.dialects.postgresql import JSONB

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

class POIDeleteData():
    __table__ = config.TABLE_NAME + "_deletes"
    def to_dict(self):
        return {
            "id": self.id,
            "account_id": self.account_id,
            "requested_on": self.requested_on,
            "delete_after": self.delete_after
        }
    
mapper_registry.map_imperatively(POIData, Table(
       config.TABLE_NAME,
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True),
        Column("location", GEOGRAPHY),
        Column("data", JSON),
        Column("account_id", String),
        Column("last_update_datetime", DATETIME(timezone=True))
))

mapper_registry.map_imperatively(POIDeleteData, Table(
        config.TABLE_NAME + "_deletes",
        mapper_registry.metadata,
        Column("id", Integer, ForeignKey(POIData.id), primary_key=True),
        Column("account_id", String),
        Column("requested_on", DATETIME(timezone=True)),
        Column("delete_after", DATETIME(timezone=True))
))