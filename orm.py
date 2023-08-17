from sqlalchemy.schema import Table
from sqlalchemy import Column, Integer, String
from geoalchemy2 import Geography
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_bigquery import DATETIME
import config
import json
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import registry

mapper_registry = registry()

class AlchemyEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj.__class__, DeclarativeMeta):
            # an SQLAlchemy class
            fields = {}
            for field in [x for x in dir(obj) if not x.startswith('_') and x != 'metadata']:
                data = obj.__getattribute__(field)
                try:
                    json.dumps(data) # this will fail on non-encodable values, like other classes
                    fields[field] = data
                except TypeError:
                    fields[field] = None
            # a json-encodable dict
            return fields

        return json.JSONEncoder.default(self, obj)


Base = declarative_base()

class POIData(Base):
    pass

class POIDeleteData(Base):
    pass
    
mapper_registry.map_imperatively(POIData, Table(
       config.TABLE_NAME,
        Base.metadata,
        Column("id", Integer, primary_key=True),
        Column("location", Geography("POINT")),
        Column("data", String),
        Column("account_id", String),
        Column("last_update_datetime", DATETIME(timezone=True))
))

mapper_registry.map_imperatively(POIDeleteData, Table(
        config.TABLE_NAME + "_deletes",
        Base.metadata,
        Column("id", Integer, primary_key=True),
        Column("account_id", String),
        Column("requested_on", DATETIME(timezone=True)),
        Column("delete_after", DATETIME(timezone=True))
))