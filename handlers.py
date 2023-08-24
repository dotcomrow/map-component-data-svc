from flask import request, Response
from sqlalchemy.orm import Session
import config
import sqlalchemy as db
import logging
from sqlalchemy import select
import orm
from shapely.geometry import mapping, shape
import json
import geoalchemy2
from geoalchemy2 import functions as func
import datetime

engine = db.create_engine('bigquery://' + config.PROJECT_ID + '/' + config.DATASET_NAME, credentials_path='google.key')
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

def buildResponse(result):
    out_results = []
    for r in result:
        o = r[0].to_dict()        
        o['location'] = mapping(geoalchemy2.shape.to_shape(o['location']))
        o['last_update_datetime'] = str(o['last_update_datetime'])
        o['data'] = json.loads(o['data'])
        out_results.append(o)
    return out_results

def handle_getItems(account_id, item_id):
    my_session = Session(engine) 
    result = None
    if item_id is None:
        result = my_session.execute(
            select(orm.POIData)
                .where(orm.POIData.account_id == account_id)         
            ).all()
    else:
        result = my_session.execute(
            select(orm.POIData)
                .where(orm.POIData.account_id == account_id)
                .where(orm.POIData.id == int(item_id))                
            ).all()
    my_session.close()
    
    out_results = buildResponse(result) 
    return Response(response=json.dumps(out_results), status=200, mimetype="application/json")

def handle_getItemsWithinBox(account_id, bbox):
    my_session = Session(engine) 
    result = my_session.execute(
        select(orm.POIData)
            .where(orm.POIData.account_id == account_id)
            .where(func.ST_Within(orm.POIData.location, func.ST_GeogFromText(bbox)))
        ).all()
    my_session.close()
    
    out_results = buildResponse(result)
    return Response(response=json.dumps(out_results), status=200, mimetype="application/json")

def handle_addItem(account_id):
    connection = engine.connect()
    index = connection.execute(db.text('call ' + config.DATASET_NAME + '.get_row_id()')).scalar()
    my_session = Session(engine)
    request_data = request.get_json()

    try:
        request_data['id'] = index
        request_data['account_id'] = account_id
        request_data['last_update_datetime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        request_data['location'] = shape(request_data['location']).wkt
        request_data['data'] = json.dumps(request_data['data'])
        newRec = orm.POIData(**request_data)
        my_session.add(newRec)
        my_session.commit()
        my_session.flush()
    except Exception as e:
        logging.error(e)
    
    result = my_session.execute(select(orm.POIData).where(orm.POIData.id == index)).all()
    my_session.close()
    
    out_results = buildResponse(result)
    return Response(response=json.dumps(out_results), status=200, mimetype="application/json")

def handle_deleteItem(account_id, item_id):
    my_session = Session(engine) 
    result = my_session.execute(
        select(orm.POIData)
            .where(orm.POIData.account_id == account_id)
            .where(orm.POIData.id == int(item_id))
        ).all()
    
    if len(result) == 0:
        return Response(response="Item does not exist", status=404)
    
    my_session.delete(result[0][0])
    my_session.commit()
    my_session.close()
    
    return Response(response="Record marked for deletion", status=200)

def handle_updateItem(account_id, item_id):
    my_session = Session(engine) 
    result = my_session.execute(
        select(orm.POIData)
            .where(orm.POIData.account_id == account_id)
            .where(orm.POIData.id == int(item_id))
        ).all()
    
    if len(result) == 0:
        return Response(response="Item does not exist", status=404)
     
    request_data = request.get_json()
    poi_data = result[0][0]
    poi_data.data = json.dumps(request_data['data'])
    poi_data.location = shape(request_data['location']).wkt
    poi_data.last_update_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    my_session.commit()
    my_session.flush()
    my_session.close()
    
    search_session = Session(engine) 
    search_res = search_session.execute(
        select(orm.POIData)
            .where(orm.POIData.account_id == account_id)
            .where(orm.POIData.id == int(item_id))
        ).all()
    
    out_results = buildResponse(search_res)
    return Response(response=json.dumps(out_results), status=200, mimetype="application/json")