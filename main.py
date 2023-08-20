from flask import Flask, request, Response
import google.cloud.logging
import logging
import json
import datetime
# from create_task import create_task
import sqlalchemy as db
from sqlalchemy.orm import Session
from sqlalchemy import select
import geoalchemy2
from shapely.geometry import mapping, shape
import orm

logClient = google.cloud.logging.Client()
logClient.setup_logging()

app = Flask(__name__)
app.config.from_object('config')
app.secret_key = app.config['SECRET_KEY']
delete_delay=20

engine = db.create_engine('bigquery://' + app.config['PROJECT_ID'] + '/' + app.config['DATASET_NAME'], credentials_path='google.key')
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

@app.get("/" + app.config['TABLE_NAME'] + "/<path:account_id>", defaults={'item_id': None})
@app.get("/" + app.config['TABLE_NAME'] + "/<path:account_id>/<path:item_id>")
def getItems(account_id, item_id):
    if account_id is None:
        return Response(response="Account ID required", status=400)
    
    my_session = Session(engine) 
    result = None
    if item_id is None:
        result = my_session.execute(
            select(orm.POIData).join(orm.POIDeleteData, isouter=True, full=False)
                .where(orm.POIData.account_id == account_id)
                .where(orm.POIDeleteData.id == None)
            ).all()
    else:
        result = my_session.execute(
            select(orm.POIData).join(orm.POIDeleteData, isouter=True, full=False)
                .where(orm.POIData.account_id == account_id)
                .where(orm.POIData.id == int(item_id))
                .where(orm.POIDeleteData.id == None)
            ).all()
    my_session.close()
    
    out_results = []
    for r in result:
        o = r[0].to_dict()        
        o['location'] = mapping(geoalchemy2.shape.to_shape(o['location']))
        o['last_update_datetime'] = str(o['last_update_datetime'])
        out_results.append(o)
        
    return Response(response=json.dumps(out_results), status=200, mimetype="application/json")
    
@app.post("/" + app.config['TABLE_NAME'] + "/<path:account_id>")
def addItem(account_id):
    logging.info(account_id)
    if account_id is None:
        return Response(response="Account ID required", status=400)
    
    connection = engine.connect()
    index = connection.execute(db.text('call ' + app.config['DATASET_NAME'] + '.get_row_id()')).scalar()
    
    request_data = request.get_json()
    request_data['id'] = index
    request_data['account_id'] = account_id
    request_data['last_update_datetime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    request_data['location'] = shape(request_data['location']).wkt
    newRec = orm.POIData(**request_data)
    my_session = Session(engine)
    my_session.add(newRec)
    my_session.commit()
    my_session.flush()
    result = my_session.execute(select(orm.POIData).where(orm.POIData.id == index)).all()
    my_session.close()
    
    out_results = []
    for r in result:
        o = r[0].to_dict()        
        o['location'] = mapping(geoalchemy2.shape.to_shape(o['location']))
        o['last_update_datetime'] = str(o['last_update_datetime'])
        out_results.append(o)
         
    return Response(response=json.dumps(out_results), status=201, mimetype="application/json")

@app.delete("/" + app.config['TABLE_NAME'] + "/<path:account_id>/<path:item_id>")
def deleteItem(account_id, item_id):
    if account_id is None:
        return Response(response="Account ID required", status=400)
    
    if item_id is None:
        return Response(response="Item Account ID required", status=400)
    
    my_session = Session(engine) 
    result = my_session.execute(
        select(orm.POIData).join(orm.POIDeleteData, isouter=True, full=False)
            .where(orm.POIData.account_id == account_id)
            .where(orm.POIData.id == int(item_id))
            .where(orm.POIDeleteData.id == None)
        ).all()
    
    if len(result) == 0:
        return Response(response="Item does not exist", status=409)
    
    my_session.delete(result[0][0])
    my_session.commit()
    my_session.close()
    
    # connection = engine.connect()
    # connection.execute(db.text('call ' + app.config['DATASET_NAME'] + '.delete_row_id(:id, :account_id, :delete_delay)'), id=int(item_id), account_id=account_id, delete_delay=delete_delay)
    
    # topic = "projects/{project_id}/topics/{topic}".format(
    #     project_id=app.config['PROJECT_ID'],
    #     topic='inventory-record-removal',  # Set this to something appropriate.
    # )
    # url = app.config['TASK_URL'].format(topic=topic)
    
    # create_task(name="Delete-{item_id}".format(item_id=item_id), 
    #     project=app.config['PROJECT_ID'],
    #     location=app.config['LOCATION'],
    #     queue=app.config['QUEUE_NAME'],
    #     url=url,
    #     logging=logging,
    #     task_start=(datetime.datetime.now() + datetime.timedelta(minutes=delete_delay)),
    #     payload={
    #         "messages": [
    #             {
    #                 "data": base64.b64encode(json.dumps({
    #                     "item_id": item_id,
    #                     "account_id": account_id,
    #                     "delete_request": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    #                     "delete_after": (datetime.datetime.now() + datetime.timedelta(minutes=delete_delay)).strftime("%Y-%m-%d %H:%M:%S")
    #                 }).encode('ascii')).decode('ascii')
    #             }
    #         ]
    #     }
    # )
    
    return Response(response="Record marked for deletion", status=200)

@app.put("/" + app.config['TABLE_NAME'] + "/<path:account_id>/<path:item_id>")
def updateItem(account_id, item_id):
    if account_id is None:
        return Response(response="Account ID required", status=400)
    
    if item_id is None:
        return Response(response="Item Account ID required", status=400)
    
    my_session = Session(engine) 
    result = my_session.execute(
        select(orm.POIData).join(orm.POIDeleteData, isouter=True, full=False)
            .where(orm.POIData.account_id == account_id)
            .where(orm.POIData.id == int(item_id))
            .where(orm.POIDeleteData.id == None)
        ).all()
    
    if len(result) == 0:
        return Response(response="Item does not exist", status=409)
     
    request_data = request.get_json()
    poi_data = result[0][0]
    poi_data.data = request_data['data']
    poi_data.location = shape(request_data['location']).wkt
    poi_data.last_update_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    my_session.commit()
    my_session.flush()
    my_session.close()
    
    search_session = Session(engine) 
    search_res = search_session.execute(
        select(orm.POIData).join(orm.POIDeleteData, isouter=True, full=False)
            .where(orm.POIData.account_id == account_id)
            .where(orm.POIData.id == int(item_id))
            .where(orm.POIDeleteData.id == None)
        ).all()
    
    out_results = []
    for r in search_res:
        o = r[0].to_dict()        
        o['location'] = mapping(geoalchemy2.shape.to_shape(o['location']))
        o['last_update_datetime'] = str(o['last_update_datetime'])
        out_results.append(o)
         
    return Response(response=json.dumps(out_results), status=200, mimetype="application/json")

if __name__ == "__main__":
    # Development only: run "python main.py" and open http://localhost:8080
    # When deploying to Cloud Run, a production-grade WSGI HTTP server,
    # such as Gunicorn, will serve the app.
    app.run(host="localhost", port=8080, debug=True)