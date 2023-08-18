from flask import Flask, request, Response
import google.cloud.logging
import logging
import json
import base64
import datetime
import pandas as pd
from create_task import create_task
import sqlalchemy as db
from sqlalchemy.orm import Session
from sqlalchemy import select, insert
import geoalchemy2
from shapely.geometry import Point, mapping, shape
import orm

app = Flask(__name__)
logClient = google.cloud.logging.Client()
logClient.setup_logging()

app.config.from_object('config')
app.secret_key = app.config['SECRET_KEY']
delete_delay=20

@app.get("/" + app.config['TABLE_NAME'] + "/<path:account_id>", defaults={'item_id': None})
@app.get("/" + app.config['TABLE_NAME'] + "/<path:account_id>/<path:item_id>")
def getItems(account_id, item_id):
    if account_id is None:
        return Response(response="Account ID required", status=400)
    
    engine = db.create_engine('bigquery://' + app.config['PROJECT_ID'] + '/' + app.config['DATASET_NAME'], credentials_path='google.key')
    my_session = Session(engine) 
    result = None
    if item_id is None:
        result = my_session.execute(
            select(orm.POIData).join(orm.POIDeleteData
                    ,orm.POIData.account_id == orm.POIDeleteData.account_id and orm.POIData.id == orm.POIDeleteData.id ,isouter=False, full=False
                    )
            .where(orm.POIData.account_id == account_id)).all()
    else:
        result = my_session.execute(
            select(orm.POIData).join(orm.POIDeleteData
                    ,orm.POIData.account_id == orm.POIDeleteData.account_id and orm.POIData.id == orm.POIDeleteData.id ,isouter=False, full=False
                    )
            .where(orm.POIData.account_id == account_id and orm.POIData.id == item_id)).all()
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
    if account_id is None:
        return Response(response="Account ID required", status=400)
    
    engine = db.create_engine('bigquery://' + app.config['PROJECT_ID'] + '/' + app.config['DATASET_NAME'], credentials_path='google.key')
    connection = engine.connect()
    
    index = connection.execute(db.text('call ' + app.config['DATASET_NAME'] + '.get_row_id()')).scalar()
    
    request_data = request.get_json()
    request_data['id'] = index
    request_data['account_id'] = account_id
    request_data['last_update_datetime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    request_data['location'] = shape(request_data['location']).wkb
    newRec = orm.POIData(**request_data)
    my_session = Session(engine)
    my_session.add(newRec)
    my_session.commit()
    my_session.flush()
    result = my_session.execute(select(orm.POIData).where(orm.POIData.ID == index)).all()
    my_session.close()
    
    out_results = []
    for r in result:
        o = r[0].to_dict()        
        o['location'] = mapping(geoalchemy2.shape.to_shape(o['location']))
        o['last_update_datetime'] = str(o['last_update_datetime'])
        out_results.append(o)
         
    return Response(response=json.dumps(out_results), status=201, mimetype="application/json")


# @app.put("/" + app.config['TABLE_NAME'])
# def updateItem():
#     query = client.query("UPDATE `" + table_string + "` set item_description = '" + request.form['item_description'] + "' where account_id = '" + user['sub'] + "' and " + app.config['TABLE_PK'] + " = '" + request.form['item_id'] + "' and create_datetime < DATETIME_SUB(CURRENT_DATETIME(), INTERVAL " + delete_delay + " MINUTE)")
#     query.result()
#     if query.num_dml_affected_rows > 0:
#         return client.query("SELECT * from `" + table_string + "` where account_id = '" + user['sub'] + "' and " + app.config['TABLE_PK'] + " = '" + request.form['item_id'] + "'").to_dataframe().to_json(orient='records')
#     else:
#         # publisher = pubsub_v1.PublisherClient()
#         # topic_name = 'projects/{project_id}/topics/{topic}'.format(
#         #     project_id=app.config['PROJECT_ID'],
#         #     topic='notify_user',  # Set this to something appropriate.
#         # )
#         # future = publisher.publish(topic_name, json.dumps(user).encode('utf-8'),)
#         # future.result()
    
#         qurystr = """SELECT t1.* FROM `{table_string}` t1 
#                                LEFT JOIN `{delete_table_string}` t2 ON t2.item_id = t1.item_id 
#                                WHERE t2.item_id IS NULL AND t1.account_id = '{account_id}' and t1.item_id = '{item_id}' and t1.create_datetime > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL {delete_delay} MINUTE)""".format(table_string=table_string, delete_table_string=delete_table_string, account_id=user['sub'], item_id=request.form['item_id'], delete_delay=delete_delay)
                               
#         logging.info(qurystr)
#         ctquery = client.query(qurystr)
#         ctquery.result()
#         if len(ctquery.to_dataframe()) == 0:
#             return Response(response="Item does not exist", status=409)
        
#         jsonObj = {
#             'item_id' : request.form['item_id'],
#             'delete_request':datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#             'delete_after':(datetime.datetime.now() + datetime.timedelta(minutes=delete_delay)).strftime("%Y-%m-%d %H:%M:%S")
#         }
        
#         rows_to_insert = [jsonObj]
#         client.insert_rows_json(delete_table_id, rows_to_insert)  # Make an API request.
        
#         topic = "projects/{project_id}/topics/{topic}".format(
#             project_id=app.config['PROJECT_ID'],
#             topic='inventory-record-removal',  # Set this to something appropriate.
#         )
#         url = app.config['TASK_URL'].format(topic=topic)
        
#         create_task(name="Delete-{item_id}".format(item_id=request.form['item_id']), 
#             project=app.config['PROJECT_ID'],
#             location=app.config['LOCATION'],
#             queue=app.config['QUEUE_NAME'],
#             url=url,
#             logging=logging,
#             task_start=(datetime.datetime.now() + datetime.timedelta(minutes=delete_delay)),
#             payload={
#                 "messages": [
#                     {
#                         "data": base64.b64encode(json.dumps({
#                             "item_id": request.form['item_id'],
#                             "account_id": user['sub'],
#                             "delete_request": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#                             "delete_after": (datetime.datetime.now() + datetime.timedelta(minutes=delete_delay)).strftime("%Y-%m-%d %H:%M:%S")
#                         }).encode('ascii')).decode('ascii')
#                     }
#                 ]
#             }
#         )
        
#         keyObj = {
#             'item_description' : request.form['item_description'],
#         }
         
#         row_id = client.query("SELECT TO_HEX(MD5(\"" + build_key(keyObj, user) + "\")) as md5").to_dataframe().iloc[0]['md5']
        
#         item_resp = client.query("SELECT * from `" + table_string + "` where account_id = '" + user['sub'] + "' and " + app.config['TABLE_PK'] + " = '" + request.form['item_id'] + "' and create_datetime > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL " + delete_delay + " MINUTE)").to_dataframe()
#         item_resp['item_description'] = request.form['item_description'] 
#         item_resp['item_id'] = row_id
#         item_resp['create_datetime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
        
#         rows_to_insert = [item_resp.to_dict(orient='records')[0]]
#         errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request. 
#         if errors == []:
#             return Response(response=str(item_resp.to_dict(orient='records')[0]).encode('utf-8'), status=201)
#         else:
#             return Response(response="Encountered errors while inserting rows: {}".format(errors), status=500) 
        
    
# @app.delete("/" + app.config['TABLE_NAME'])
# def deleteItem():
#     query = client.query("DELETE FROM `" + table_string + "` where account_id = '" + user['sub'] + "' and " + app.config['TABLE_PK'] + " = '" + request.form['item_id'] + "' and create_datetime < DATETIME_SUB(CURRENT_DATETIME(), INTERVAL " + delete_delay + " MINUTE)")
#     query.result()
#     logging.info("Deleted {num} rows".format(num=query.num_dml_affected_rows))
#     if query.num_dml_affected_rows > 0:
#         return Response(response="Item deleted", status=200)
#     else:
#         ctquery = client.query("SELECT * FROM `" + table_string + "` where account_id = '" + user['sub'] + "' and " + app.config['TABLE_PK'] + " = '" + request.form['item_id'] + "'  and create_datetime > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL " + delete_delay + " MINUTE)")
#         ctquery.result()
#         if ctquery.to_dataframe().size == 0:
#             errorTxt = "Item does not exist"
#             return Response(response="Delete error: {error}".format(error=errorTxt), status=409)
            
#         ctquery = client.query("SELECT count(*) FROM `" + delete_table_string + "` where " + app.config['TABLE_PK'] + " = '" + request.form['item_id'] + "'")
#         ctquery.result()
#         if ctquery.to_dataframe().iloc[0]['f0_'] > 0:
#             return Response(response="Record marked for deletion", status=200)
             
#         jsonObj = {
#             'item_id' : request.form['item_id'],
#             'delete_request':datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#             'delete_after':(datetime.datetime.now() + datetime.timedelta(minutes=delete_delay)).strftime("%Y-%m-%d %H:%M:%S")
#         }
        
#         rows_to_insert = [jsonObj]
#         client.insert_rows_json(delete_table_id, rows_to_insert)  # Make an API request.
        
#         topic = "projects/{project_id}/topics/{topic}".format(
#             project_id=app.config['PROJECT_ID'],
#             topic='inventory-record-removal',  # Set this to something appropriate.
#         )
#         url = app.config['TASK_URL'].format(topic=topic)
        
#         create_task(name="Delete-{item_id}".format(item_id=request.form['item_id']), 
#             project=app.config['PROJECT_ID'],
#             location=app.config['LOCATION'],
#             queue=app.config['QUEUE_NAME'],
#             url=url,
#             logging=logging,
#             task_start=(datetime.datetime.now() + datetime.timedelta(minutes=delete_delay)),
#             payload={
#                 "messages": [
#                     {
#                         "data": base64.b64encode(json.dumps({
#                             "item_id": request.form['item_id'],
#                             "account_id": user['sub'],
#                             "delete_request": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#                             "delete_after": (datetime.datetime.now() + datetime.timedelta(minutes=delete_delay)).strftime("%Y-%m-%d %H:%M:%S")
#                         }).encode('ascii')).decode('ascii')
#                     }
#                 ]
#             }
#         )
        
#         return Response(response="Record marked for deletion", status=200)

if __name__ == "__main__":
    # Development only: run "python main.py" and open http://localhost:8080
    # When deploying to Cloud Run, a production-grade WSGI HTTP server,
    # such as Gunicorn, will serve the app.
    app.run(host="localhost", port=8080, debug=True)