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
    connection = engine.connect()
    
    metadata = db.MetaData()
    table = db.Table(app.config['TABLE_NAME'], metadata, autoload_with=engine)
    delete_table = db.Table(app.config['TABLE_NAME'] + "_deletes", metadata, autoload_with=engine)

    result = None
    if item_id is None:
        result = connection.execute(
            table.select().join(delete_table, table.c['ACCOUNT_ID'] == delete_table.c['ACCOUNT_ID'] and table.c['ID'] == delete_table.c['ID'] ,isouter=True, full=False)
            .where(table.c['ACCOUNT_ID'] == request.view_args['account_id'])).mappings().to_json(orient='records')
        logging.info(result)
    else:
        result = connection.execute(
            table.select().join(delete_table, table.c['ACCOUNT_ID'] == delete_table.c['ACCOUNT_ID'] and table.c['ID'] == delete_table.c['ID'] ,isouter=True, full=False)
            .where(table.c['ACCOUNT_ID'] == request.view_args['account_id'] and table.c['ID'] == item_id)).mappings().to_json(orient='records')
        logging.info(result)
    
    return Response(response=result, status=200)
    
@app.post("/" + app.config['TABLE_NAME'] + "/<path:account_id>")
def addItem(account_id):
    if account_id is None:
        return Response(response="Account ID required", status=400)
    
    engine = db.create_engine('bigquery://' + app.config['PROJECT_ID'] + '/' + app.config['DATASET_NAME'], credentials_path='google.key')
    connection = engine.connect()
    
    metadata = db.MetaData()
    table = db.Table(app.config['TABLE_NAME'], metadata, autoload_with=engine)
    
    index = connection.execute(db.text('call ' + app.config['DATASET_NAME'] + '.get_row_id()'), dict(account_id=account_id)).scalar()
    
    
    request_data = request.get_json()
    request_data['ID'] = index
    request_data['ACCOUNT_ID'] = account_id
    request_data['LAST_UPDATE_DATETIME'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    query = table.insert().values(request_data)
    my_session = Session(engine)
    my_session.execute(query)
    my_session.close()
    
    return Response(response=str(request_data).encode('utf-8'), status=201)


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