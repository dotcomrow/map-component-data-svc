from flask import Flask, request, Response
import google.cloud.logging
import handlers

logClient = google.cloud.logging.Client()
logClient.setup_logging()

app = Flask(__name__)
app.config.from_object('config')
app.secret_key = app.config['SECRET_KEY']

@app.get("/" + app.config['TABLE_NAME'] + "/<path:account_id>", defaults={'item_id': None})
@app.get("/" + app.config['TABLE_NAME'] + "/<path:account_id>/<path:item_id>")
def getItems(account_id, item_id):
    if account_id is None:
        return Response(response="Account ID required", status=400)
    
    return handlers.handle_getItems(account_id, item_id)

@app.get("/" + app.config['TABLE_NAME'] + "/<path:account_id>/features")
def getItemsWithinBox(account_id):
    if account_id is None:
        return Response(response="Account ID required", status=400)
    
    bbox = request.args.get('bbox')
    if bbox is None:
        return Response(response="Bounding box required", status=400)
    
    return handlers.handle_getItemsWithinBox(account_id, bbox)
    
@app.post("/" + app.config['TABLE_NAME'] + "/<path:account_id>")
def addItem(account_id):
    if account_id is None:
        return Response(response="Account ID required", status=400)
    
    return handlers.handle_addItem(account_id)

@app.delete("/" + app.config['TABLE_NAME'] + "/<path:account_id>/<path:item_id>")
def deleteItem(account_id, item_id):
    if account_id is None:
        return Response(response="Account ID required", status=400)
    
    if item_id is None:
        return Response(response="Item Account ID required", status=400)
    
    return handlers.handle_deleteItem(account_id, item_id)

@app.put("/" + app.config['TABLE_NAME'] + "/<path:account_id>/<path:item_id>")
def updateItem(account_id, item_id):
    if account_id is None:
        return Response(response="Account ID required", status=400)
    
    if item_id is None:
        return Response(response="Item Account ID required", status=400)
    
    return handlers.handle_updateItem(account_id, item_id)

if __name__ == "__main__":
    # Development only: run "python main.py" and open http://localhost:8080
    # When deploying to Cloud Run, a production-grade WSGI HTTP server,
    # such as Gunicorn, will serve the app.
    app.run(host="localhost", port=8080, debug=True)