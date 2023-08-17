import datetime
import json

from google.cloud import tasks_v2
from google.protobuf import duration_pb2, timestamp_pb2
from google.oauth2.service_account import Credentials
import google.auth.transport.requests


def create_task(name:str, 
                project:str,
                location:str,
                queue:str,
                url:str,
                logging:object,
                payload:dict,
                task_start:datetime):
    client = tasks_v2.CloudTasksClient()

    project = project
    location = location
    queue = queue
    url = url
    task_name = name
    deadline = 900

    parent = client.queue_path(project, location, queue)
    
    credentials = Credentials.from_service_account_file(
        'google.key',
        scopes=['https://www.googleapis.com/auth/cloud-platform'])
    
    request = google.auth.transport.requests.Request()
    credentials.refresh(request)

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url
        }
    }
    if payload is not None:
        if isinstance(payload, dict):
            payload = json.dumps(payload)
        task["http_request"]["headers"] = {"Content-type": "application/json", "Authorization": "Bearer " + credentials.token}

        converted_payload = payload.encode()
        task["http_request"]["body"] = converted_payload
        logging.info("Creating task {}".format(payload))

    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(task_start)
    task["schedule_time"] = timestamp

    if task_name is not None:
        task["name"] = client.task_path(project, location, queue, task_name)

    if deadline is not None:
        duration = duration_pb2.Duration()
        task["dispatch_deadline"] = duration.FromSeconds(deadline)
    
    response = client.create_task(request={"parent": parent, "task": task})
    return response.name
