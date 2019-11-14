from flask import Flask, render_template
from flask_restful import Api, Resource, reqparse

import os
from dotenv import load_dotenv

from Utils import service, database

# Load Environment variable
APP_ROOT = os.path.join(os.path.dirname(__file__), '.')
dotenv_path = os.path.join(APP_ROOT, ".env")
print('Loading environment variables in ', dotenv_path)
load_dotenv(dotenv_path)

# Database Config
maria_address = os.getenv('MARIA_ADDRESS')
maria_database = os.getenv('MARIA_DATABASE')
maria_user = os.getenv('MARIA_USER')
maria_password = os.getenv('MARIA_PASSWORD')
maria_port = os.getenv('MARIA_PORT')

# Print Maria config to examinate
print('MariaDB IP: ', maria_address)
print('MariaDB Database Name: ', maria_database)
print('MariaDB user: ', maria_user)
print('MariaDB password: ', maria_password)
print('MariaDB Port: ', maria_port)

# APP

app = Flask(__name__)
api = Api(app)

predictions = []

'''
 {
  "eventID": "evt_123",
  "startTime": "2019-11-01 03:24:47",
  "endTime": "2019-11-05 08:54:20",
  "connector": "mysql",
  "sensor_kw": "sensors",
  "device_kw": "devices",
  "eventTableName": "Event",
  "deviceLogTableName": "DeviceLog"
  "result": {}
  "devices": {}
}
'''
'''
Accept Multiple params
https://stackoverflow.com/questions/48095713/accepting-multiple-parameters-in-flask-restful-add-resource
'''

predict_parser = reqparse.RequestParser()
predict_parser.add_argument('eventID', type=str, required=True)
predict_parser.add_argument('startTime', type=str, required=True)
predict_parser.add_argument('endTime', type=str, required=True)
predict_parser.add_argument('y_Device',type=str, required=True)
predict_parser.add_argument('x_Device', type=str, required=True)
predict_parser.add_argument('host_ip',type=str, required=False, default=maria_address)
predict_parser.add_argument('database_name',type=str, required=False, default=maria_database)
predict_parser.add_argument('user',type=str, required=False, default=maria_user)
predict_parser.add_argument('password', type=str, required=False, default=maria_password)
predict_parser.add_argument('port', type=str, required=False, default=maria_port)
predict_parser.add_argument('sensor_kw', type=str, required=False, default="sensors")
predict_parser.add_argument('device_kw', type=str, required=False, default="devices")
predict_parser.add_argument("eventTableName", type=str, required=False, default="Event")
predict_parser.add_argument("deviceLogTableName", type=str, required=False, default="DeviceLog")
predict_parser.add_argument("connector", type=str, required=False, default="mysql")

class Prediction(Resource):
    def get(self):
        args = predict_parser.parse_args()
        eventID = args["eventID"]
        startTime = args["startTime"]
        endTime = args["endTime"]
        y_device = args["y_Device"]
        x_device = args["x_Device"]
        host_ip = args["host_ip"]
        database_name=args["database_name"]
        user = args["user"]
        password = args["password"]
        port = args["port"]
        sensor_kw =args["sensor_kw"]
        device_kw = args["device_kw"]
        eventTableName=args["eventTableName"]
        deviceLogTableName=args["deviceLogTableName"]
        connector=args["connector"]

        result = service.getAB_fromEventID(eventID=eventID, startTime=startTime, endTime=endTime,
                                            y_Device=y_device, x_Device=x_device,
                                            host_ip=host_ip, database_name= database_name,
                                            user=user, password=password, port=port,
                                            sensor_kw=sensor_kw, device_kw=device_kw,
                                            eventTableName=eventTableName, deviceLogTableName=deviceLogTableName,
                                            connector=connector)
        if(result):
            # Update result with startTime, endTime, y_device, x_device, eventID
            result.update({"eventID":eventID})
            result.update({"startTime":startTime})
            result.update({"endTime":endTime})
            result.update({"y_device":y_device})
            result.update({"x_device":x_device})

            return result, 200
        return "An Error has Occured", 404

device_parser = reqparse.RequestParser()
device_parser.add_argument('eventID', type=str, required=True)
device_parser.add_argument('startTime', type=str, required=True)
device_parser.add_argument('endTime', type=str, required=True)
device_parser.add_argument('host_ip',type=str, required=False, default=maria_address)
device_parser.add_argument('database_name',type=str, required=False, default=maria_database)
device_parser.add_argument('user',type=str, required=False, default=maria_user)
device_parser.add_argument('password', type=str, required=False, default=maria_password)
device_parser.add_argument('port', type=str, required=False, default=maria_port)
device_parser.add_argument('sensor_kw', type=str, required=False, default="sensors")
device_parser.add_argument('device_kw', type=str, required=False, default="devices")
device_parser.add_argument("eventTableName", type=str, required=False, default="Event")
device_parser.add_argument("deviceLogTableName", type=str, required=False, default="DeviceLog")
device_parser.add_argument("connector", type=str, required=False, default="mysql")

class Devices(Resource):
    def get(self):
        args = device_parser.parse_args()
        eventID = args["eventID"]
        startTime = args["startTime"]
        endTime = args["endTime"]
        host_ip = args["host_ip"]
        database_name=args["database_name"]
        user = args["user"]
        password = args["password"]
        port = args["port"]
        sensor_kw =args["sensor_kw"]
        device_kw = args["device_kw"]
        eventTableName=args["eventTableName"]
        deviceLogTableName=args["deviceLogTableName"]
        connector=args["connector"]

        result = service.getProductNames_fromEventID(eventID=eventID, startTime=startTime, endTime=endTime,
                                            host_ip=host_ip, database_name= database_name,
                                            user=user, password=password, port=port,
                                            sensor_kw=sensor_kw, device_kw=device_kw,
                                            eventTableName=eventTableName, deviceLogTableName=deviceLogTableName,
                                            connector=connector)
        if(result):
            # Update result with startTime, endTime, y_device, x_device, eventID
            result.update({"eventID":eventID})
            result.update({"startTime":startTime})
            result.update({"endTime":endTime})

            return result, 200
        return "An Error has Occured", 404

api.add_resource(Prediction, "/predict")
api.add_resource(Devices, "/devices")

@app.route('/')
def home():
    return render_template('home.html')

if __name__=='__main__':
    app.run(debug=True)
