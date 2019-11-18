import pandas as pd
from . import cluster_event
from . import database
from . import calculate
import sys
import ast

def getAB_fromEventID(eventID, startTime, endTime,
                        y_Device, x_Device,
                        host_ip="172.25.0.1", database_name="sipiot",
                        user="sip", password="^62UaE{]a)VT3{sp", port=33060,
                        sensor_kw="sensors", device_kw="devices",
                        eventTableName = "Event", deviceLogTableName = "DeviceLog", connector="mysql"):

    try:
        event_df = database.queryTable(tableName = eventTableName,
                                        host_ip=host_ip, database_name=database_name,
                                        user=user, password=password, port=port,
                                        connector=connector)

        deviceLog_df = database.queryTable(tableName = deviceLogTableName,
                                        host_ip=host_ip, database_name=database_name,
                                        user=user, password=password, port=port,
                                        connector=connector)

        report_df = cluster_event.getClusterTable_fromEventID(event_id=eventID,
                                                                startTime=startTime, endTime=endTime,
                                                                eventDF=event_df, devicelogDF=deviceLog_df,
                                                                sensor_kw=sensor_kw, device_kw=device_kw)

        coef_A, coef_B, pearson_coef, train_err, test_err = calculate.calculate_degree_1(inputDataframe=report_df, y_name=y_Device, x_name=x_Device, show_report=True)

        resultDict = {
            "pearson": pearson_coef,
            "train_error": train_err,
            "test_error": test_err,
            "coef_A": coef_A,
            "coef_B": coef_B
        }

    except IOError as e:
        errno, strerror = e.args
        print("I/O error({0}): {1}".format(errno,strerror))

        resultDict = {
            "errno" : errno,
            "error" : strerr
        }

    except ValueError as e:
        errno, strerror = e.args
        print("I/O error({0}): {1}".format(errno,strerror))

        resultDict = {
            "errno" : errno,
            "error" : strerr
        }

    except:
        print("Unexpected error:", sys.exc_info()[0])
        resultDict = {
            "error":"Unexpected Error"
        }

    finally:
        return resultDict

def getProductNames_fromEventID(eventID,
                        host_ip="172.25.0.1", database_name="sipiot",
                        user="sip", password="^62UaE{]a)VT3{sp", port=33060,
                        eventTableName = "Event", connector="mysql"):

    try:
        event_df = database.queryTable(tableName = eventTableName, connector=connector)
        event_df = event_df[event_df['id']==eventID]

        device_0 = event_df["devices"].values[0]
        device_0 = ast.literal_eval(device_0) if (device_0) else []

        device_1 = event_df["sensors"].values[0]
        device_1 = ast.literal_eval(device_1) if (device_1) else []

        device_2 = event_df["formulaSensor"].values[0]
        device_2 = ast.literal_eval(device_2) if (device_2) else []

        listDevices = device_0 + device_1 + device_2

        resultDict = {
            "devices": listDevices
        }

    except IOError as e:
        errno, strerror = e.args
        print("getProductNames_fromEventID I/O error({0}): {1}".format(errno,strerror))

        resultDict = {
            "errno" : errno,
            "error" : strerr
        }

    except ValueError as e:
        errno, strerror = e.args
        print("getProductNames_fromEventID I/O error({0}): {1}".format(errno,strerror))

        resultDict = {
            "errno" : errno,
            "error" : strerr
        }

    except:
        print("getProductNames_fromEventID Unexpected error:", sys.exc_info()[0])
        resultDict = {
            "error":"Unexpected Error"
        }

    finally:
        return resultDict
