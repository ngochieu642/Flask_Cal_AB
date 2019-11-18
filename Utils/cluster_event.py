import ast
import json
import math
import datetime
import functools
import numpy as np
import re
import pandas as pd

from sklearn.cluster import KMeans

# Get dictionary from dataframe
def get_DeviceLog(deviceID_list, log_df):
    '''
    This function receives device ID, log device
    Then it returns a dictionary {deviceID: log of deviceID(dataframe)}
    '''
    device_log_df = []
    for i in deviceID_list:
        device_log_df.append(log_df[log_df['device_id']==i])

    return dict(zip(deviceID_list, device_log_df))

def get_DeviceSensor_from_eventID(eventID, log_df, event_df, device_kw="devices", sensor_kw="formulaSensor"):
    '''
    This function receive eventId, Log Dataframe, Event Dataframe and keywords
    These keywords used to access the id of device in event_df

    It then returns 2 dictionary:
        + device: {deviceId: log of deviceId(dataframe)}
        + sensor: {sensorId: log of sensorId(dataframe)}
    '''
    specificEvent = event_df[event_df['id']==eventID]

    deviceList = ast.literal_eval(specificEvent[device_kw].to_list()[0])
    sensorList = ast.literal_eval(specificEvent[sensor_kw].to_list()[0]) if \
    type(specificEvent[sensor_kw].to_list()[0]) is str else []

    lightDict = get_DeviceLog(deviceList, log_df)
    sensorDict = get_DeviceLog(sensorList, log_df)

    return lightDict, sensorDict

# Enrich data
def enrich_data(device_dict, kind="light"):
    '''
    This function receives a dict of {keyword: dataframe}, then it will add 2 columns to dataframe
    1: time - TimeStamp
    2: time64 - int64
    '''
    for (key, dataframe) in device_dict.items():
        dataframe['time'] = pd.to_datetime(dataframe['created_log_at'])
        dataframe['time64'] = dataframe['time'].apply(time2Int)

        dataframe['kind'] = kind

# Conver json
def convertToValue(x, deviceType="light"):
    '''
    Receive a JSON-like string. Extract information from it.
    '''
    jsonObject = json.loads(x)

    if deviceType=="light":
        return jsonObject["dimmer"]
    elif deviceType=="sensor":
        return jsonObject["photo"]
    else:
        return None

def dict_convertJSON(device_dict):
    '''
    This function receives a dict of {keyword: dataframe}, then it will conver the data column to value we can use
    '''
    for (key, dataframe) in device_dict.items():
        device_kind = dataframe['kind'].to_list()[0]
        print('Device Type: ', device_kind)
        dataframe['value'] = dataframe['data'].apply(lambda x: convertToValue(x, deviceType=device_kind))

# Only keep the data we want
def simplify_df(dataframe):
    '''Only keep those that has action is updated, take time64, kind, value'''
    return dataframe[dataframe['action']=='updated'][['time64','kind','value']]

def simplify_dict(device_dict):
    '''
    This function receives a dict of {keyword: dataframe}, then it will simplify dataframes using simplify_df
    '''
    list_dataframe = []
    keys = []

    for(key, dataframe) in device_dict.items():
        list_dataframe.append(simplify_df(dataframe))
        keys.append(key)
    return dict(zip(keys, list_dataframe))

# Limit data in a time range
def fromString_toInt64(inputTimeString):
    '''
    Converts a string to int64
    '''
    timeStamp = pd.to_datetime(inputTimeString)
    time64 = np.datetime64(timeStamp)
    return (time64.astype('uint64') / 1e6).astype('uint32')

def getDataframe_inRange(dataframe, start_time64, end_time64):
    '''
    Get Dataframe that has time64 betwwen start_time64 and end_time64
    '''
    return dataframe.loc[(dataframe['time64'] >= start_time64) & (dataframe['time64'] <= end_time64)]

def getDict_inRange(device_dict, start_time64, end_time64):
    '''
    This function receives a dict of {keyword: dataframe}, then it will get dataframe that betwwen start_time64 and end_time64
    '''
    keys = []
    df_inRange = []

    for(key, dataframe) in device_dict.items():
        df_inRange.append(getDataframe_inRange(dataframe=dataframe, start_time64=start_time64, end_time64=end_time64))
        keys.append(key)

    return dict(zip(keys, df_inRange))

# Add ID
def add_dataframe_ID(dataframe, ID):
    '''Function to Add a column to dataframe'''
    dataframe['ID'] = ID
    return dataframe

def add_dict_ID(device_dict):
    keys = []
    list_df = []

    for (key, dataframe) in device_dict.items():
        keys.append(key)
        list_df.append(add_dataframe_ID(dataframe=dataframe, ID=key))

    return dict(zip(keys, list_df))

# Concat All dataframes
def getDataframe_fromDicts(listOfDict):
    '''
    This function receives a dict of {keyword: dataframe}, then it will concat dataframes into 1 dataframe
    This dataframe include these fields: time64 | kind | value | ID
    '''
    list_df = []
    countAppend = 1

    for device_dict in listOfDict:
        if len(device_dict)>0 :
            for (key, dataframe) in device_dict.items():
                list_df.append(dataframe)
                print('Append: ', countAppend)
                countAppend += 1

    return pd.concat(list_df,ignore_index=True) if len(list_df) > 0 else pd.DataFrame(columns=['time64','kind','value','ID'])

def getNumberOfCluster(listOfDict):
    '''
    This function receives a dict of {keyword: dataframe}, then it will get dataframe biggest size
    '''
    list_size = []

    for device_dict in listOfDict:
        for (key, value) in device_dict.items():
#             print('Size: ', value.shape[0])
            list_size.append(value.shape[0])
    return max(list_size)

# Cluster
def getListOf_time64(listOfNames):
    '''
    This functions get all the columns started with time64's name
    '''
    list_time64 =[]

    for eachName in listOfNames:
        if re.match('^time64', eachName):
            list_time64.append(eachName)
    return list_time64

# Each row is a dictionary
def getTime64(eachRow, listTime):
    '''
    This function return average of value from valus of a dict, if key is in listTime. If there
    were no values in eachRow, return None
    '''
    listTime64 = []

    for (key, value) in eachRow.items():
        if key in listTime and not math.isnan(value):
            listTime64.append(value)

    if(len(listTime64)==0):
        return None
    else:
        return int(np.average(listTime64))

def separateID_mergeLabel(event_df):
    '''
    This function return the clustered table based on event df
    '''
    uniqueIDs = event_df['ID'].unique()
    uniqueIDs_df_list = []

    # let how='outer' if you want to take NaN values
    for ID in uniqueIDs:
        uniqueIDs_df_list.append(event_df[event_df['ID']==ID][['time64','value','label']].\
                                 rename(columns={'value':ID}).\
                                 groupby(by='label').first().reset_index()) # Mean or first ??

    final_df = functools.reduce(lambda x,y: pd.merge(x, y, left_on='label', right_on='label', how='outer'), uniqueIDs_df_list)
    final_df = final_df.groupby(by=final_df.columns, axis=1).mean()

    # Get list of related time64 columns
    list_time64 = getListOf_time64(final_df.columns)
    final_df['time64'] = final_df.apply(lambda row: getTime64(row, list_time64), axis=1)
    final_df['date'] = pd.to_datetime(final_df['time64'],unit='s')

    return final_df.drop(columns=list_time64).sort_values(by='time64')

# TIME UTILS
def time2Int(timeStamp):
    '''
    This function convert TimeStamp to uint32
    '''
    time64 = np.datetime64(timeStamp)
    return (time64.astype('uint64') / 1e6).astype('uint32')

def int64_2_timeStamp(dataframe):
    '''
    This function add column date to dataframe - get from time64 column
    '''
    dataframe['date'] = pd.to_datetime(dataframe['time64'], unit='s')

# Wrapper
def getClusterTable_fromEventID(event_id,
                                startTime, endTime,
                                eventDF, devicelogDF,
                                sensor_kw="sensors", device_kw="devices"):
    '''
    This function receive envent_id, startTime, endTime, event dataframe, device log dataframe, keywords
    Then it will return cluster table based on input
    '''
    try:
        # Get Dict
        light_dict, sensor_dict = get_DeviceSensor_from_eventID(eventID=event_id,
                                                            log_df=devicelogDF,
                                                            event_df=eventDF,
                                                            sensor_kw=sensor_kw,
                                                            device_kw=device_kw)
        # Add time64 and Kind
        enrich_data(light_dict, kind="light")
        enrich_data(sensor_dict, kind="sensor")

        # Convert Data from JSON to format we can use. The kind help us to find the key we want.
        dict_convertJSON(light_dict)
        dict_convertJSON(sensor_dict)

        # Only Keep Data we need
        sensor_dict_simple = simplify_dict(sensor_dict)
        light_dict_simple = simplify_dict(light_dict)

        # Get Data in time range
        '''
        Format of startTime, endTime
        startTime = '2019-11-01 03:24:47'
        endTime = '2019-11-05 08:54:20'
        '''
        light_dict_simple_inRange = getDict_inRange(device_dict=light_dict_simple,
                                                start_time64=fromString_toInt64(startTime),
                                                end_time64=fromString_toInt64(endTime))

        sensor_dict_simple_inRange = getDict_inRange(device_dict=sensor_dict_simple,
                                                start_time64=fromString_toInt64(startTime),
                                                end_time64=fromString_toInt64(endTime))

        # Add ID of product
        light_ID_dict = add_dict_ID(light_dict_simple_inRange)
        sensor_ID_dict = add_dict_ID(sensor_dict_simple_inRange)

        # Concat to a single dataframe, we will do cluster on this
        all_df = getDataframe_fromDicts([light_ID_dict, sensor_ID_dict])

        if (all_df.shape[0] > 0):

            # Cluster using kmean on time64
            noOfCluster = getNumberOfCluster([light_ID_dict, sensor_ID_dict])
            print('Number of cluster: ', noOfCluster)

            km = KMeans(
                n_clusters=noOfCluster, init='random',
                n_init=10, max_iter=2000,
                tol=1e-04, random_state=0
            )

            all_df['label'] = km.fit_predict(all_df[['time64']])

            # Split to each devices table and merge them by label, sort by time64 and get TimeStamp for date64
            clustered_df = separateID_mergeLabel(all_df)

            return clustered_df.drop(columns=['label'])
        else:
            return pd.DataFrame(columns=['time64','date'])
    except:
        print('ERROR! Returns the dafault skeleton')
        return pd.DataFrame(columns=['time64','date'])

def getCleanDatafranme(inputDataframe):
    return inputDataframe.dropna()
