import mysql.connector
from mysql.connector import Error
import sqlalchemy as db
import pymysql
import pandas as pd
import re
import time

def isProduct(inputString):
    '''
    Check if the inputString start with prd_
    '''
    return True if len(re.findall(r'^prd_.*$',inputString)) else False

def getProductsName(inputDataframe):
    '''
    This function receives a dataframe, each column is a device id
    It will return a list of product name
    '''
    allNames = list(inputDataframe.columns)
    product_name = [name for name in allNames if isProduct(name)]
    return product_name

def buildQuery(tableName):
    return 'SELECT * from ' + tableName

def queryTable(tableName, host_ip='172.25.0.1', database_name='sipiot',
               user='sip',password='^62UaE{]a)VT3{sp', port=33060, connector='mysql'):

    if (connector=='mysql'):
        try:
            startTime = time.time()
            # Create a connection
            connection = mysql.connector.connect(host=host_ip,
                                                database=database_name,
                                                user=user,
                                                password=password,
                                                port=port)
            if connection.is_connected():
                db_Info = connection.get_server_info()
                print("Connected to MySQL Server version ", db_Info)

                # Create a cusor
                cursor = connection.cursor()
                cursor.execute("select database();")

                # You need to fetchone
                record = cursor.fetchone()
                print("You're connected to database: ", record)

                # Query
                object_df = pd.read_sql_query(buildQuery(tableName),connection)
                return object_df

        except Error as e:
            print("Error while connecting to MySQL", e)
            return None

        finally:
            if (connection.is_connected()):
                cursor.close()
                connection.close()
                print("MySQL connection is closed")
                print("Operating time: ", time.time() - startTime)

    elif (connector=="sqlalchemy"):
        try:
            startTime = time.time()

            db_string = 'mysql+pymysql://' + user + ':' + str(password) + '@' + str(host_ip) + ':' + str(port) + '/' + database_name

            print('Connecting at: ', db_string)

            engine = db.create_engine(db_string)
            connection = engine.connect()

            object_df = pd.read_sql_query(buildQuery(tableName), connection)
            return object_df

        except Error as e:
            print('Errpr while connecting to MySQL',e)
            return None
        finally:
            connection.close()
            print("MySQL connection is closed")
            print("Operating time: ", time.time() - startTime)
