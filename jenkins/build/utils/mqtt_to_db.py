import json
import logging
import random
import time
import utils.constant as constant
import pandas as pd
import os
import sys
import utils.alert as alert
import pymssql

from paho.mqtt import client as mqtt_client
from datetime import datetime,date, timedelta
from sqlalchemy import create_engine,text,engine
from datetime import datetime




class PREPARE:


    def __init__(self,server,database,user_login,password,table,table_columns,table_log,table_columns_log,mqtt_topic,mqtt_broker,slack_notify_token):
        self.server = server
        self.database = database
        self.user_login = user_login
        self.password = password
        self.table_log = table_log
        self.table = table
        self.table_columns = table_columns
        self.table_columns_log = table_columns_log
        self.slack_notify_token = slack_notify_token
        self.mqtt_topic = mqtt_topic
        self.mqtt_broker = mqtt_broker
        self.data_nows = None
        self.data_prvs = None
        self.df_insert = pd.DataFrame({'' : []})

    def stamp_time(self):
        now = datetime.now()
        print("\nHi this is job run at -- %s"%(now.strftime("%Y-%m-%d %H:%M:%S")))

    def check_table(self):
        #connect to db
        cnxn,cursor=self.conn_sql()
        # create table
        try:
            cursor.execute('''
            CREATE TABLE '''+self.table+''' (
                '''+self.table_columns+'''
                )
                ''')
            cnxn.commit()
            cursor.close()
            self.info_msg(self.check_table.__name__,f"create a {self.table_log} table successfully" )
        except Exception as e:
            if 'There is already an object named' in str(e):
                self.info_msg(self.check_table.__name__,f"found a {self.table} table" )
            elif 'Column, parameter, or variable' in str(e):
                self.error_msg(self.check_table.__name__,"define columns mistake" ,e)
            else:
                self.error_msg(self.check_table.__name__,"unknow cannot create table" ,e)

    def check_table_log(self):
        #connect to db
        cnxn,cursor=self.conn_sql()
        # create table
        try:
            cursor.execute('''
            CREATE TABLE '''+self.table_log+''' (
                '''+self.table_columns_log+'''
                )
                ''')
            cnxn.commit()
            cursor.close()
            self.info_msg(self.check_table_log.__name__,f"create a {self.table_log} table successfully" )
        except Exception as e:
            if 'There is already an object named' in str(e):
                self.info_msg(self.check_table_log.__name__,f"found a {self.table} table" )
            elif 'Column, parameter, or variable' in str(e):
                self.error_msg(self.check_table_log.__name__,"define columns log mistake" ,e)
            else:
                self.error_msg(self.check_table_log.__name__,"unknow cannot create table log" ,e)

    def error_msg(self,process,msg,e):
        result = {"status":constant.STATUS_ERROR,"process":process,"message":msg,"error":e}
    
        try:
            self.alert_slack(self.alert_error_msg(result))
            self.log_to_db(result)
            #sys.exit()
        except Exception as e:
            self.info_msg(self.error_msg.__name__,e)
            #sys.exit()

    def alert_slack(self,msg):
        value= alert.slack_notify(self.slack_notify_token,msg)
        if value == constant.STATUS_OK:
            self.info_msg(self.alert_slack.__name__,'send msg to slack notify')
        else:
            self.info_msg(self.alert_slack.__name__,value)

    def alert_error_msg(self,result):
        if self.slack_notify_token != None:
            return f'\nproject: {self.table}\nprocess: {result["process"]}\nmessage: {result["message"]}\nerror: {result["error"]}\n'
                
    def info_msg(self,process,msg):
        result = {"status":constant.STATUS_INFO,"process":process,"message":msg,"error":"-"}
        print(result)

    def ok_msg(self,process):
        result = {"status":constant.STATUS_OK,"process":process,"message":"program running done","error":"-"}
        try:
            self.log_to_db(result)
            print(result)
        except Exception as e:
            self.error_msg(self.ok_msg.__name__,'cannot ok msg to log',e)
    
    def conn_sql(self):
        #connect to db
        try:
            cnxn = pymssql.connect(self.server, self.user_login, self.password, self.database)
            cursor = cnxn.cursor()
            return cnxn,cursor
        except Exception as e:
            self.alert_slack("Danger! cannot connect sql server")
            self.info_msg(self.conn_sql.__name__,e)
            sys.exit()

    def log_to_db(self,result):
        #connect to db
        cnxn,cursor=self.conn_sql()
        try:
            result_error = str(result["error"]).replace("'",'"')
            print(result_error)
            cursor.execute(f"""
                INSERT INTO [{self.database}].[dbo].[{self.table_log}] 
                values(
                    getdate(), 
                    '{result["status"]}', 
                    '{result["process"]}', 
                    '{result["message"]}', 
                    '{str(result["error"]).replace("'",'"')}'
                    )
                    """
                )
            cnxn.commit()
            cursor.close()
        except Exception as e:
            self.alert_slack("Danger! cannot insert log table")
            self.info_msg(self.log_to_db.__name__,e)
            #sys.exit()

class MQTT_TO_DB(PREPARE):
    def __init__(self,server,database,user_login,password,table,table_columns,table_log,table_columns_log,mqtt_topic,mqtt_broker,slack_notify_token=None):
        super().__init__(server,database,user_login,password,table,table_columns,table_log,table_columns_log,mqtt_topic,mqtt_broker,slack_notify_token)        

    def on_connect(self,client, userdata, flags, rc):
        if rc == 0 and client.is_connected():
            print("Connected to MQTT Broker!")
            client.subscribe(self.mqtt_topic)
        else:
            print(f'Failed to connect, return code {rc}')

    def on_disconnect(self,client, userdata, rc):
        FIRST_RECONNECT_DELAY = 1
        RECONNECT_RATE = 2
        MAX_RECONNECT_COUNT = 12
        MAX_RECONNECT_DELAY = 60
        FLAG_EXIT = False

        logging.info("Disconnected with result code: %s", rc)
        reconnect_count, reconnect_delay = 0, FIRST_RECONNECT_DELAY
        while reconnect_count < MAX_RECONNECT_COUNT:
            logging.info("Reconnecting in %d seconds...", reconnect_delay)
            time.sleep(reconnect_delay)

            try:
                client.reconnect()
                logging.info("Reconnected successfully!")
                return
            except Exception as err:
                logging.error("%s. Reconnect failed. Retrying...", err)

            reconnect_delay *= RECONNECT_RATE
            reconnect_delay = min(reconnect_delay, MAX_RECONNECT_DELAY)
            reconnect_count += 1
        logging.info("Reconnect failed after %s attempts. Exiting...", reconnect_count)
        #global FLAG_EXIT
        FLAG_EXIT = True

    def df_to_db(self):
        #connect to db
        cnxn,cursor=self.conn_sql()
        try:
            df = self.df_insert
            for index, row in df.iterrows():
                cursor.execute(f"""
                INSERT INTO [{self.database}].[dbo].[{self.table}] 
                values(
                    getdate(), 
                    '{row.my_str}',
                    '{row.temp2}',
                    '{row.tmp1}',
                    '{row.mc_no}',
                    '{row.process}'
                    )
                    """
                )

            cnxn.commit()
            cursor.close()
            self.df_insert = pd.DataFrame({'' : []})  
            self.info_msg(self.df_to_db.__name__,f"insert data successfully")        
        except Exception as e:
            self.error_msg(self.df_to_db.__name__,"cannot insert df to sql",e)

    def check_change_bite(self):
        if self.data_prvs != None:
            if int(self.data_nows["temp2"]) < int(self.data_prvs["temp2"]):
                df_insert = [self.data_prvs]
                self.df_insert = pd.DataFrame.from_dict(df_insert)
        self.data_prvs = self.data_nows
            
    def on_message(self,client, userdata, msg):
        #print(f'Received `{msg.payload.decode()}` from `{msg.topic}` topic')
        datas = msg.payload.decode()
        datas = json.loads(datas)
        mc_no = msg.topic.split("/")[-1]
        process = msg.topic.split("/")[-2]
        datas["mc_no"] = mc_no
        datas["process"] = process
        self.data_nows = datas
        self.check_change_bite()
        if not self.df_insert.empty:
            self.df_to_db()
            #print(self.df_insert)

    def connect_mqtt(self):
        BROKER = self.mqtt_broker
        PORT = 1883
        CLIENT_ID = f'python-mqtt-tcp-pub-sub-{random.randint(0, 10000)}'

        client = mqtt_client.Client(CLIENT_ID)
        #client.username_pw_set(USERNAME, PASSWORD)
        client.on_connect = self.on_connect
        client.on_message = self.on_message
        client.connect(BROKER, PORT, keepalive=3)
        client.on_disconnect = self.on_disconnect
        return client

    def run(self):
        try:
            logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s',
                                level=logging.DEBUG)
            self.stamp_time()
            self.check_table()
            self.check_table_log()
            client = self.connect_mqtt()
            client.loop_forever()
        except Exception as e:
            self.error_msg(self.run.__name__,"mqtt subscription crash",e)