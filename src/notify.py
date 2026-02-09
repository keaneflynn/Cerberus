import os
import numpy as np
import warnings
from pickle import load, dump
from psycopg import connect
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime, timezone
from dateutil import tz
from psycopg.errors import OperationalError
from urllib.error import URLError
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Remove pandas warnings that are not relevant to code stack
warnings.filterwarnings("ignore", category=UserWarning)
pd.options.mode.chained_assignment = None 

class slack:
    def __init__(self):
        # will migrate to .env soon
        slack_web_token = <API_KEY>
        self.slack_channel = <CHANNEL_NAME>
        self.slack_channel_id = <CHANNEL_ID> # uploading files uses channel ID
        self.bot_username = 'Cerberus'
        self.slack_client = WebClient(token = slack_web_token)
        self.dbname = '<DBNAME>'
        self.username = '<USERNAME>'
        self.password = '<PASSWORD>'
        self.host = '<HOST>'
        self.port = '<PORT>'

    def queryData(self):
        try:
            conn = connect(dbname = self.dbname,
                           user = self.username,
                           password = self.password,
                           host = self.host,
                           port = self.port)
            df = pd.read_sql_query('''SELECT datetime, ip_address, client, 
                                   latitude, longitude, packet_loss, mean_ping
                                   FROM network_connections WHERE 
                                   datetime >= NOW() - INTERVAL '7 days';''',
                                   conn)                
        except OperationalError:
            conn = None
            raise OperationalError('Error: could not connect to database')
        finally:
            if conn:
                conn.close()        
        return df
    
    def statusReport(self, network_status_file, sql_df):
        file_path = os.path.abspath(network_status_file)
        try:
            with open(file_path, 'rb') as file:
                weekly_report_time = load(file)
        except EOFError:
            # Initial startup will generate current time
            # for pickle file
            with open(file_path, 'wb') as file:
                dump(datetime.now(), file)
            return None
        except FileNotFoundError:
            with open(file_path, 'wb') as file:
                dump(datetime.now(), file)
            print('Error: network status pickle file not found. New one created')
            return None
        except Exception as e:
            print('Error: {}'.format(e))
        time_delta = datetime.now() - weekly_report_time
        if time_delta.days >= 7:
            start_date = str(sql_df['datetime'][0].astimezone(tz.tzlocal()))[:-6]
            end_date = str(sql_df['datetime'].iloc[-1].astimezone(tz.tzlocal()))[:-6]
            sql_df.boxplot(column = 'mean_ping', by = 'ip_address', showfliers = False)
            plt.title('Weekly SWEON Status report from \n{} to {}'.format(start_date, end_date))
            plt.suptitle('')
            plt.xticks(rotation = 75, ha = 'center')
            plt.xlabel('IP Address')
            plt.ylabel('Mean Ping Time (ms)')
            plt.tight_layout()
            plt.savefig('src/temp/weeklyPlot.png')
            with open(file_path, 'wb') as file:
                dump(datetime.now(), file)
            try:  
                self.slack_client.files_upload_v2(channel = self.slack_channel_id,
                                                  file = 'src/temp/weeklyPlot.png',
                                                  initial_comment = 'Weekly SWEON Status Report')
            except SlackApiError:
                print('Slack API connection unsuccessful, check credentials')
        # Perhaps add a message with client uptimes as well?
        # We'll see how the graphs work first then come back to this
                
    def networkDisconnection(self, connection_df, backhaul_pickle):
        # Notifies and records status of entire network disconnection
        # as a result of the network backhaul being disconnected.
        # Will also notify users of a reconnection of the network backhaul.
        try:
            with open(os.path.join(backhaul_pickle), 'rb') as file:
                if load(file) == 'online': 
                    time_val = 24
                    time_delta = (datetime.now(timezone.utc) - 
                                  pd.Timedelta(hours = time_val, minutes = 15))
                    df = connection_df[connection_df['datetime'] > time_delta]
                    df['online'] = np.where(df['packet_loss'] >= 90, 0, 1)
                    if df['online'].sum() > 0: 
                        # CURRENTLY ONLINE, PREVIOUSLY ONLINE
                        status = 'online'
                    else: 
                        # CURRENLY OFFLINE, PREVIOUSLY ONLINE
                        status = 'offline'
                        with open(os.path.join(backhaul_pickle), 'wb') as file:
                            dump(status, file)
                        message = '''\
Network backhaul has been disconnected for {} hours. If issue persists during \
periods of direct sunlight, contact the UNR seismology lab to troubleshoot \
the connection issue. If it's winter, try waiting it out for a bit. \
'''.format(time_val)
                        try:
                            self.slack_client.chat_postMessage(channel = self.slack_channel, 
                                                               text = message,
                                                               username = self.bot_username)
                        except SlackApiError:
                            print('Slack API connection unsuccessful, check credentials')
                        except Exception as e:
                            raise(e)
                else: 
                    time_val = 10 # Should grab everything from most
                                  # recent burst of ping messages
                    time_delta = (datetime.now(timezone.utc) - 
                                  pd.Timedelta(minutes = time_val))
                    df = connection_df[connection_df['datetime'] > time_delta]
                    df['online'] = np.where(df['packet_loss'] >= 90, 0, 1)
                    if sum(df['online'] > 0): 
                        # PREVIOUSLY OFFLINE, BACK ONLINE                        
                        status = 'online'
                        message = '''\
Network backhaul connection has been reestablished. Network clients can now \
communicate with primary compute & database server. Advise monitor connection \
conditions to ensure there are no issues with network tower power supply or \
point-to-point antenna alignment.'''
                        with open(os.path.join(backhaul_pickle), 'wb') as file:
                            dump(status, file)
                        try:
                            self.slack_client.chat_postMessage(channel = self.slack_channel, 
                                                               text = message,
                                                               username = self.bot_username)
                        except SlackApiError:
                            print('Slack API connection unsuccessful, check credentials')
                    else: 
                        # PREVIOUSLY OFFLINE, STILL OFFLINE
                        status = 'offline'               
        except FileNotFoundError:
            # Create new pickle file when one does not exist
            with open(os.path.join(backhaul_pickle), 'wb') as file:
                dump('online', file)
                status = None
        except EOFError:
            # Populate new pickle file when one does not exist
            # Mostly to catch odd errors with pickle file
            with open(os.path.join(backhaul_pickle), 'wb') as file:
                dump('online', file)
                status = None
        return status
                
    def clientDisconnection(self, sql_df, disconnect_file):
        # Notifies and records disconnection of individual network clients
        # after a specified amount of time of disconnection.
        notify_list = []
        offline_time_delta = (datetime.now(timezone.utc) - 
                              pd.Timedelta(hours = 2, minutes = 15))
        # Consider anything GTOET 90% packet loss offline
        # and create corresponding dataframe
        offline_recent_df = sql_df[sql_df['datetime'] > offline_time_delta]
        offline_df = offline_recent_df[offline_recent_df['packet_loss'] >= 90] 
        offline_df['offline_count'] = offline_df.groupby('ip_address')['ip_address'].transform('count')
        offline_client_status = offline_df.drop_duplicates(subset = ['ip_address'])
        # Create dataframe for slients that recently came back online
        online_time_delta = (datetime.now(timezone.utc) - 
                             pd.Timedelta(minutes = 10))
        online_recent_df = offline_recent_df[offline_recent_df['datetime'] > online_time_delta]
        online_df = online_recent_df[online_recent_df['packet_loss'] <= 10] 
        online_client_status = online_df.drop_duplicates(subset = ['ip_address']) 
        try:
            # Normal operating conditions with previous statuses
            # saved to pickle file
            with open(disconnect_file, 'rb') as file:
                disconn_ip = load(file)
        except FileNotFoundError:
            # Condition likely found on first runtime with 
            # no pickle file present. Will kill the program.
            with open(disconnect_file, 'wb') as file:
                dump(None, file)
                disconn_ip = None
            return None
        except EOFError:
            # Catch condition where pickle file gets corrupted
            # Will terminate the program
            with open(disconnect_file, 'wb') as file:
                dump(None, file)
            disconn_ip = None
            return None
        try:
            for _, client in offline_client_status.iterrows():
                # Loops through clients when they are considered 'offline'
                client_list = []
                # Tweak number 7 below for amount of time for disconnect message
                # Use (hours * 4) - 1 for whatever time threshold desired
                if client['offline_count'] >= 7:
                    client_list.extend([client['ip_address'], client['client'],
                                        client['latitude'], client['longitude'],
                                        str(client['datetime'].astimezone(tz.tzlocal()))[:-6]])
                    notify_list.append(client_list)
            ip_addresses = [int(i[0].split('.')[-1]) for i in notify_list]
            radio_exclude = None
            for i in range(len(ip_addresses)):
                if ip_addresses[i] in disconn_ip:
                    # If there has been a previous notification
                    # it will be ignored this time
                    continue
                message = '''\
ALERT: SWEON network client disconnected. Issue with client equipment or ethernet \
cable connection. Recommended to bring voltmeter, field laptop, ethernet cable,\
rj45 connectors, and ethernet crimping tool to site.
Client name: {}
IP address: {}
Latitude: {}
Longitude: {}
Datetime of disconnection: {} \
'''.format(notify_list[i][1], notify_list[i][0], notify_list[i][2], 
           notify_list[i][3], notify_list[i][4])  
                for j in range(len(ip_addresses)):
                    if i != j and ip_addresses[j] - ip_addresses[i] == 100:
                        radio_exclude = ip_addresses[j]
                        message = '''\
ALERT: SWEON network client AND corresponding radio disconnected. Likely caused \
by solar outage or other electrical issue. If the client does not reconnect \
automatically in the next couple of days, visit the site to troubleshoot. \
Verify that there are no critical electrical malfunctions. Review the following \
information and proceed accordingly:
Client name: {}
IP address: {}
Latitude: {}
Longitude: {}
Datetime of disconnection: {} \
'''.format(notify_list[i][1], notify_list[i][0], notify_list[i][2], 
           notify_list[i][3], notify_list[i][4])
                if ip_addresses[i] == radio_exclude:
                    pass
                else:
                    try:
                        self.slack_client.chat_postMessage(channel = self.slack_channel, 
                                                           text = message,
                                                           username = self.bot_username)
                    except SlackApiError as e:
                        print('Slack API connection unsuccessful, check credentials')
                    except URLError as e:
                        print('HTTP connection failed, likely issue with internet connection')
            with open(disconnect_file, 'wb') as file:
                dump(ip_addresses, file)
        except TypeError:
            # No clients are currently considered 'offline'
            if disconn_ip is not None:
                # Clients were previosuly 'offline'
                notify_list = []
                for _, client in online_client_status.iterrows():
                    # For each row, send out message that client has come back online
                    message = '''\
Network client has reconnected to the SWEON network. Continue to monitor \
client connection and electrical health to ensure continuous functionality.
Client name: {}
IP address: {}
Latitude: {}
Longitude: {}
Datetime of reconnection: {} \
'''.format(client['client'], client['ip_address'], 
           client['Latitude'], client['Longitude'], 
           str(client['datetime'].astimezone(tz.tzlocal()))[:-6])
                    try:
                        self.slack_client.chat_postMessage(channel = self.slack_channel, 
                                                           text = message,
                                                           username = self.bot_username)
                    except SlackApiError:
                        print('Slack API connection unsuccessful, check credentials')      
            else:
                # No clients previously 'offline'
                print('No disconnected clients: {}'.format(str(datetime.now())))
                return None          
        except Exception:
            raise
    
