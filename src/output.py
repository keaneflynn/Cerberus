import os
import json
import psycopg
from uuid import uuid4
from datetime import datetime


class fileOut:
    def __init__(self, output_type):
        self.datetime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        if output_type == 'postgres':   
            pass
        else:
            self.output_directory = os.path.abspath('./outfiles/')
            os.makedirs(self.output_directory, exist_ok=True)

    def jsonOutput(self, network_status_dictionary):
        filename = 'network_status' + '_' + self.datetime + '.json'
        filepath = os.path.join(self.output_directory, filename)
        with open("{}".format(filepath), 'w') as f:
            json.dump(network_status_dictionary, f)

    def postgresAppend(self, network_status_dictionary):
        # Create connection tunnel to postgres database
        with psycopg.connect(
                dbname=<DBNAME>, 
                user=<USERNAME>,
                password=<PASSWORD>,
                host=<HOST>,
                port='5432') as conn:

            # Open a cursor to perform database operations
            with conn.cursor() as cur:
                
                for client in network_status_dictionary:
                    # Pass data to fill a query placeholders and let Psycopg perform
                    # the correct conversion (no SQL injections!)
                    cur.execute(
                        """INSERT INTO network_connections
                        (uuid, datetime, ip_address, client, latitude, longitude,
                        packet_loss, min_ping, mean_ping, max_ping) VALUES 
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (uuid4(), client['datetime'], client['ip_address'],
                         client['client'], client['latitude'], client['longitude'],
                         client['packet_loss'], client['min_ping'], 
                         client['mean_ping'], client['max_ping']))

                    # Make the changes to the database persistent
                    conn.commit()

