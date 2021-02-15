#!/usr/bin/env python
from confluent_kafka import Consumer
import json
import ccloud_lib
import re
import psycopg2

DBname = "breadcrumbdatabase"
DBuser = "fot"
DBpwd = "hello"

def dbconnect():
	connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
	)
	connection.autocommit = True
	return connection


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'python_example_group_1',
        'auto.offset.reset': 'earliest',
    })

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    total_count = 0
    previous_velocity = 0
    previous_direction = 0
    numRows = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                if (numRows != 0):
                    print("Inserted " + numRows + " Rows")
                    numRows = 0
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:

                try:
                    # Check for Kafka message
                    data = json.loads(msg.value())

                    #Validate that EVENT_NO_TRIP exists and is an integer
                    if (data['EVENT_NO_TRIP'] == ""):
                        print("No Trip ID - Dropping row")
                        continue
                    trip_id = int(data['EVENT_NO_TRIP'])

                    #Validate that VEHICLE_ID exists and is an integer
                    if (data['VEHICLE_ID'] == ""):
                        print("No Vehicle ID - Dropping row")
                        continue
                    vehicle_id = int(data['VEHICLE_ID'])

                    #Validate that tstamp is of the form DD-MMM-YY
                    date_pattern = re.compile("^[01][0-9]-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-\d{2}$")
                    if (date_pattern.match(data['OPD_DATE']) is None):
                        print("Date does not match DD-MMM-YY format - Dropping row")
                        continue
                    tstamp = data['OPD_DATE']

                    #Validate that latitude exists and is between 45 and 46
                    if (data['GPS_LATITUDE'] == ""):
                        print("No latitude data - Dropping row")
                        continue

                    latitude = float(data['GPS_LATITUDE'])
                    if (latitude < 45 or latitude > 46):
                        print("Latitude out of bounds for Clark county area - Dropping row")
                        continue
                    
                    #Validate that longitude exists and is between 45 and 46
                    if (data['GPS_LONGITUDE'] == ""):
                        print("No longitude data - Dropping row")
                        continue

                    longitude = float(data['GPS_LONGITUDE'])
                    if (longitude < -123 or longitude > -122):
                        print("Longitude out of bounds for Clark county area - Dropping row")
                        continue
                    
                    #Validate that velocity exists and that it is an integer. Convert to mph
                    if (data['VELOCITY'] == ""):
                        print("No velocity value found - setting to previous record's velocity")
                        velocity = previous_velocity
                    else:
                        velocity = data['VELOCITY']
                    previous_velocity = velocity
                    speed = float(velocity) * 2.236936

                    #Validate that direction exists and that it is an integer between 0 and 359
                    #Set to previous direction if not found or if out of bounds.
                    if (data['DIRECTION'] == "" or int(data['DIRECTION']) < 0 or int(data['DIRECTION']) > 359):
                        print("Out of bounds direction value found - setting to previous record's direction")
                        direction = previous_direction
                    else:
                        direction = data['DIRECTION']
                    previous_direction = direction

                    conn = dbconnect()
                    dbCursor = conn.cursor()
                    dbCursor.execute("""INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;""", (trip_id, None, vehicle_id, None, None))
                    dbCursor.execute("""INSERT INTO breadcrumb (tstamp, latitude, longitude, direction, speed, trip_id) VALUES (%s, %s, %s, %s, %s, %s);""", (tstamp, latitude, longitude, direction, speed, trip_id)) 
                    numRows = numRows + 1    
                except:
                    print("Record badly formed - dropping row")
                    continue
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
