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
                    print("Inserted " + str(numRows) + " Rows")
                    numRows = 0
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:

                try:
                    # Check for Kafka message
                    data = json.loads(msg.value())
                    
                    #Validating that trip_id exists and is an integer value
                    if (data['trip_id'] == ""):
                        print("No Trip ID - Dropping row")
                        continue
                    trip_id = int(data['trip_id'])

                    route_id = data['route_id']

                    #Validating service_key
                    if (data['service_key'] == "W"):
                        service_key = "Weekday"
                    elif (data['service_key'] == "S"):
                        service_key = "Saturday"
                    elif (data['service_key'] == "U"):
                        service_key = "Sunday"
                    else:
                        service_key = None

                    #Validating direction
                    if (data['direction'] == "0"):
                        direction = "Out"
                    elif (data['direction'] == "1"):
                        direction = "Back"
                    else:
                        direction = None

                    conn = dbconnect()
                    dbCursor = conn.cursor()
                    dbCursor.execute("""INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (trip_id) DO UPDATE SET (route_id, service_key, direction) = (EXCLUDED.route_id, EXCLUDED.service_key, EXCLUDED.direction);""", (trip_id, route_id, None, service_key, direction))
                    numRows = numRows + 1    
                except Exception as e:
                    print(e)
                    continue
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
