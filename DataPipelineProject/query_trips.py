#!/usr/bin/env python
import json
import psycopg2
import csv

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
    conn = dbconnect()
    dbCursor = conn.cursor()
    dbCursor.execute("select latitude, longitude, speed from breadcrumb join trip using(trip_id) where route_id is not null and vehicle_id = '2287';")
    records = dbCursor.fetchall()
    with open('records.tsv', 'wt') as out:
        tsv_writer = csv.writer(out, delimiter='\t')
        for record in records:
            tsv_writer.writerow(record)
