from httplib import BAD_REQUEST

from flask.json import jsonify
from flask_restful import reqparse, abort
from flask import Flask, request
from utils import init_db, get_db_cursor, get_where_statement
from schemas import CreateDeviceReading
import json
import os

import time


app = Flask(__name__)
app.config['TESTING'] = os.environ.get('FLASK_ENV') == 'testing'
init_db(app)


@app.route('/devices/<string:device_uuid>/readings/', methods = ['POST'])
def request_device_readings_post(device_uuid):
    # Grab the post parameters
    create_device_reading_schema = CreateDeviceReading()
    unmarshal_result = create_device_reading_schema.load(request.data)
    if unmarshal_result.errors:
        abort(BAD_REQUEST, message=str(unmarshal_result.errors))
    post_data = unmarshal_result.data
    sensor_type = post_data.get('type')
    value = post_data.get('value')
    date_created = post_data.get('date_created', int(time.time()))
    # Insert data into db
    cur, conn = get_db_cursor(app)
    cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                (device_uuid, sensor_type, value, date_created))

    conn.commit()
    conn.close()
    # Return success
    return 'success', 201

@app.route('/devices/<string:device_uuid>/readings/', methods = ['GET'])
def request_device_readings_get(device_uuid):
    """
    This endpoint allows clients to POST or GET data specific sensor types.

    POST Parameters:
    * type -> The type of sensor (temperature or humidity)
    * value -> The integer value of the sensor reading
    * date_created -> The epoch date of the sensor reading.
        If none provided, we set to now.

    Optional Query Parameters:
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    * type -> The type of sensor value a client is looking for
    """
    # Execute the query
    where_statement = get_where_statement(device_uuid, request.args)
    cur, conn = get_db_cursor(app)
    cur.execute('select * from readings where {}'.format(where_statement))
    rows = cur.fetchall()
    # Return the JSON
    conn.close()
    return jsonify([dict(zip(['device_uuid', 'type', 'value', 'date_created'], row)) for row in rows]), 200


@app.route('/devices/<string:device_uuid>/readings/quartiles/', methods = ['GET'])
def request_device_readings_quartiles(device_uuid):
    """
    This endpoint allows clients to GET the 1st and 3rd quartile
    sensor reading value for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    # TODO: implement a better database to be able to calculate quartiles properly
    #
    """
    where_statement = get_where_statement(device_uuid, request.args)
    where_statement = where_statement if where_statement else 'where 1'

    cur, conn = get_db_cursor(app)
    query = '''
    select value, NTILE(4) OVER(ORDER BY value) as quartile from readings where {}
    '''.format(where_statement)
    cur.execute(query)
    rows = cur.fetchall()
    data = {}
    for row in rows:
        if row['quartile'] == 1:
            data['quartile_1'] = row['value']
        if row['quartile'] == 3:
            data['quartile_3'] = row['value']
    conn.close()
    return jsonify(data), 200


@app.route('/devices/<string:device_uuid>/readings/<string:metric>/', methods = ['GET'])
def request_device_readings_min(device_uuid, metric):
    """
    This endpoint allows clients to GET the max sensor reading for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for

    Optional Query Parameters
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    if metric not in ['min', 'max', 'mean', 'median', 'mode']:
        return 'Invalid value for metric', 404

    where_statement = get_where_statement(device_uuid, request.args)

    cur, conn = get_db_cursor(app)
    if metric == 'median':
        query = '''
        SELECT AVG(value) as total
            FROM (
            SELECT value
                  FROM readings where {}
                  ORDER BY value
                  LIMIT 2 - (SELECT COUNT(*) FROM readings where {}) % 2    -- odd 1, even 2
                  OFFSET (SELECT (COUNT(*) - 1) / 2 FROM readings where {}))
        '''.format(where_statement, where_statement, where_statement)
        cur.execute(query)
        row = cur.fetchone()
        total = row['total']
        response = jsonify(dict(value=total)), 200
    elif metric == 'mode':
        cur.execute('select `value`, count(*) as n from readings where {} group by value order by 2 DESC limit 1'.format(where_statement))
        row = cur.fetchone()
        total = row['value']
        response = jsonify(dict(value=total)), 200
    else:
        if metric == 'mean':
            metric = 'avg'
        cur.execute('select {}(value) as value from readings where {}'.format(metric, where_statement))
        row = cur.fetchone()
        # Return the JSON
        response = jsonify(dict(zip(['value'], row))), 200

    conn.close()
    return response


@app.route('/readings/summary/', methods = ['GET'])
def request_readings_summary():
    """
    This endpoint allows clients to GET a full summary
    of all sensor data in the database per device.

    Optional Query Parameters
    * type -> The type of sensor value a client is looking for
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    where_statement = get_where_statement(args=request.args)
    where_statement = where_statement if where_statement else '1=1'
    cur, conn = get_db_cursor(app)
    query = '''
    select 
        device_uuid,
        min(value) as `min`,
        max(value) as `max`,
        avg(value) as `mean`,
        (
        select value from (select value, ntile(4) over (order by value) as q from readings where {} and device_uuid=main_readings.device_uuid) as q1 where q = 1
        ) as quartile_1,
        (
        select value from (select value, ntile(4) over (order by value) as q from readings where {} and device_uuid=main_readings.device_uuid) as q1 where q = 3
        ) as quartile_3,  
        (
        SELECT AVG(value) 
        FROM (
        SELECT value
              FROM readings where {} and device_uuid=main_readings.device_uuid
              ORDER BY value
              LIMIT 2 - (SELECT COUNT(*) FROM readings where {}) % 2    -- odd 1, even 2
              OFFSET (SELECT (COUNT(*) - 1) / 2 FROM readings where {}))
        ) as median,
        (
            select value from (select `value`, count(*) as n from readings where {} and device_uuid=main_readings.device_uuid group by value order by 2 DESC limit 1)
        ) as `mode`       
    from readings as main_readings where {} group by device_uuid
    '''.format(
        where_statement,
        where_statement,
        where_statement,
        where_statement,
        where_statement,
        where_statement,
        where_statement
    )
    cur.execute(query)
    rows = cur.fetchall()
    data = []
    for row in rows:
        min = row['min']
        max = row['max']
        mean = row['mean']
        quartile_1 = row['quartile_1']
        quartile_3 = row['quartile_3']
        median = row['median']
        mode = row['mode']
        data.append(dict(
            device_uuid=row['device_uuid'],
            min=min,
            max=max,
            mean=mean,
            quartile_1=quartile_1,
            quartile_3=quartile_3,
            median=median,
            mode=mode
        ))


    conn.close()

    return jsonify(data), 200

if __name__ == '__main__':
    app.run()
