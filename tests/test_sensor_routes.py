import json
import pytest
import sqlite3
import time
import unittest
import statistics
import numpy as np

from app import app


def quartiles(dataPoints):
    # check the input is not empty
    # 1. order the data set
    sortedPoints = sorted(dataPoints)
    # 2. divide the data set in two halves
    mid = len(sortedPoints) // 2 # uses the floor division to have integer returned

    if (len(sortedPoints) % 2 == 0):
        # even
        lowerQ = statistics.median(sortedPoints[:mid])
        upperQ = statistics.median(sortedPoints[mid:])
    else:
        # odd
        lowerQ = statistics.median(sortedPoints[:mid])  # same as even
        upperQ = statistics.median(sortedPoints[mid+1:])
    return (lowerQ, upperQ)


class SensorRoutesTestCases(unittest.TestCase):

    def setUp(self):
        # Setup the SQLite DB
        conn = sqlite3.connect('test_database.db')
        conn.execute('DROP TABLE IF EXISTS readings')
        conn.execute('CREATE TABLE IF NOT EXISTS readings (device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)')
        
        self.device_uuid = 'test_device'
        self.device_uuid2 = 'test_device2'

        # Setup some sensor data
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 22, int(time.time()) - (60 * 60 * 20)))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 22, int(time.time()) - 100))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 50, int(time.time()) - 50))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 100, int(time.time())))

        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    ('other_uuid', 'temperature', 22, int(time.time())))

        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid2, 'temperature', 22, int(time.time())))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid2, 'humidity', 22, int(time.time())))
        conn.commit()

        app.config['TESTING'] = True

        self.client = app.test_client

    def test_device_readings_get(self):
        # Given a device UUID
        # When we make a request with the given UUID
        request = self.client().get(
            '/devices/{}/readings/'.format(self.device_uuid)
        )

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have three sensor readings
        self.assertTrue(len(json.loads(request.data)) == 4)

    def test_device_readings_post(self):
        conn = sqlite3.connect('test_database.db')
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('select count(*) as total from readings where device_uuid="{}"'.format(self.device_uuid))
        row = cur.fetchone()
        total = row['total']

        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid), data=
        json.dumps({
            'type': 'temperature',
            'value': 101
        }), content_type='application/json')

        # Then we should receive a 201
        self.assertEqual(request.status_code, 400)

        # Given a device UUID
        # When we make a request with the given UUID to create a reading
        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid), data=
            json.dumps({
                'type': 'temperature',
                'value': 100 
            }), content_type='application/json')

        # Then we should receive a 201
        self.assertEqual(request.status_code, 201)

        # And when we check for readings in the db

        cur.execute('select * from readings where device_uuid="{}"'.format(self.device_uuid))
        rows = cur.fetchall()

        # We should have three
        self.assertTrue(len(rows) == total + 1)

    def test_device_readings_get_temperature(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's temperature data only.
        """
        request = self.client().get('/devices/{}/readings/?type={}'.format(self.device_uuid, 'temperature'))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have three sensor readings
        data = json.loads(request.data)

        self.assertTrue(len(data) == 4)

    def test_device_readings_get_humidity(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's humidity data only.
        """
        request = self.client().get('/devices/{}/readings/?type={}'.format(self.device_uuid2, 'humidity'))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have three sensor readings
        data = json.loads(request.data)

        self.assertTrue(len(data) == 1)

    def test_device_readings_get_past_dates(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's sensor data over
        a specific date range. We should only get the readings
        that were created in this time range.
        """
        start = int(time.time()) - (60 * 60 * 30)
        end = int(time.time()) - (60 * 60 * 10)
        request = self.client().get(
            '/devices/{}/readings/?start={}&end={}'.format(self.device_uuid, start, end)
        )

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have three sensor readings
        data = json.loads(request.data)

        self.assertTrue(len(data) == 1)

    def test_device_readings_min(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's min sensor reading.
        """
        request = self.client().get(
            '/devices/{}/readings/min/'.format(self.device_uuid),
            headers={"Content-Type": "application/json"}
        )
        self.assertEqual(request.status_code, 200)
        data = json.loads(request.data)
        self.assertIn('value', data)
        self.assertTrue(data.get('value') == 22)

    def test_device_readings_max(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's max sensor reading.
        """
        request = self.client().get(
            '/devices/{}/readings/max/'.format(self.device_uuid)
        )
        self.assertEqual(request.status_code, 200)
        data = json.loads(request.data)
        self.assertIn('value', data)
        self.assertTrue(data.get('value') == 100)

    def test_device_readings_median(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's median sensor reading.
        """
        request = self.client().get(
            '/devices/{}/readings/median/'.format(self.device_uuid)
        )
        self.assertEqual(request.status_code, 200)
        data = json.loads(request.data)
        self.assertIn('value', data)

        conn = sqlite3.connect('test_database.db')
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('select * from readings where device_uuid="{}"'.format(self.device_uuid))
        rows = cur.fetchall()
        datum = [row['value'] for row in rows]

        self.assertEquals(data.get('value'), statistics.median(datum))

    def test_device_readings_mean(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's mean sensor reading value.
        """
        request = self.client().get(
            '/devices/{}/readings/mean/'.format(self.device_uuid)
        )
        self.assertEqual(request.status_code, 200)
        data = json.loads(request.data)
        self.assertIn('value', data)

        conn = sqlite3.connect('test_database.db')
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('select * from readings where device_uuid="{}"'.format(self.device_uuid))
        rows = cur.fetchall()
        datum = [row['value'] for row in rows]

        self.assertEquals(data.get('value'), statistics.mean(datum))

    def test_device_readings_mode(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's mode sensor reading value.
        """
        request = self.client().get(
            '/devices/{}/readings/mode/'.format(self.device_uuid)
        )
        self.assertEqual(request.status_code, 200)
        data = json.loads(request.data)
        self.assertIn('value', data)

        conn = sqlite3.connect('test_database.db')
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('select * from readings where device_uuid="{}"'.format(self.device_uuid))
        rows = cur.fetchall()
        datum = [row['value'] for row in rows]
        self.assertEquals(data.get('value'),  statistics.mode(datum))

    def test_device_readings_quartiles(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's 1st and 3rd quartile
        sensor reading value.
        """
        request = self.client().get(
            '/devices/{}/readings/quartiles/'.format(self.device_uuid)
        )
        self.assertEqual(request.status_code, 200)
        data = json.loads(request.data)
        self.assertIn('quartile_1', data)
        self.assertIn('quartile_3', data)

        conn = sqlite3.connect('test_database.db')
        cur = conn.cursor()
        cur.execute('select * from readings where device_uuid="{}"'.format(self.device_uuid))
        rows = cur.fetchall()
        datum = [row[2] for row in rows]
        # quertiles = quartiles(datum)
        # quartile_1 = np.quantile(datum, .25)
        # quartile_3 = np.quantile(datum, .75)
        self.assertEquals(data.get('quartile_1'), 22)
        self.assertEquals(data.get('quartile_3'), 50)

    def test_readings_summary(self):
        request = self.client().get('/readings/summary/')
        self.assertEqual(request.status_code, 200)
        data = json.loads(request.data)
        for row in data:
            self.assertIn('device_uuid', row)
            self.assertIn('min', row)
            self.assertIn('max', row)
            self.assertIn('mean', row)
            self.assertIn('quartile_1', row)
            self.assertIn('quartile_3', row)
            self.assertIn('median', row)
            self.assertIn('mode', row)
