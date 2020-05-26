import asyncio
import time
import uuid
from aiohttp import ClientTimeout
from aiohttp.test_utils import AioHTTPTestCase, TestClient, unittest_run_loop
from db import save_readings, truncate_readings, init_database, get_pool
from main import make_app

asyncio.get_event_loop().run_until_complete(init_database())


class DeviceReadingsTestCase(AioHTTPTestCase):

    async def setUpAsync(self):
        await super(DeviceReadingsTestCase, self).setUpAsync()

    def setUp(self, *args, **kwargs):
        self.device_a = uuid.uuid1()
        self.device_b = uuid.uuid1()
        self.device_c = uuid.uuid1()
        super(DeviceReadingsTestCase, self).setUp(*args, **kwargs)
        self.loop.run_until_complete(get_pool(loop=self.loop))

    async def tearDownAsync(self):
        await truncate_readings()

    async def get_client(self, server):
        """Return a TestClient instance."""
        timeout = ClientTimeout(total=60)
        return TestClient(server, loop=self.loop, timeout=timeout)

    def get_app(self):
        return make_app()

    def assert_device_readings(self, status, data, length):
        self.assertEqual(status, 200)
        self.assertTrue(data)
        self.assertEqual(data.get('status'), "success")

        readings = data.get('data')
        self.assertEqual(len(readings), length)

    async def create_readings_battery (self, data):
        for record in data:
            device_uuid, _type, _value, date_created = record
            await save_readings(device_uuid, _type, _value, date_created)

    async def assert_device_metric(self, device_uuid, metric, expected_value,
                                   start=None, end=None, _type=None, status=200):
        qst = ''
        if start:
            qst += '&start=%d' % start
        if end:
            qst += '&end=%d' % end
        if _type:
            qst += '&type=%s' % _type
        result = await self.client.get('/devices/{}/readings/{}?{}'.format(device_uuid, metric, qst))
        self.assertEqual(result.status, status)
        data = await result.json()
        self.assertTrue(data)
        self.assertEqual(data.get('status'), "success")
        if metric == 'quartiles':
            self.assertIn('quartile_1', data.get('data'))
            self.assertIn('quartile_3', data.get('data'))
        else:
            self.assertEqual(int(expected_value), int(float(data.get('data'))))

    @unittest_run_loop
    async def test_add_device_readings(self):
        data = {
            'type': 'temperature',
            'value': 100,
            'date_created': int(time.time())
        }
        result = await self.client.post('/devices/{}/readings'.format(self.device_a), data=data)
        self.assertEqual(result.status, 200)

    @unittest_run_loop
    async def test_get_readings(self):
        battery = [
            (self.device_a, 'temperature', 22, int(time.time()) - (60 * 60 * 20)),
            (self.device_a, 'temperature', 22, int(time.time()) - 100),
            (self.device_a, 'temperature', 23, int(time.time()) - 100),
            (self.device_b, 'temperature', 80, int(time.time()) - 100),
            (self.device_c, 'temperature', 100, int(time.time()) - 100)
        ]
        await self.create_readings_battery(battery)
        start = int(time.time() - (60 * 60 * 22))
        result = await self.client.get('/devices/{}/readings?start={}'.format(self.device_a, start))
        self.assert_device_readings(result.status, await result.json(), 3)

    @unittest_run_loop
    async def test_get_readings_filtering_by_dates(self):
        battery = [
            (self.device_a, 'temperature', 22, int(time.time()) - (60 * 60 * 20)),
            (self.device_a, 'temperature', 22, int(time.time()) - (60 * 60 * 18)),
            (self.device_a, 'temperature', 22, int(time.time()) - (60 * 60 * 10)),
            (self.device_b, 'temperature', 150, int(time.time()) - (60 * 60 * 10)),
            (self.device_b, 'temperature', 12, int(time.time()) - (60 * 60 * 10))
        ]
        await self.create_readings_battery(battery)
        start = int(time.time() - (60 * 60 * 22))
        end = int(time.time() - (60 * 60 * 17))
        result = await self.client.get('/devices/{}/readings?start={}&end={}'.format(self.device_a, start, end))
        self.assert_device_readings(result.status, await result.json(), 2)


    @unittest_run_loop
    async def test_get_redings_filtering_by_type(self):
        battery = [
            (self.device_a, 'temperature', 22, int(time.time()) - (60 * 60 * 10)),
            (self.device_a, 'temperature', 26, int(time.time()) - (60 * 60 * 18)),
            (self.device_a, 'temperature', 22, int(time.time()) - (60 * 60 * 10)),
            (self.device_a, 'humidity', 45, int(time.time()) - (60 * 60 * 18)),
            (self.device_a, 'humidity', 42, int(time.time()) - (60 * 60 * 10))
        ]
        await self.create_readings_battery(battery)
        start = int(time.time() - (60 * 60 * 22))
        result = await self.client.get('/devices/{}/readings?start={}&type=humidity'.format(self.device_a, start))
        self.assert_device_readings(result.status, await result.json(), 2)


    @unittest_run_loop
    async def test_get_metric_by_device_uuid(self):
        battery = [
            (self.device_a, 'temperature', 21, int(time.time()) - (60 * 60 * 10)),
            (self.device_a, 'temperature', 26, int(time.time()) - (60 * 60 * 18)),
            (self.device_a, 'temperature', 22, int(time.time()) - (60 * 60 * 10)),
            (self.device_a, 'humidity', 90, int(time.time()) - (60 * 60 * 14)),
            (self.device_a, 'humidity', 80, int(time.time()) - (60 * 60 * 13)),
            (self.device_a, 'humidity', 80, int(time.time()) - (60 * 60 * 13)),
            (self.device_a, 'humidity', 80, int(time.time()) - (60 * 60 * 13)),
            (self.device_a, 'humidity', 80, int(time.time()) - (60 * 60 * 13)),
            (self.device_a, 'humidity', 80, int(time.time()) - (60 * 60 * 13))
        ]
        await self.create_readings_battery(battery)
        start = int(time.time() - (60 * 60 * 22))
        await self.assert_device_metric(self.device_a, 'max', 26, start=start, _type='temperature')
        await self.assert_device_metric(self.device_a, 'min', 21, start=start, _type='temperature')
        await self.assert_device_metric(self.device_a, 'median', 22, start=start, _type='temperature')
        await self.assert_device_metric(self.device_a, 'mean', 23, start=start, _type='temperature')
        await self.assert_device_metric(self.device_a, 'mode', 21, start=start, _type='temperature')
        await self.assert_device_metric(self.device_a, 'quartiles', 23, start=start, _type='humidity')

    @unittest_run_loop
    async def test_get_readings_summary(self):
        battery = [
            (self.device_a, 'temperature', 21, int(time.time()) - (60 * 60 * 10)),
            (self.device_a, 'temperature', 26, int(time.time()) - (60 * 60 * 18)),
            (self.device_b, 'temperature', 22, int(time.time()) - (60 * 60 * 10)),
            (self.device_a, 'humidity', 90, int(time.time()) - (60 * 60 * 14)),
            (self.device_a, 'humidity', 80, int(time.time()) - (60 * 60 * 13)),
            (self.device_a, 'humidity', 80, int(time.time()) - (60 * 60 * 13)),
            (self.device_b, 'humidity', 80, int(time.time()) - (60 * 60 * 13)),
            (self.device_b, 'humidity', 80, int(time.time()) - (60 * 60 * 13)),
            (self.device_c, 'humidity', 80, int(time.time()) - (60 * 60 * 13))
        ]
        await self.create_readings_battery(battery)
        start = int(time.time() - (60 * 60 * 22))
        result = await self.client.get('/readings/summary?start={}'.format(start))
        self.assertEqual(result.status, 200)
        data = await result.json()
        self.assertTrue(data)
        self.assertEqual(data.get('status'), "success")
        self.assertEqual(len(data.get('data')), 3)

        for row in data.get('data'):
            self.assertIn('device_uuid', row)
            self.assertIn('min', row)
            self.assertIn('max', row)
            self.assertIn('mean', row)
            self.assertIn('quartile_1', row)
            self.assertIn('quartile_3', row)
            self.assertIn('median', row)
            self.assertIn('mode', row)




