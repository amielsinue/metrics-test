from aiohttp import web
from db import get_device_readings, init_database, get_device_metrics, get_readings_summary, save_readings
import asyncio
import aiohttp_cors


async def success_response(data, message='', code=200):
    try:
        return web.json_response(dict(status='success', data=data, message=message), status=code)
    except Exception as e:
        _ = e
        return await error_response(str(e), 500)


async def error_response(error_message, code=400):
    return web.json_response(dict(status='error', message=error_message), status=code)


async def device_readings_post(request):
    device_uuid = request.match_info.get('device_uuid')
    data = await request.post()
    _type = data.get('type')
    value = int(data.get('value'))
    date_created = int(data.get('date_created'))
    if not(_type and value and date_created):
        return await error_response("type, value and date_created parameters are required", 400)
    if _type in ['temperature', 'humidity'] and (value > 100 or value < 0):
        return await error_response("value for temperature and humidity has to be between 0 and 100", 400)

    await save_readings(device_uuid, _type, value, date_created)
    return await success_response([], 'Record created')


def get_query_params(query):
    return [
        int(query.get('start')) if query.get('start') else 0,
        int(query.get('end')) if query.get('end') else 0,
        query.get('type') if query.get('type') else None
    ]


async def device_readings_get(request):
    device_uuid = request.match_info.get('device_uuid')
    start, end, _type = get_query_params(request.query)
    data = await get_device_readings(device_uuid, start=start, end=end, _type=_type)
    return await success_response(data)


async def device_readings_metrics_get(request):
    device_uuid = request.match_info.get('device_uuid')
    metric = request.match_info.get('metric')
    if metric not in ['max', 'median', 'mean', 'quartiles', 'min', 'mode']:
        return await error_response("invalid metric", 400)
    start, end, _type = get_query_params(request.query)
    value = await get_device_metrics(device_uuid, metric, start=start, end=end, _type=_type)
    if isinstance(value, list):
        return await success_response(dict(quartile_1=value[0], quartile_3=value[1]))
    return await success_response(str(value))


async def readings_metrics_summary(request):
    start, end, _type = get_query_params(request.query)
    data = await get_readings_summary(start, end, _type)
    return await success_response(data)


def make_app():
    app = web.Application()
    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
            allow_methods="*"
        )
    })
    app.router.add_post('/devices/{device_uuid}/readings', device_readings_post)
    app.router.add_get('/devices/{device_uuid}/readings', device_readings_get)
    app.router.add_get('/devices/{device_uuid}/readings/{metric}', device_readings_metrics_get)
    app.router.add_get('/readings/summary', readings_metrics_summary)
    for route in list(app.router.routes()):
        cors.add(route)
    return app

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(init_database())
    web.run_app(make_app())
