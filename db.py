import os
import time

import asyncpg
import asyncpgsa

pool = None


async def get_pool():
    global pool
    if pool:
        return pool
    pool = await asyncpgsa.create_pool(os.environ.get('POSTGRES_URL'))
    return pool



async def init_database():
    _pool = await get_pool()
    # Execute a statement to create a new table.
    async with _pool.transaction() as conn:
        await conn.execute('''
                CREATE TABLE IF NOT EXISTS readings (
                    device_uuid TEXT, 
                    type TEXT, 
                    value INTEGER, 
                    date_created INTEGER
                );
                CREATE INDEX IF NOT EXISTS idx_device_uuid 
                ON readings(device_uuid);
                
                CREATE INDEX IF NOT EXISTS idx_type 
                ON readings(type);
                
                CREATE OR REPLACE FUNCTION _final_median(NUMERIC[])
                   RETURNS NUMERIC AS
                $$
                   SELECT AVG(val)
                   FROM (
                     SELECT val
                     FROM unnest($1) val
                     ORDER BY 1
                     LIMIT  2 - MOD(array_upper($1, 1), 2)
                     OFFSET CEIL(array_upper($1, 1) / 2.0) - 1
                   ) sub;
                $$
                LANGUAGE 'sql' IMMUTABLE;
                DROP AGGREGATE IF EXISTS median(NUMERIC);
                CREATE AGGREGATE median(NUMERIC) (
                      SFUNC=array_append,
                      STYPE=NUMERIC[],
                      FINALFUNC=_final_median,
                      INITCOND='{}'
                    );
                CREATE OR REPLACE FUNCTION _final_mode(anyarray)
                  RETURNS anyelement AS
                $BODY$
                    SELECT a
                    FROM unnest($1) a
                    GROUP BY 1 
                    ORDER BY COUNT(1) DESC, 1
                    LIMIT 1;
                $BODY$
                LANGUAGE SQL IMMUTABLE;
                 
                -- Tell Postgres how to use our aggregate
                DROP AGGREGATE IF EXISTS cmode(anyelement);
                CREATE AGGREGATE cmode(anyelement) (
                  SFUNC=array_append, --Function to call for each row. Just builds the array
                  STYPE=anyarray,
                  FINALFUNC=_final_mode, --Function to call after everything has been added to array
                  INITCOND='{}' --Initialize an empty array when starting
                );
                CREATE EXTENSION IF NOT EXISTS quantile;
            ''')


async def get_device_readings(device_uuid, _type=None, start=None, end=None):
    start = start if start else int(time.time() - (60 * 60))
    end = end if end else int(time.time())
    _pool = await get_pool()
    try:
        async with _pool.acquire() as conn:
            query = 'select * from readings where device_uuid = $1 and date_created >= $2 and date_created <= $3'
            if _type:
                query = "{} and type = '{}'".format(query, _type)
            rows = await conn.fetch(query, str(device_uuid), start, end)
            return [dict(r) for r in rows]
    except Exception as e:
        _ = e
        return []


async def get_device_metrics(device_uuid, metric, _type=None, start=None, end=None):
    start = start if start else int(time.time() - (60 * 60))
    end = end if end else int(time.time())
    _pool = await get_pool()
    try:
        async with _pool.acquire() as conn:
            where = '''
                device_uuid = $1 and 
                date_created >= $2 and 
                date_created <= $3
            '''
            if _type:
                where = "{} and type = '{}'".format(where, _type)

            if metric == 'mean':
                metric = 'avg'
            if metric == 'mode':
                metric = 'cmode'
            if metric != 'quartiles':
                query = '''
                    select 
                    {}(value) as v
                    from readings 
                    where 
                    {}
                '''.format(metric, where)
                val = await conn.fetchval(query, str(device_uuid), start, end)
                return val
            else:
                query = '''
                select 
                    quantile(value, ARRAY[0.25, 0.75]) as v
                from readings 
                where 
                    {}
                '''.format(where)
                row = await conn.fetchval(query, str(device_uuid), start, end)
                return row

    except Exception as e:
        _ = e
        return 0


async def get_readings_summary(start=None, end=None, _type=None):
    start = start if start else int(time.time() - (60 * 60))
    end = end if end else int(time.time())
    _pool = await get_pool()
    try:
        async with _pool.acquire() as conn:
            where = '''                     
                    date_created >= $1 and 
                    date_created <= $2
                '''
            if _type:
                where = "{} and type = '{}'".format(where, _type)

            query = '''
                select 
                    device_uuid,
                    min(value) as _min,
                    max(value) as _max,
                    avg(value) as _mean,
                    median(value) as _median,
                    cmode(value) as _mode,
                    quantile(value, ARRAY[0.25, 0.75]) as quartiles
                from readings 
                where 
                    {} group by device_uuid
                '''.format(where)
            rows = await conn.fetch(query, start, end)
            return [{
                'device_uuid': r['device_uuid'],
                'min': r['_min'],
                'max': r['_max'],
                'mean': int(r['_mean']),
                'median': int(r['_median']),
                'mode': int(r['_mode']),
                'quartile_1': int(r['quartiles'][0]) if r['quartiles'][0] else 0,
                'quartile_3': int(r['quartiles'][1]) if r['quartiles'][1] else 0,
            } for r in rows]
    except Exception as e:
        _ = e
        return []


async def save_readings(device_uuid, type, value, date_created):
    _pool = await get_pool()
    # Execute a statement to create a new table.
    try:
        async with _pool.transaction() as conn:
            # Insert a record into the created table.
            await conn.execute('''
                    INSERT INTO readings (device_uuid, type, value, date_created) VALUES ($1, $2, $3, $4)
                ''', str(device_uuid), str(type), int(value), int(date_created))
    except Exception as e:
        _ = e


async def truncate_readings():
    _pool = await get_pool()
    # Execute a statement to create a new table.
    async with _pool.transaction() as conn:
        # Insert a record into the created table.
        await conn.execute('TRUNCATE readings;')

