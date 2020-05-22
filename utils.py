import sqlite3


def get_db_name(app):
    return 'database.db' if not app.config['TESTING'] else 'test_database.db'


def init_db(app):
    # Setup the SQLite DB
    db_name = get_db_name(app)
    conn = sqlite3.connect(db_name)
    conn.execute(
        'CREATE TABLE IF NOT EXISTS readings (device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)')
    conn.close()


def get_db_cursor(app):
    db_name = get_db_name(app)
    conn = sqlite3.connect(db_name)
    conn.row_factory = sqlite3.Row
    return conn.cursor(), conn


def get_where_statement(device_uuid=None, args={}):
    filters = {'type': str, 'start': int, 'end': int}
    map_fields = {'start': 'date_created', 'end': 'date_created'}

    if device_uuid:
        where_statements = ['device_uuid = "{}"'.format(device_uuid)]
    else:
        where_statements = []

    for _filter, _type in filters.items():
        if args.get(_filter, type=_type):
            _filter_value = args.get(_filter, type=_type)
            _field = _filter if not map_fields.get(_filter) else map_fields.get(_filter)
            if _field == 'date_created':
                _comparator = '>=' if _filter == 'start' else '<='
                where_statements.append('{} {} {}'.format(_field, _comparator, _filter_value))
            else:
                where_statements.append('{} = "{}"'.format(_field, _filter_value))

    return '{}'.format(' and '.join(where_statements)) if len(where_statements) else ''
