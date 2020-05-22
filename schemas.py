import json
from httplib import BAD_REQUEST

from flask_restful import abort
from marshmallow import Schema, fields, pre_load, post_load, validate


class CreateDeviceReading(Schema):
    type = fields.Str(required=True)
    value = fields.Int(required=True)

    @pre_load
    def pre_load(self, data):
        data = json.loads(data)
        if data.get('type') in ['temperature', 'humidity'] and (data.get('value') < 0 or data.get('value') > 100):
            abort(BAD_REQUEST, message='Invalid {} field, should be between 0 - 100'.format(data.get('type')))

        return data