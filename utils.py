import datetime

from rediser import redis


class Utils:
    @staticmethod
    def round_time_to_last_hour_as_epoch():
        dt = datetime.date.today()
        dt = datetime.datetime(dt.year, dt.month, dt.day)
        return int(dt.timestamp())

    @staticmethod
    def encode_user_id(user_id):
        complex

    @staticmethod  # visit, user for collaborative filtering long to int conversion
    def encode_attribute(attr, val):
        key = 'encoded_' + val + ':'
        existing_visit_id = redis.get(key + str(val))
        if existing_visit_id:
            return existing_visit_id
        else:
            new_val = redis.incr('encoded_visit_id_idx')
            redis.set(key, new_val)
            return new_val
