import datetime

from rediser import redis


class Utils:
    user_id_idx_key = 'encoded_attr_id_idx:user_id'
    user_idx = redis.get(user_id_idx_key )
    if user_idx is None:
        redis.set(user_id_idx_key, 1)

    item_id_idx_key = 'encoded_attr_id_idx:item_id'
    item_idx = redis.get(item_id_idx_key)
    if item_idx is None:
        redis.set(item_id_idx_key, 1)

    @staticmethod
    def round_time_to_last_hour_as_epoch():
        dt = datetime.date.today()
        dt = datetime.datetime(dt.year, dt.month, dt.day)
        return int(dt.timestamp())

    @staticmethod  # visit, user for collaborative filtering long to int conversion
    def encode_attribute(attr, val):
        key = 'encoded:' + str(attr) + ':' + str(val) + ':'
        existing_attr_id = redis.get(key + str(val))
        if existing_attr_id:
            return existing_attr_id
        else:
            new_val = redis.incr('encoded_attr_id_idx:' + str(attr))
            redis.set(key, new_val)
            return new_val
