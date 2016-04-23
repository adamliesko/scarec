import datetime


class Utils:
    @staticmethod
    def round_time_to_last_hour_as_epoch():
        dt = datetime.date.today()
        dt = datetime.datetime(dt.year, dt.month, dt.day)
        return int(dt.timestamp())
