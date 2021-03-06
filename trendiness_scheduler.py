import time

import schedule

from recommenders.recency_recommender import RecencyRecommender
from recommenders.popularity_recommender import PopularityRecommender


class TrendinessScheduler:
    TIME_DIFFS = {'1h': 60 * 60,
                  '4h': 60 * 60 * 4,
                  '24h': 60 * 60 * 24,
                  '48h': 60 * 60 * 48,
                  '72h': 60 * 60 * 72,
                  '168h': 60 * 60 * 168}

    @staticmethod
    def update_popular_articles(time_interval):
        current_timestamp = int(time.time())
        origin_timestamp = current_timestamp - TrendinessScheduler.TIME_DIFFS[time_interval]
        PopularityRecommender.update_popular_articles(origin_timestamp, time_interval)

    @staticmethod
    def update_recent_articles():
        current_timestamp = int(time.time())
        RecencyRecommender.update_recent_articles(current_timestamp)


schedule.every(10).minutes.do(TrendinessScheduler.update_popular_articles, '1h')
schedule.every(10).minutes.do(TrendinessScheduler.update_popular_articles, '4h')
schedule.every(1).hour.do(TrendinessScheduler.update_popular_articles, '24h')
schedule.every(1).hour.do(TrendinessScheduler.update_popular_articles, '48h')
schedule.every(2).hours.do(TrendinessScheduler.update_popular_articles, '72h')
schedule.every(2).hours.do(TrendinessScheduler.update_popular_articles, '168h')

schedule.every(10).minutes.do(TrendinessScheduler.update_recent_articles)

while True:
    try:
        schedule.run_pending()
    except Exception:
        pass
    finally:
        time.sleep(100)

# TODO: use background processes, separate class
