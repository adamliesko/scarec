import schedule
import time
from recommenders import recency_recommender
from recommenders import popularity_recommender


def update_trending_articles(time_interval):
    print('loled')
    current_timestamp = int(time.time())
    # RecencyRecommender.update_recent_articles()
    # PopularityRecommender.update_popular_articles()


schedule.every(1).minute.do(update_trending_articles, '1h')
schedule.every(10).minutes.do(update_trending_articles, '4h')
schedule.every(1).hour.do(update_trending_articles, '24h')
schedule.every(1).hour.do(update_trending_articles, '48h')
schedule.every(1).hour.do(update_trending_articles, '72h')

while True:
    schedule.run_pending()
    time.sleep(1)
