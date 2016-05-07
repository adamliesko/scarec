trendiness_updater: python3 trendiness_scheduler.py
model_updater: python3 model_refresher_scheduler.py
api: UWSGI_WORKERS=8   uwsgi --master --http :5100 --wsgi-file scarec.py --callable app

#DISABLED - USE SYSTEMWIDE:
#es: /elasticsearch-2.1.1/bin/elasticsearch -d
#redis: redis-server redis.conf
#rq_worker: rq worker impression_processing
