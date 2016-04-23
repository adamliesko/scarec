trendiness_updater: python3 trendiness_scheduler.py
api: UWSGI_WORKERS=8 uwsgi --http :5100 --wsgi-file scarec.py --callable app

#DISABLED - USE STANDALONE:
#api: gunicorn -w=1 scarec:app
#es: /Users/Adam/scarec/elasticsearch-2.1.1/bin/elasticsearch -d
#redis: redis-server redis.conf
#rq_worker: rq worker impression_processing
