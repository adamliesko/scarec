#es: /Users/Adam/scarec/elasticsearch-2.1.1/bin/elasticsearch -d
redis: redis-server redis.conf
trendiness_updater: python3 trendiness_scheduler.py
api: gunicorn -w=6 scarec:app
worker: rq worker

# na macu zookeper:   zkServer start
