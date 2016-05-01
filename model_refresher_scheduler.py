import time
import urllib.request
import schedule


def update_kmeans_model():
    urllib.request.urlopen("localhost:80/ml_models/kmeans").read()


def update_als_collaborative_model():
    urllib.request.urlopen("localhost:80/ml_models/als").read()


schedule.every(2).hours.do(update_als_collaborative_model())
schedule.every(24).hours.do(update_kmeans_model())

while True:
    try:
        schedule.run_pending()
    except Exception:
        pass
    finally:
        time.sleep(100)

# TODO: use background processes, separate class
