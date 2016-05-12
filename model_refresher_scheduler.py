import time
import urllib.request
import schedule


class ModelRefresherScheduler:
    @staticmethod
    def update_kmeans_model():
        urllib.request.urlopen("http://localhost:80/ml_models/kmeans").read()

    @staticmethod
    def update_als_collaborative_model():
        urllib.request.urlopen("http://localhost:80/ml_models/als").read()

    @staticmethod
    def update_rf_models():
        urllib.request.urlopen("http://localhost:80/ml_models/fr").read()


# schedule.every(2).hours.do(ModelRefresherScheduler.update_als_collaborative_model())
# schedule.every(24).hours.do(ModelRefresherScheduler.update_kmeans_model())
# schedule.every(24).hours.do(ModelRefresherScheduler.update_rf_models())

while True:
    try:
        schedule.run_pending()
    except Exception:
        pass
    finally:
        time.sleep(100)
