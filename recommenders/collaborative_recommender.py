import os
import logging
import time

from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel
from rediser import redis
from spark_context import sc
from utils import Utils
from recommenders.abstract_recommender import AbstractRecommender

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CollaborativeRecommender(AbstractRecommender):
    MODEL = None
    RANK = 5
    SEED = 42
    ITERATIONS = 15
    REGULARIZATION_PARAMETER = 0.1

    @classmethod  # using translated values of item and user - because of mapping to integer
    # we should do this on bg and not like this sync slow ..
    def recommend_to_user(cls, user_id, recs_count=10):
        translated_user_id = Utils.encode_attribute('user_id', user_id)
        try:
            recommendations = cls.MODEL.recommendProducts(int(translated_user_id), recs_count)
        except Exception:
            print(
                'WARNING: Missing encoded user id, model not learned for this')  # todo: try to pick up some same from the cluster
            return {}
        recs = {}
        for r in recommendations:
            recs[int(Utils.decode_attribute('item_id', r.product))] = r.rating

        return recs

    @classmethod
    def save_model(cls, model_path=os.environ.get('ALS_MODEL_PATH')):
        cls.MODEL.save(sc, model_path)

    @classmethod
    def load_model(cls, model_path=os.environ.get('ALS_MODEL_PATH')):
        cls.MODEL = MatrixFactorizationModel.load(sc, model_path)

    @classmethod
    def train_model(cls):
        logger.info("Running train_model method")
        start = time.time()

        cls.MODEL = ALS.trainImplicit(cls.load_train_data, cls.RANK, seed=cls.SEED,
                                      iterations=cls.ITERATIONS, lambda_=cls.REGULARIZATION_PARAMETER)
        logger.info("Train_model finished, took: ", str(time.time() - start))

    @classmethod
    def load_train_data(cls):
        logger.info("Loading initial data")
        keys = redis.keys('windowed_visits*')

        cls.reset_and_set_loaded_visits_set(keys)
        raw_user_item_pairs = [cls.__get_redis_members(key) for key in keys]
        visits = []
        for pairs in raw_user_item_pairs:
            visits += [cls.__create_user_item_tuple(pairs) for pairs in pairs]
        return sc.parallelize(visits)

    @classmethod
    def update_model(cls):
        # TODO: do only incremental learn - use persist RDD and add windowed visits from only a certain point in time
        cls.train_model()

    @staticmethod
    def __create_user_item_tuple(visit):
        user, item = visit.decode('utf-8').split(':')
        enc_user = Utils.encode_attribute('user_id', user)
        enc_item = Utils.encode_attribute('item_id', item)

        return (int(enc_user), int(enc_item), 1.0)  # implicit 1 rating

    @staticmethod
    def __reset_and_set_loaded_visits_set(keys):
        key = 'processed_windowed_visits'
        redis.delete(key)
        redis.sadd(key, keys)
