import os
import logging
import time

from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel
from rediser import redis
from spark_context import sc
from utils import Utils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CollaborativeRecommender:
    @classmethod
    def __train_model(self):
        logger.info("Running __train_model")
        start = time.time()
        self.model = ALS.trainImplicit(self.visits_RDD, self.rank, seed=self.seed,
                                       iterations=self.iterations, lambda_=self.regularization_parameter)
        logger.info("Run of __train_model finished, took: ", str(time.time() - start))

    def __load_initial_data(self):
        logger.info("Loading initial data")
        keys = redis.keys('windowed_visits*')

        self.__reset_and_set_loaded_visits_set(keys)
        raw_user_item_pairs = [self.__get_redis_members(key) for key in keys]
        visits = []
        for pairs in raw_user_item_pairs:
            visits += [self.__create_user_item_tuple(pairs) for pairs in pairs]
        self.visits_RDD = sc.parallelize(visits)

    @staticmethod
    def __get_redis_members(key):
        return redis.smembers(key)

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





    def add_new_visits(self):
        """Add additional visits in the format (user_id, article_id)
        """
        logger.info("Adding new visits")
        keys = redis.keys('windowed_visits*')
        processed_keys = redis.smembers('processed_windowed_visits')
        new_keys = [key for key in keys if not key in processed_keys]

        dist_keys = self.sc.parallelize(new_keys)
        raw_user_item_pairs = dist_keys.map(lambda key: redis.smembers(key))
        visits = raw_user_item_pairs.map(lambda user_item_pair: user_item_pair.split(':')).map(
            lambda user, item: (user, item))

        new_visits_rdd = self.sc.parallelize(visits)
        self.visits_RDD = self.visits_RDD.union(new_visits_rdd)

    def recommend_to_user(self, user_id, articles_count=10):
        translated_user_id = Utils.encode_attribute('user_id', user_id)
        recommendations = self.model.recommendProducts(int(translated_user_id), articles_count)
        recommendations = [Utils.decode_attribute('item_id', r.product) for r in recommendations]
        return recommendations

    def __init__(self, sc):
        self.rank = 8
        self.seed = 42
        self.iterations = 10
        self.regularization_parameter = 0.1

        logger.info("Starting up CollaborativeFiltering SPARK")

        self.sc = sc

        model_path = 'als_model_' + '_'.join([str(self.rank), str(self.iterations), str(self.regularization_parameter)])

        if False and os.path.exists(model_path):
            self.model = MatrixFactorizationModel.load(self.sc, model_path)

        else:
            # nacitame data z redisu alebo odniekial , namapujeme ich na (user,article) party
            self.__load_initial_data()
            self.visits_RDD.cache()
            # Train the model
            self.__train_model()
            self.model.save(self.sc, model_path)







    # def __predict_visits(self, user_id_article_id_rdd):
    #     """Gets predictions for a given (user_id, article_id) formatted RDD
    #     Returns: an RDD with format (article_id, confidence)
    #     """
    #     predictions_rdd = self.model.predictAll(user_id_article_id_rdd)
    #     predictions_conf_rdd = predictions_rdd.map(lambda x: (x.article, x.rating))
    #
    #     return predictions_conf_rdd



    # DEPRECATED: use recommend_to_user
    # def get_prediction_for_article_ids(self, user_id, article_ids):
    #     """Given a user_id and a list of article_ids, predict ratings for them
    #     """
    #     requested_articles_rdd = self.sc.parallelize(article_ids).map(lambda x: (user_id, x))
    #     # Get predicted confidences (implicit feedback only)
    #
    #     predictions = self.__predict_visits(requested_articles_rdd).collect()
    #
    #     return predictions
    #
    # def get_top_recommendations(self, user_id, articles_count):
    #     user_seen = redis.get('user_visits:' + user_id)
    #     articles_recent = redis.zrange('articles', 0, 1000)
    #
    #     user_unseen_articles_ids = articles_recent - user_seen
    #     user_unseen_articles = [(user_id, article_id) for article_id in user_unseen_articles_ids]
    #     user_unseen_articles_rdd = self.sc.paralelize(user_unseen_articles)
    #
    #     # Get predicted confidence
    #     recommendations = self.__predict_visits(user_unseen_articles_rdd).takeOrdered(articles_count,
    #                                                                                   key=lambda x: -x[1])
    #
    #     return recommendations