import os
import logging
import time

from pyspark.mllib.recommendation import ALS
from rediser import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CollaborativeRecommenderEngine:
    def __train_model(self):
        logger.info("Running __train_model")
        start = time.time()
        self.model = ALS.trainImplicit(self.ratings_RDD, self.rank, seed=self.seed,
                                       iterations=self.iterations, lambda_=self.regularization_parameter)
        logger.info("Run of __train_model finished, took: ", time.time() - start)

    def __predict_visits(self, user_id_article_id_rdd):
        """Gets predictions for a given (user_id, article_id) formatted RDD
        Returns: an RDD with format (article_id, confidence)
        """
        predictions_rdd = self.model.predictAll(user_id_article_id_rdd)
        predictions_conf_rdd = predictions_rdd.map(lambda x: (x.article, x.rating))

        return predictions_conf_rdd

    def add_visits(self, visits):
        """Add additional movie ratings in the format (user_id, movie_id, rating)
        """
        # Convert ratings to an RDD
        new_visits_rdd = self.sc.parallelize(visits)
        # Add new ratings to the existing ones
        self.visits_RDD = self.visits_rdd.union(new_visits_rdd)
        # self.__train_model() - when to do it, on the fly swap etc

        return visits

    def get_prediction_for_article_ids(self, user_id, article_ids):
        """Given a user_id and a list of article_ids, predict ratings for them
        """
        requested_articles_rdd = self.sc.parallelize(article_ids).map(lambda x: (user_id, x))
        # Get predicted confidences (implicit feedback only)

        predictions = self.__predict_visits(requested_articles_rdd).collect()

        return predictions

    def get_top_recommendations(self, user_id, articles_count):

        # load all articles from redis
        # filter out those not in seen in 1 req
        #
        user_seen = redis.get('user_visits:' + user_id)
        articles_recent = redis.zrange('articles', 0, 1000, True)

        user_unseen_articles_ids = articles_recent - user_seen
        user_unseen_articles_rdd = [(user_id, article_id) for article_id in user_unseen_articles_ids]

        # Get predicted confidence
        recommendations = self.__predict_visits(user_unseen_articles_rdd).takeOrdered(articles_count,
                                                                                      key=lambda x: -x[1])

        return recommendations

    def __init__(self, sc):
        self.rank = 8
        self.seed = 42
        self.iterations = 10
        self.regularization_parameter = 0.1

        logger.info("Starting up CollaborativeFiltering SPARK")

        self.sc = sc

        model_path = 'als_model' + '_'.join([str(self.rank), str(self.iterations), str(self.regularization_parameter)])

        if os.path.exists(model_path):
            self.model = ALS.load
        else:
            # nacitame data z redisu alebo odniekial , namapujeme ich na (user,article) party
            self.visits_RDD = [].cache()
            # Train the model
            self.__train_model()
