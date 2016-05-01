import logging
import falcon
from clustering.clustering_model import ClusteringModel
from recommenders.collaborative_recommender import CollaborativeRecommender


class ItemsResource:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def on_get(self, req, resp, type):
        if type == 'als':
            CollaborativeRecommender.update_model()
        elif type == 'kmeans':
            ClusteringModel.update_model()
        resp.status = falcon.HTTP_200
