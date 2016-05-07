import logging
import falcon
from classification.random_forester import RandomForester
from clustering.clustering_model import ClusteringModel
from recommenders.collaborative_recommender import CollaborativeRecommender

#TODO: make this processing async
class ModelsResource:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def on_get(self, req, resp, type):
        if type == 'als':
            CollaborativeRecommender.update_model()
        elif type == 'kmeans':
            ClusteringModel.update_model()
        elif type == 'rf':
            RandomForester.update_model()
        resp.status = falcon.HTTP_200
