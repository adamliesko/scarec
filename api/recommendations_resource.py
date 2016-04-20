# Let's get this party started
import json

import falcon

from models.recommendation import Recommendation
from recommenders.recommender_facade import RecommenderFacade


# Falcon follows the REST architectural style, meaning (among
# other things) that you think in terms of resources and state
# transitions, which map to HTTP verbs.
class RecommendationsResource:
    WEIGHTS_KEY = '2'
    RECS_KEY = '3'

    def on_post(self, req, resp, algorithm, time_interval):
        body = req.stream.read()
        if not body:
            raise falcon.HTTPBadRequest('Empty request body',
                                        'A valid JSON document is required.')
        data = json.loads(body.decode('utf-8'))
        recommendation_req = Recommendation(data)
        recommendation_req.persist()
        recommendations = RecommenderFacade.recommend_to_user(recommendation_req, data, algorithm, time_interval)
        response_body = self.build_recommendation_response(recommendations)
        resp.body(response_body)
        resp.status = falcon.HTTP_200

    def build_recommendation_response(self, recommendations):
        resp = {}
        resp["recs"] = {}
        resp["recs"]["ints"] = {self.RECS_KEY: recommendations}
        resp["recs"]["floats"] = {self.WEIGHTS_KEY: [0.5] * len(recommendations)}
        return resp

        # identifikuj cluster
