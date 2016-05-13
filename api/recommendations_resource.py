import json

import falcon

from models.recommendation import Recommendation
from recommenders.recommender_facade import RecommenderFacade


class RecommendationsResource:
    WEIGHTS_KEY = '2'  # index for weights of recommendations, not used in our case, it is prefilled statically
    WEIGHT = 0.9  # FIXED_VALUE, MAYBE WE COULD GET THE VALUE FROM THE AGGREGATORS TODO: think about this
    RECS_KEY = '3'  # index for actual recommendations for PLISTA

    # algorithm is required param, see the RecommenderFacade map for list of supported values
    def on_post(self, req, resp, algorithm):
        body = req.stream.read()
        if not body:
            raise falcon.HTTPBadRequest('Empty request body',
                                        'A valid JSON document is required.')
        data = json.loads(body.decode('utf-8'))

        recommendation_req = Recommendation(data)
        recommendation_req.persist()
        recommendations = RecommenderFacade.recommend_to_user(recommendation_req, algorithm)
        response_body = self.build_recommendation_response(recommendations)
        resp.body = str(json.dumps(response_body))
        resp.status = falcon.HTTP_200

    # # without supplied algorithm id, use the default one
    # def on_post(self, req, resp):
    #     body = req.stream.read()
    #     if not body:
    #         raise falcon.HTTPBadRequest('Empty request body',
    #                                     'A valid JSON document is required.')
    #     data = json.loads(body.decode('utf-8'))
    #
    #     recommendation_req = Recommendation(data)
    #     recommendation_req.persist()
    #     recommendations = RecommenderFacade.recommend_to_user(recommendation_req, RecommenderFacade.DEFAULT_ALGORITHM_NO)
    #     response_body = self.build_recommendation_response(recommendations)
    #     resp.body = str(json.dumps(response_body))
    #     resp.status = falcon.HTTP_200


    def build_recommendation_response(self, recommendations):
        resp = {"recs": {}}
        resp["recs"]["ints"] = {self.RECS_KEY: recommendations}
        resp["recs"]["floats"] = {self.WEIGHTS_KEY: [self.WEIGHT] * len(recommendations)}
        return resp
