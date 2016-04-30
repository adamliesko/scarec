from models.impression import Impression


class RecommenderStrategy:
    @staticmethod
    def recommend_to_user(user_id):
        pass

    @staticmethod
    def user_impressions(user_id):
        return Impression.user_impressions(user_id)