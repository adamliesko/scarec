from collections import defaultdict


class Aggregator:
    @staticmethod
    def init_final_recs(value):
        recs = {}
        return defaultdict(lambda: value, recs)

    @staticmethod
    def merge_recommendations(weights=[], *recommendations_w_values):
        pass
