from recommender_strategies.aggregators.aggregator import Aggregator


class ProductAggregator(Aggregator):
    @staticmethod
    def merge_recommendations(*recommendations_w_values, weights=None):
        final_recs = Aggregator.init_final_recs(0)
        if not weights:
            weights = [1] * len(recommendations_w_values)

        i = 0
        for recs_group in recommendations_w_values:
            recs_group_weight = weights[i]
            for rec, val in recs_group.items():
                if val > final_recs.get(rec, 0):
                    final_recs[rec] = val * recs_group_weight
            i += 1
        final_recs = sorted(final_recs.items(), reverse=True, key=lambda x: float(x[1]))
        final_recs = [item for item, score in final_recs]

        return final_recs
