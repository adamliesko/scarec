from recommender_strategies.aggregators.aggregator import Aggregator


class SumAggregator(Aggregator):
    @staticmethod
    def merge_recommendations(*recommendations_w_values, weights=None):
        final_recs = Aggregator.init_final_recs(0)
        if not weights:
            weights = [1] * len(recommendations_w_values)

        i = 0
        for recs_group in recommendations_w_values:
            recs_group_weight = weights[i]
            for rec, val in recs_group.items():
                final_recs[rec] += val * recs_group_weight

        return final_recs
