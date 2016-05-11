from recommender_strategies.aggregators.aggregator import Aggregator


class ProductAggregator(Aggregator):
    @staticmethod
    def merge_recommendations(*recommendations_w_values, weights=None):
        final_recs = Aggregator.init_final_recs(1)
        if not weights:
            weights = [1] * len(recommendations_w_values)

        i = 0
        for recs_group in recommendations_w_values:
            recs_group_weight = weights[i]
            for rec, val in recs_group.items():
                final_recs[rec] *= (1+val) * recs_group_weight

        final_recs = sorted(final_recs.items(), key=lambda x: -1 * float(x[1]))

        return list(final_recs.keys())
