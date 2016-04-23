import json

from spark_context import sc
from pyspark.mllib.util import MLUtils
from clustering.clustering_evaluator import ClusteringEvaluator
from clustering.clustering_model import ClusteringModel
from contextual import Context
from contextual import ContextEncoder
from rediser import redis

SEED = 13

sc.addPyFile("/Users/Adam/PycharmProjects/scarec/contextual.zip")
sc.addPyFile("/Users/Adam/PycharmProjects/scarec/rediser.py")
sc.addPyFile("/Users/Adam/PycharmProjects/scarec/clustering/clustering_evaluator.py")


class ClusteringEvalRunner:
    OUTPUT_RDD_FILE_PATH = "/Users/Adam/PLISTA_DATASET/cross_validation"

    @classmethod
    def split_dataset_into_folds(cls, input_path, k=5):
        weights = [1 / k] * k
        labeled_points = MLUtils.loadVectors(sc, input_path)
        return labeled_points.randomSplit(weights, SEED)

    @classmethod
    def run_eval(cls, input_path, ks, iters):
        a, b, c, d, e = cls.split_dataset_into_folds("/Users/Adam/PLISTA_DATASET/eval_rdd_labeled_points_300_dense")

        train_data = sc.union([a, b, c, d])
        test_data = e
        #        for k in ks:
        #            for iter in iters:

        k = 10
        iter = 10
        model_path = ClusteringModel.build_model_path(k, iter)
        ClusteringModel.learn_model(model_path, train_data, k, iter)
        evaluator = ClusteringEvaluator(model_path)
        wssse = evaluator.calculate_wssse(test_data)
        redis.set('wssse:' + model_path, wssse)

        print("MODEL " + model_path + "wssse: " + wssse)


# Clu#ClusteringRunner.run_eval([3, 5, 7, 10,  15,20,, 25, 30, 35, 40, 50, 60, 70, 80, 90, 100, 120], [1,5,10], [1, 5, 10])steringRunner.run_eval([3, 5, 7, 10,  15,20,, 25, 30, 35, 40, 50, 60, 70, 80, 90, 100, 120], [1,5,10], [1, 5, 10])

# ClusteringRunner.run_eval([3], [1], [1])


INPUT_PATH = "/Users/Adam/PLISTA_DATASET/clustering_eval/click*"
OUTPUT_RDD_FILE_PATH = "/Users/Adam/PLISTA_DATASET/eval_rdd_labeled_points_300_dense_clicks"


def create_sparse_vector(line):
    dictie = json.loads(line)
    context = Context(dictie)
    hashed_context = context.extract_to_json()
    return ContextEncoder.encode_context_to_sparse_vec(hashed_context)


def create_dense_vector(line):
    dictie = json.loads(line)
    context = Context(dictie)
    hashed_context = context.extract_to_json()
    return ContextEncoder.encode_context_to_dense_vec(hashed_context)


def convert_file_to_sparse_contextual_vectors(input=INPUT_PATH, output=OUTPUT_RDD_FILE_PATH):
    data = sc.textFile(input)
    # data.persist()
    data.map(lambda line: create_sparse_vector(line)).saveAsTextFile(output)


def convert_file_to_dense_contextual_vectors(input=INPUT_PATH, output=OUTPUT_RDD_FILE_PATH):
    data = sc.textFile(input)
    # data.persist()
    data.map(lambda line: create_dense_vector(line)).saveAsTextFile(output)

# convert_file_to_dense_contextual_vectors()

# ClusteringEvalRunner.run_eval("/Users/Adam/PLISTA_DATASET/eval_rdd_labeled_points", 1, 1)
