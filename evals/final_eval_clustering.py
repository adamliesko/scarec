import os
import sys

from pyspark.mllib.clustering import KMeansModel, KMeans
from pyspark.mllib.linalg import Vectors
from clustering.clustering_spark_context import sc
from pyspark.mllib.util import MLUtils
import random

SEED = 13
PRIMES = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101]

ITERATIONS = [1, 2, 3, 5, 7, 10, 15, 20]

sc.addPyFile("/Users/Adam/PycharmProjects/scarec/contextual.zip")
sc.addPyFile("/Users/Adam/PycharmProjects/scarec/rediser.py")
sc.addPyFile("/Users/Adam/PycharmProjects/scarec/clustering/clustering_evaluator.py")
# Path for spark source folder
os.environ['SPARK_HOME'] = "/Users/Adam/scarec/spark-1.6.1-bin-hadoop2.6"

# Append pyspark  to Python Path
sys.path.append("/Users/Adam/scarec/spark-1.6.1-bin-hadoop2.6/python")
os.environ["PYSPARK_PYTHON"] = "/Users/Adam/Py3Env/bin/python"
os.environ['PYTHONPATH'] = '/Users/Adam/scarec/spark-1.6.1-bin-hadoop2.6/'

input_path = "/Users/Adam/PLISTA_DATASET/eval_rdd_labeled_points_300_dense_clicks"

labeled_points = MLUtils.loadVectors(sc, input_path)

k = 19
dim_size = 300
random_centers = [[round(random.random()) for x in range(dim_size)] for y in range(k)]

initialModel = KMeansModel(random_centers)

model = KMeans.train(labeled_points, k, maxIterations=100, runs=100,
                     initialModel=initialModel)

model.save(sc, "/Users/Adam/PycharmProjects/kmeans_model")
