from pyspark import SparkContext
import os
import sys


# Path for spark source folder
os.environ['SPARK_HOME']= "/Users/Adam/scarec/spark-1.6.1-bin-hadoop2.6"

# Append pyspark  to Python Path
sys.path.append("/Users/Adam/scarec/spark-1.6.1-bin-hadoop2.6/python")
os.environ["PYSPARK_PYTHON"] = "/Users/Adam/Py3Env/bin/python"
os.environ['PYTHONPATH'] = '/Users/Adam/scarec/spark-1.6.1-bin-hadoop2.6/'


sc = SparkContext(appName="Clustering")
