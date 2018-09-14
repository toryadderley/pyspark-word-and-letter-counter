
import sys
from pyspark import SparkConf, SparkContext
sc = SparkContext("local", "pysaprk word counts")
lines = sc.textFile(sys.argv[1]).cache()
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
counts = pairs.reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("pg100_counts");
sc.stop()


