
import sys
import csv

from pyspark import SparkConf, SparkContext

# create Spark context with Spark configuration
sc = SparkContext("local", "Pyspark App")

# read text file
lines = sc.textFile(sys.argv[1]).cache()

# split each word in the text file
words = lines.flatMap(lambda line: line.split(" "))

# split each word
letters = lines.flatMap(lambda line: line.split(" "))
letters = words.filter(lambda letter: letter.isalpha()) \
             .map(lambda word: word[0].lower())

# count occurance of each letter
letter_pairs = letters.map(lambda letter: (letter, 1))
letter_counts = letter_pairs.reduceByKey(lambda a, b: a + b)
letters_collect = letter_counts.collect()

# count occurance of each word
pairs = words.map(lambda word: (word, 1))
word_counts = pairs.reduceByKey(lambda a, b: a + b)
words_collect = word_counts.collect()

# Save to output text files in the directory
word_counts.saveAsTextFile("words-counts");
letter_counts.saveAsTextFile("letters-counts");

# Save to output csv files in the directory
words_csv = open("words-output.csv", "w")
writer = csv.writer(words_csv, dialect="excel")
writer.writerows(words_collect)
words_csv.close()

letters_csv = open("letters-output.csv", "w")
writer = csv.writer(letters_csv, dialect="excel")
writer.writerows(letters_collect)
letters_csv.close()

# end Spark context
sc.stop()


