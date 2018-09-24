import sys
import csv
import re

from pyspark import SparkConf, SparkContext


# Helper Function
def getKey(item):
    return item[0]


################
# MAIN PROGRAM #
################

# Create a SparkContext object with Spark configuration
conf = SparkConf().setAppName("Word-Letter-Counter")
sc = SparkContext(conf=conf)

# Read input text file
lines = sc.textFile(sys.argv[1]).cache()

# Splits lines in the text file into separate strings of words, containing only alphabetic characters
# Makes the all charaters lower case
words = lines.flatMap(lambda line: re.split('[^a-zA-Z]', line)) \
    .map(lambda word: word.lower())

# Filters for non alphabetic characters, returns the first letter of every word
letters = words.filter(lambda letter: letter.isalpha()) \
    .map(lambda word: word[0])

# Read input and produces a set of key-value pairs
# And groups every pair with the same key
letter_pairs = letters.map(lambda letter: (letter, 1))
word_pairs = words.map(lambda word: (word, 1))

# Sums every value with the same key and outputs total amount of values for a single key
letter_counts = letter_pairs.reduceByKey(lambda a, b: a + b)
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

# Makes a sorted list of the occurances of each word and letter
letters_collect = letter_counts.collect()
sorted_letter_counts = sorted(letters_collect)
words_collect = word_counts.collect()
sorted_word_counts = sorted(words_collect, key=getKey)

# Save to output text files in the directory
word_counts.saveAsTextFile("words-ouput")
letter_counts.saveAsTextFile("letters-output")

# Save to output csv files in the directory
words_csv = open("words-output.csv", "w")
writer = csv.writer(words_csv, dialect="excel")
writer.writerows(sorted_word_counts)
words_csv.close()

letters_csv = open("letters-output.csv", "w")
writer = csv.writer(letters_csv, dialect="excel")
writer.writerows(sorted_letter_counts)
letters_csv.close()

# End Spark context
sc.stop()
