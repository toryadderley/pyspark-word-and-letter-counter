import sys
import csv
import re

from pyspark import SparkConf, SparkContext

####################
# HELPER FUNCTIONS #
####################

def getKey(word):
    """
    Used to sort words by first character
    """
    return word[0]

def createCSV(obj, filename):
    """
    Create csv file from data
    """
    file = open(filename + ".csv", "w")
    writer = csv.writer(file, dialect="excel")
    writer.writerows(obj)
    file.close()


################
# MAIN PROGRAM #
################

# Create a SparkContext object with Spark configuration
conf = SparkConf().setAppName("Word-Letter-Counter")
sc = SparkContext(conf=conf)

# Load data from raw text file into RDD
filename = sys.argv[1]
lines = sc.textFile(filename).cache()


# Splits lines in the text file into separate strings of words.
# Filters out non alphabetic characters.
# Makes all alphabetic characters lower case.
words = (
    lines.flatMap(lambda line: re.split("[^a-zA-Z]", line))
    .filter(lambda letter: letter.isalpha())
    .map(lambda word: word.lower())
)

 
# Filters for non alphabetic characters, returns the first letter of every word
letters = words.map(lambda word: word[0])

# Read input and produces a sets of key-value pairs of (letter, 1) and (word, 1)
letter_pairs = letters.map(lambda letter: (letter, 1))
word_pairs = words.map(lambda word: (word, 1))

# Sums every value with the same key to gain the total occurances for each word and letter
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
createCSV(sorted_letter_counts, "letters_output")
createCSV(sorted_word_counts, "words_output")

# End Spark context
sc.stop()
