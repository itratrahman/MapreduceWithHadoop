####This source code gives an example demonstration of how to word frequncy from a book

##import statements
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

##Regular expression pattern required for matching every line of the file
WORD_REGEXP = re.compile(r"[\w']+")

##The MapReduce class which is passed an object of MRJob
class WordFrequency(MRJob):	

	##Function to define the multistep MapReduce jobs
	def steps(self):
	
		return [
		
			##Mapper and Reducer functions of 1st step of the MapReduce job
			MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
			
			##Mapper and Reducer functions of 2nd step of the MapReduce job
			MRStep(mapper=self.mapper_make_counts_key,
                   reducer = self.reducer_output_words)
		
		]
		
	##Mapper function groups and sorts the words in the file based on their count
	def mapper_get_words(self, _, line):
	
		##Each line in the file is a paragraph
		##Return all non-overlapping matches of pattern in the line
		words = WORD_REGEXP.findall(line) 
		
		##Iterating through every word in the list
		for word in words:
		
			word = unicode(word, "utf-8", errors="ignore") #avoids issues in mrjob 5.0

			##Yielding the word (all letter lowercased), and 1
			yield word.lower(), 1		
		
	##The reducer function outputs the words and their number of occurences
	def reducer_count_words(self, word, values):
	
		##Yielding the word, and its number of occurences
		yield word, sum(values)
	
	##The mapper function groups and sorts the resulting counts from the 1st MapReduce job based on their words
	def mapper_make_counts_key(self, words, count):
	
		##Yielding the count (value rounded to 4 decimal places and padded with leading 0s) and the corresponding list of words
		yield '%04d'%int(count), words
	
	##The reducer function outputs the counts and their correspoonding words
	def reducer_output_words(self, count, words):
	
		##Iterting through the list of words in the word
		for word in words:
		
			##Yielding the count the word
			yield count, word		

##The main method of the sourcecode		
if __name__ == '__main__':

	##Running the map reduce class above
	WordFrequency.run()