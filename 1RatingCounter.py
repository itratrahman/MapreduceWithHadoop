####This source code gives an example demonstration of finding the most rated movie

##import statements
from mrjob.job import MRJob
from mrjob.step import MRStep

##The MapReduce class which is passed an object of MRJob
class MRRatingCounter(MRJob):

	##Function to define the multistep MapReduce jobs
	def steps(self):
	
		##Function to define the multistep MapReduce jobs
		return [
		
			##Mapper and Reducer functions of 1st step of the MapReduce job
			MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
			
			##Reducer functions of 2nd step of the MapReduce job
			MRStep(mapper=self.mapper_passthrough,
                   reducer = self.reducer_find_max)
		
		]

	##Mapper function extracts the information we care about and outputs key value pairs
	def mapper_get_ratings(self, _, line): 
	
		##Splitting the tab delimited data and storing them into individual variables
		(userID, movieID, rating, timestamp) = line.split('\t')  

		##Yielding rating and 1
		yield movieID, 1
			
	##The reducer function uses the grouped and sorted key/value pairs and outputs the final result
	def reducer_count_ratings(self, movieID, counts): 
	
		##Yielding None as key and a tuple of number of ratings of the movie and its movie ID
		yield None, (sum(counts), movieID)
		
	def mapper_passthrough(self, key, value):
        
		yield key, value

	##The reducer yields the most popular movie id and the number of times it is rated
	def reducer_find_max(self, key, values):
	
		yield max(values)
	
##The main method of the sourcecode				
if __name__ == '__main__':

	##Running the map reduce class above
	MRRatingCounter.run()