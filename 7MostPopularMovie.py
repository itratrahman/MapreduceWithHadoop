####This source code gives an example demonstration of finding the most rated movie by its name
##Here movie title is stored in a different file to the MapReduce file that we sent to the MapReduce job
##So here we have to configure the add file options

##import statements
from mrjob.job import MRJob
from mrjob.step import MRStep

##The MapReduce class which is passed an object of MRJob
class MostPopularMovie(MRJob):

	##Function to configure additional options that we want to accept on the command line from the script
    def configure_options(self):
	
		##Setting up configuration
        super(MostPopularMovie, self).configure_options()
		
		##Add file option to tell there is another auxillary file that we want to send to the MapReduce job
        self.add_file_option('--items', help='Path to u.item')

	##Function to define the multistep MapReduce jobs
    def steps(self):

        return [
		
			##Mapper and Reducer functions of 1st step of the MapReduce job
            MRStep(mapper=self.mapper_get_ratings,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer_count_ratings),
				   
			##Reducer functions of 2nd step of the MapReduce job	   
            MRStep(mapper = self.mapper_passthrough,
                   reducer = self.reducer_find_max)
        ]

	##Mapper function extracts the information we care about and outputs key value pairs
    def mapper_get_ratings(self, _, line):
	
		##Splitting the tab delimited data and storing them into individual variables
		(userID, movieID, rating, timestamp) = line.split('\t')
		##Yielding rating and 1
		yield movieID, 1

	##The reducer function creates a dictionary of movie id & movie title as key/value pairs 
    def reducer_init(self):
	
		##Creating a dictionary to key/value pairs of movie id & movie title
        self.movieNames = {}

		##Opeining the file u.ITEM stored in variable f
        with open("u.ITEM") as f:
		
			##iterating through every line of the file
            for line in f:
			
				##Splitting the pip delimited data in the line and store in the variable fields
                fields = line.split('|')
				
				##Setting the 1st and 2nd data of each line as key/value pair of the dictionary
                self.movieNames[fields[0]] = fields[1].decode('utf-8', 'ignore')

	##The reducer function uses the grouped and sorted key/value pairs and outputs the final result
    def reducer_count_ratings(self, key, values):
		
		##Yielding None as key and a tuple of number of ratings of the movie and its movie title retrieved from the dictionary movieNames
        yield None, (sum(values), self.movieNames[key])

	#This mapper does nothing; it's just here to avoid a bug in some
	#versions of mrjob related to "non-script steps." Normally this
	#wouldn't be needed.
    def mapper_passthrough(self, key, value):
        
		yield key, value

	##The reducer function uses the grouped and sorted key/value pairs and outputs the final result
    def reducer_find_max(self, key, values):
	
		##Yielding None as key and a tuple of number of ratings of the movie and its movie title retrieved from the dictionary movieNames
        yield max(values)

##The main method of the sourcecode			
if __name__ == '__main__':
	
	##Running the map reduce class above
    MostPopularMovie.run()