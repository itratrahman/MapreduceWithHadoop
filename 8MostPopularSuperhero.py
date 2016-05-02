####This source code gives an example demonstration of finding the most rated movie by its name
##Here movie title is stored in a different file to the MapReduce file that we sent to the MapReduce job
##So here we have to configure the add file options

##import statements
from mrjob.job import MRJob
from mrjob.step import MRStep

##The MapReduce class which is passed an object of MRJob
class MostPopularSuperHero(MRJob):

	##Function to configure additional options that we want to accept on the command line from the script
    def configure_options(self):
	
		##Setting up configuration
        super(MostPopularSuperHero, self).configure_options()
		
		##Add file option to tell there is another auxillary file that we want to send to the MapReduce job
        self.add_file_option('--names', help='Path to Marvel-names.txt')

	##Function to define the multistep MapReduce jobs
    def steps(self):
        return [
		
			##Mapper and Reducer functions of 1st step of the MapReduce job
            MRStep(mapper=self.mapper_count_friends_per_line,
                   reducer=self.reducer_combine_friends),
				   
			##Reducer functions of 2nd step of the MapReduce job	   
            MRStep(mapper=self.mapper_prep_for_sort,
                   mapper_init=self.load_name_dictionary,
                   reducer = self.reducer_find_max_friends)
        ]

	##Mapper function extracts the information we care about and outputs key value pairs
    def mapper_count_friends_per_line(self, _, line):
	
		##Splting the space delimited data in the line
        fields = line.split()
		
		##Extracting the character id and storing it in approriate variable
        heroID = fields[0]
		
		##Number of friends of the movie character
        numFriends = len(fields) - 1
		
		##Yielding the character id and number of friends as key/value pair
        yield int(heroID), int(numFriends)

	##Reducer function which takes the grouped and sorted key/value paris 
	#and outputs character id and total friends counts 
    def reducer_combine_friends(self, heroID, friendCounts):
	
        yield heroID, sum(friendCounts)

    def mapper_prep_for_sort(self, heroID, friendCounts):
	
		##Extracting the character name corresponding to character id
        heroName = self.heroNames[heroID]
		
		##Yielding None, and a tuple of friendsCounts and character names
        yield None, (friendCounts, heroName)

	##Reducer to find the movie character with the highest count of friends
    def reducer_find_max_friends(self, key, value):
	
        yield max(value)

	##A function to create a dictionary of character id & movie title as key/value pairs 
    def load_name_dictionary(self):
	
		##Creating a dictionary to store the character id & movie title
        self.heroNames = {}

		##Opening the given file
        with open("Marvel-names.txt") as f:
		
			##Iterating through every line in the file
            for line in f:
			
				##Splitting the " delimited data and storing it in the list "fields"
                fields = line.split('"')
				
				##Extracting the character id from the list
                heroID = int(fields[0])
				
				##Setting the 1st and 2nd data of each line as key/value pair of the dictionary
                self.heroNames[heroID] = unicode(fields[1], errors='ignore')

##The main method of the sourcecode			
if __name__ == '__main__':

	##Running the map reduce class above
    MostPopularSuperHero.run()
