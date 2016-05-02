####This source code gives an example demonstration of how to find average frineds by age from a large data set using MapReduce

##import statements
from mrjob.job import MRJob

##The MapReduce class which is passed an object of MRJob
class AverageFriendsByAge(MRJob):

	##Mapper function extracts the information we care about and outputs key value pairs
	def mapper(self, _, line): 
	
		##Splitting the comma delimited data and storing them into individual variables
		(ID, name, age, numFriends) = line.split(',')  

		##Yielding age, number of friends; 
		#number of friends is casted into a float so that we can perform arithmetice operations on	
		yield age, float(numFriends)
			
	##The reducer function uses the grouped and sorted key/value pairs and outputs the final result
	def reducer(self, age, numFriends): 
	
		##Variable to store the total number of friends
		total = 0
		
		##Variable to store the number of items in the list 
		numElements = 0
		
		##for loop to calculate the average number of friends in a particular age group
		for x in numFriends: 
		
			##Totaling the elements in the list
			total += x
			##incrementing the counter
			numElements += 1
			
		##Yielding age, average number of friends in an age group
		yield age, total/numElements
	
##The main method of the sourcecode		
if __name__ == '__main__':

	##Running the map reduce class above
	AverageFriendsByAge.run()