####This source code gives an example demonstration of how to find average frineds by age from a large data set using MapReduce

##import statements
from mrjob.job import MRJob
from mrjob.step import MRStep

##The MapReduce class which is passed an object of MRJob
class TotalSpentByCustomer(MRJob):

	##Function to define the multistep MapReduce jobs
	def steps(self):
	
		return [
		
			##Mapper and Reducer functions of 1st step of the MapReduce job
			MRStep(mapper=self.mapper_get_CustomerAndAmounts,
                   reducer=self.reducer_totalAmounts),
			
			##Mapper and Reducer functions of 2nd step of the MapReduce job
			MRStep(mapper=self.mapper_make_totalOrders_key,
                   reducer = self.reducer_output)
		
		]

	##Mapper function extracts the information and yields customerID and OrderAmount as key/value pairs in an iterator
	def mapper_get_CustomerAndAmounts(self, _, line): 
	
		##Splitting the comma dilimited data in a line and storing them in individual varaibles
		(customerID, Item, OrderAmount) = line.split(',')  
		
		
		##Yielding the Customer ID & list of cutomer's spending
		yield customerID, float(OrderAmount)
		
			
	##The reducer outputs the cutomer id and the amount of money they spent
	def reducer_totalAmounts(self, customerID, orders): 
	
		##Yielding the Cutomer ID and the total amount customer spent
		yield customerID, sum(orders)
	
	##Mapper function extracts the information from the first MapReduce job and yields orderTotal and customerID as key/value pairs in an iterator
	def mapper_make_totalOrders_key(self, customerID, orderTotal): 
	
		##Yieding the Amounts and its corresponding customers
		yield '%04.02d'%int(orderTotal), customerID
	
	##The reducer function uses the grouped and sorted key/value pairs and outputs the cutomer id and their corresponding order
	def reducer_output(self, Order, customerIDs): 

		
		##Iterting through every customer in customerIDs
		for customer in customerIDs:
		
			##Yielding the count the word
			yield customer, Order	
	
##The main method of the sourcecode		
if __name__ == '__main__':

	##Running the map reduce class above
	TotalSpentByCustomer.run()