####This source code gives an example demonstration of how to max temperature from a corpus containing data of location, date, min/max temp, and temperature

##import statements
from mrjob.job import MRJob

##The MapReduce class which is passed an object of MRJob
class MRMaxTemperature(MRJob):

	##A function to convert the tenth of Celcius data to Fahrenheit
	def MakeFahrenheit(self, tenthsOfCelcius):
	
		celsius = float(tenthsOfCelcius)/10.0
		
		fahrenheit = celsius*1.8 + 32.0
		
		return fahrenheit
		
	##Mapper function extracts the information yeilds location and temperature as key/value pairs in an iterator
	def mapper(self, _, line):
	
		##Splitting the comma delimited data and storing them into individual variables
		(location, date, type, data, x, y, z, w) = line.split(',')

		##Exctract the key/value pair of the line under consideration only if the 'type' is 'TMIN'
		if (type == 'TMAX'):
		
			##Convert the temperature from tenth of Celcius to Fahrenheit
			temperature = self.MakeFahrenheit(data)
			
			##Yielding the location and the temperature (two values)
			yield location, temperature
		
	##The reducer function uses the grouped and sorted key/value pairs and outputs location and its corresponding max temperature
	def reducer(self, location, temps):
	
		##Yielding the location and the minimum of all the temperatures in the list
		yield location, max(temps)

##The main method of the sourcecode		
if __name__ == '__main__':

	##Running the map reduce class above
	MRMaxTemperature.run()