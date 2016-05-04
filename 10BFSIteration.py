####This sourcecode does the MapReduce job of BFS iteration to find the degree of separation from 
##one from movie character to a chosen movie character

##Import statements
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

##Class node 
class Node:

	##Variable initialization function
	def __init__(self):
	
		##A class variable to store the character Id
		self.characterID = ''
		##A class variable to store the list of connections
        self.connections = []
		##A class variable to store the distance of a Node
        self.distance = 9999
		##A class varaible to store the color of the Node
        self.color = 'WHITE'
	
	##A function to format the data extracted from the line
	#Format is ID|EDGES|DISTANCE|COLOR
	def fromLine(self, line):
	
		##Extracting the pip delimited data from the line
		fields = line.split('|')
		
		##if statement to make sure the length of the field extracted from the line is 4
        if (len(fields) == 4):
		
			##Extracting the characterID from the line
            self.characterID = fields[0]
			##Extracting the comma separated data from the line
            self.connections = fields[1].split(',')
			##Extracting the distance from the line
            self.distance = int(fields[2])
			##Extracting the color from the line
            self.color = fields[3]
	
	
	##Processing the line to in readable format
	def getLine(self):
	
		#Join all the connections stored in the list as comma separated data
        connections = ','.join(self.connections)
		
		##Join the following fields as pip delimited data
        return '|'.join( (self.characterID, connections, str(self.distance), self.color))
	
		

##The MapReduce class
class MRBFSIteration(MRJob):	

	##Input & Output Protocol
	INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

	##Setting up configuration
    def configure_options(self):
        
		##Setting up configuration
		super(MRBFSIteration, self).configure_options()
        
		#Add pass through option to pass a an argument to the entire MapReduce job
		self.add_passthrough_option(
            '--target', help="ID of character we are searching for")
	
	
	def mapper(self, _, line):
	
		##Creating an object of the node class
        node = Node()
		
		##Calling the Node class method fromLine with the line as argument
        node.fromLine(line)
        
		#If the node color is GRAY then node needs to be expanded...
        if (node.color == 'GRAY'):
			
			##Iterating through every connection in the node connections
            for connection in node.connections:
			
				##Passing the object of node by reference
                vnode = Node()
				##Setting the character ID of the node
                vnode.characterID = connection
				##Updating the distance of the node i.e. adding one to the retrieved distance
                vnode.distance = int(node.distance) + 1
                ##Updating the color of the node to GRAY, meaning this node is about to explored
				vnode.color = 'GRAY'
				##If the target passed in the command line matches the current character ID under consideration in current iteration
                if (self.options.target == connection):
                    
					##A varaible to store the useful information of the character id under consideration in a tupl
					counterName = ("Target ID " + connection +
                        " was hit with distance " + str(vnode.distance))
                    
					##Incrementing the MapReduce class's counter
					self.increment_counter('Degrees of Separation',
                        counterName, 1)
						
				##Yielding the character id and the processed line of data for the character id under consideration in an iterator
                yield connection, vnode.getLine()

			#We've processed this node, so color it black
            node.color = 'BLACK'
		
	##The reducer function of the MapReduce job
	def reducer(self, key, values):
			
			##Default value of edges, distance, and color
			edges = []
			distance = 9999
			color = 'WHITE'

			##The for loop iterates through every line of the value
			#and updating the processed line of data based on condition
			for value in values:
				
				##Creating an object of the node class
				node = Node()
				
				##Calling the Node class method fromLine with the line as argument
				node.fromLine(value)
				
				##If the number of associated movie character is more than one, 
				#then the list is initialised to the conncections of the node
				if (len(node.connections) > 0):
					#edges = node.connections
					edges.extend(node.connections)

				##If the distance of node of value under consideration is less than the default distance
				#then the distance is initialised to node distance
				if (node.distance < distance):
					distance = node.distance

				##Color is update based on node.color of the value under consideration
				if ( node.color == 'BLACK' ):
					color = 'BLACK'

				if ( node.color == 'GRAY' and color == 'WHITE' ):
					color = 'GRAY'

			##To make sure every parameter of node is updated if they are not updated previously
			node = Node()
			node.characterID = key
			node.distance = distance
			node.color = color
			#There's a bug in mrjob for Windows where sorting fails
			#with too much data. As a workaround, we're limiting the
			#number of edges to 500 here. You'd remove the [:500] if you
			#were running this for real on a Linux cluster.
			node.connections = edges[:500]
			
			yield key, node.getLine()
	
	
##Main method of the file
if __name__ == '__main__':

	##Running the map reduce class above
	MRBFSIteration.run()
	
