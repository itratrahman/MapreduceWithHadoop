####This source code preprocesses the data extracted from the file "Marvel-graph.txt",
##and output the processed data in the File "BFS-iteration-0.txt"

# Call this with one argument: the character ID you are starting from.
# For example, Spider Man is 5306, The Hulk is 2548. Refer to Marvel-names.txt
# for others.

##Import statement
import sys

print 'Creating BFS starting input for character ' + sys.argv[1]

##Creating and Opening a file for writing the processed data
with open("BFS-iteration-0.txt", 'w') as out:

	##Opening the file for reading, extracting the relevant data for processing
    with open("Marvel-graph.txt") as f:

		##Iterating trought every line in the file "Marvel-graph.txt"
        for line in f:
		
			##Splitting the space delimited data in the file
            fields = line.split()
			
			##Extracting the movie character which is the first field in the line
            heroID = fields[0]
			
			##Number of associated connections of the movie character
            numConnections = len(fields) - 1
			
			##A list of chracter ID of the associated connections
            connections = fields[-numConnections:]

			##Default value of color and distance if the heroID 
			#is not that of movie character under consideration in the current iteration
            color = 'WHITE'
            distance = 9999

			##if the heroID matches the movie chracter under consideration in the current iteration
			#then assign a color of GRAY and a distance of 0
            if (heroID == sys.argv[1]) :
                color = 'GRAY'
                distance = 0

			##If statement to check if the heroID is wrongfully extracted as a space character
            if (heroID != ''):
			
				##Join all the connections stored in the list as comma separated data
                edges = ','.join(connections)
				
				##Join the following fields as pip delimited data
                outStr = '|'.join((heroID, edges, str(distance), color))
				
				##Writing the string in the file "BFS-iteration-0.txt"
                out.write(outStr)
				
				##Writing a new line character at the end of the line
                out.write("\n")

	##Closing the file "Marvel-graph.txt"
    f.close()
	
##Closing the file "BFS-iteration-0.txt"
out.close()
