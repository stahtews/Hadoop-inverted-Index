# Hadoop-inverted-Index

The focus of Inverted Index program is to find the relevant document which consists of all the keywords
typed in command line. The strategy is to go through the set of given input files and search for given set
of keywords and return the filename and number of occurances of those keywords. The ultimate goal is,
file which contains all the keywords with highest count makes it to the top of search result.
This strategy of going through each file and searching for set of keywords is tedious and time
consuming. This task can be classified as embarrasingly parallel ,as it involves search operation across
many files. This is a perfect criteria for implementing in Map Reduce technique. The computation is
divided between each mapper instance. Output from each mapper instance is given to Reducer and
eventually to combiner. This strategy of dividing the job among many nodes makes the computation
quicker.
The program consists of 3 classes , Map class , reduce class and main class .There are two important
concepts of Map Reduce. The Data flow and Division of tasks. The program is divided into Map task ,
reduce rask and combine task. Each instance of Map, Reduce class runs on different datanodes. The data
flow is controlled by specifying the arguments type.
Mapper instance on each datanode takes one input file, divides the content into tokens and counts the
number of occurances and saves in a hashmap. The output in the form of < Keyword, filename_Count>
is sent to Reducer class.
Reducer instance on each datanode takes the input sent from mapper and filters tuples based on the
keyword or Key. The values are consolidated based on filename and added to end of output string. This
output string along with Key is sent to combiner class.
Combiner instance takes the input from all Reducer instances and tries to do the computation in the
reducer class but when the input doesn’t meet the standards, it writes the input into output file part-
00000.
This is the strategy followed by MapReduce to distribute the input and computation among several
nodes, which greatly reduces the processing time.
