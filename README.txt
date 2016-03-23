
## Prerequisites
+ JDK 1.8 
+ JRE 1.8 
+ Apache Hadoop 2.7.2
## Before we start
Register yourself as one of the hadoop group user and proceed with the following commands,
Make sure hadoop is running before you proceed with the commands
#### Task0 - Merging the given files: 
~~~~
hadoop jar Table0.jar org.Table0 /input0 /output0 
~~~~
###### WARNING : input directory which has all three files(name sensitive) and output directory for Task0
Output format : 
<artist_name><SEP><artist_id><I>.....<artist_id><SEP><location><I>......<location><SEP><song><I>......<song>

#### Task1 - Consolidation of locations, artists and songs: 
~~~~
hadoop jar Table1.jar org.Table1 /output0 /output1 
~~~~
###### WARNING : output directory from Task0  and output directory for Task1
Output format : 
<location><SEP><artist_name1><SEP><artist_name2>................
#### Task2 - Analysis on the dataset :
hadoop jar Table2.jar org.Table2 /output1      /output0              /output2 
				      Task1O/P    Task0 O/P        Task2O/P
Output format : 
<artistname><SEP><artist_id><I>.....<artist_id><SEP><location><I>......<location><SEP><song><I>......<song>

###### Execute the above commands in a sequence and you get the output in the specified formats.
#### Assumptions Made : 
+ There can be multiple locations, artistids and songtitles available, so included them all while I was merging in Task0
+ Task2 has three command line arguments, i.e. args[0] will be Task1's output, args[1] will be Task0's output and args[2] will be the result Task2's output.
+ Location & Artist Names are captured by Regular expression.

#### Bugs : 
Minor bugs are there (rare test cases that are not provided in the input datasets), apart from that I have all the Tasks working properly! 
###### Note : If you want to change the input files (Short files of your own ? ) then you may have to change the code or overwrite the files already there under input0/
#### References: 
1. http://stackoverflow.com/questions/2072222/regular-expression-for-positive-and-a-negative-decimal-value-in-java
2. http://regexr.com/
3. Hadoop Essentials Book
4. Hadoop WordCount program – Boilerplate https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html


##### Copyrights (C) Vaikunth Sridharan