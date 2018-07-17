#!/bin/bash
#

# compile scripts
# hadoop com.sun.tools.javac.Main *java
# jar cf PharCount.jar *.class

# remove /output and /out dir on HDFS
hdfs dfs -rm -r /input /temp1 /temp2 /temp3

# create dir input on HDFS
hdfs dfs -mkdir /input

# copy all the data files to hdfs input
hdfs dfs -put ./input/* /input/

# run mapreduce tasks
hadoop jar src/PharCount.jar PrescriberCount /input /temp1

hadoop jar src/PharCount.jar DrugCount /temp1/part-r-00000 /temp2

hadoop jar src/PharCount.jar OrderByCost /temp2/part-r-00000 /temp3

# from dfs copy final raw result
hadoop dfs -copyToLocal /temp3/part-r-00000 ./output

# remove previous result if any
rm output/top_cost_drug.txt

# print out final report as top_cost_drug.txt in output
hadoop jar src/PharCount.jar FileConverter

# path clean
hdfs dfs -rm -r /temp1 /temp2 /temp3
rm output/part-r-00000
