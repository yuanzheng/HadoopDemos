#!/bin/bash

# test the hadoop cluster by running PageRank (ignore Edge cases)

# create input directory on HDFS
hadoop fs -mkdir -p /transition

# put input files to HDFS
hdfs dfs -put /root/src/PageRankData/transitionsmall.txt /transition

# create a directory on HDFS for init PR matrix and 
hdfs dfs -rm -r /output*

hdfs dfs -rm -r /pagerank*

hdfs dfs -mkdir /pagerank0

hdfs dfs -put /root/src/PageRankData/prsmall.txt /pagerank0

rm -r /root/src/PageRank_Output/

i=3
# run jar
hadoop jar /root/src/PageRank-1.0-jar-with-dependencies.jar /transition /pagerank /output $i

echo -e "\nTest PageRank-1.0:"

# print the output files
echo -e "\npagerank1:"
hdfs dfs -ls /pagerank$i
hdfs dfs -cat /pagerank$i/*

# print the output of PageRank weight
echo -e "\nPage Rank output:"
mkdir /root/src/PageRank_Output/
hdfs dfs -get /pagerank$i/* /root/src/PageRank_Output


