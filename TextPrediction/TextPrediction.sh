#!/bin/bash

# test the hadoop cluster by running TextPrediction(NGram)

# create input directory on HDFS
hadoop fs -mkdir -p bookList

# put input files to HDFS
hdfs dfs -put /root/src/bookList/TheBacchaeofEuripides.txt bookList

# create a directory on HDFS for mysql connector jar
hadoop fs -mkdir -p /mysql

hadoop fs -put /root/src/mysql-connector-java-5.1.39-bin.jar /mysql

rm -r /root/src/TextPrediction_Output/

# run wordcount
hadoop jar /root/src/TextPrediction-3.0-jar-with-dependencies.jar bookList output 3 3 4


#echo -e "\nTest TextPrediction 3.0:"
echo -e "\nTest TextPrediction 3.0:"

# print the input files
echo -e "\nbookList:"
hdfs dfs -ls bookList

# print the output of nGram library
echo -e "\nNGram Library output:"
mkdir /root/src/TextPrediction_Output/
hdfs dfs -get output/part-r-00000 /root/src/TextPrediction_Output
hdfs dfs -rm -r output/