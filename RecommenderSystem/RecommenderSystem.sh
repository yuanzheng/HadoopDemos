#!/bin/bash

# test the hadoop cluster by running RecommenderSystem

# create input directory on HDFS
hadoop fs -mkdir -p movieList

# put input files to HDFS
hdfs dfs -put /root/src/MovieBigDataSet/input movieList

rm -r /root/src/RecommenderSystem_Output/

# run Recommender System
hadoop jar /root/src/RecommenderSystem-1.0-jar-with-dependencies.jar movieList/input DataDivideByUserOutput

echo -e "\nTest TextPrediction 3.0-debug-config:"

# print the input files
echo -e "\nUserMovieListOutput:"
hdfs dfs -ls DataDivideByUserOutput

# print the output 
echo -e "\nDataDivideByUser output:"
mkdir /root/src/RecommenderSystem_Output/
hdfs dfs -get DataDivideByUserOutput/part-r-00000 /root/src/RecommenderSystem_Output
#hdfs dfs -rm -r output/