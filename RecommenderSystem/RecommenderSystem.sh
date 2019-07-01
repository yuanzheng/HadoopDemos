#!/bin/bash

# test the hadoop cluster by running RecommenderSystem

# create input directory on HDFS
hadoop fs -mkdir -p movieList

# put input files to HDFS
hdfs dfs -put /root/src/MovieBigDataSet/input movieList

rm /root/src/RecommenderSystem_Output/part-r*

# run Recommender System
hadoop jar /root/src/RecommenderSystem-1.0-jar-with-dependencies.jar movieList/input \
                                                                     DataDivideByUserOutput \
                                                                     coOccurrenceMatrixOutput \
                                                                     normalizationOutput

echo -e "\nTest RecommenderSystem-1.0:"

# print the input files
echo -e "\nUserMovieListOutput:"
hdfs dfs -ls DataDivideByUserOutput
echo -e "\ncoOccurrenceMatrixOutput:"
hdfs dfs -ls coOccurrenceMatrixOutput
echo -e "\nnormalizationOutput:"
hdfs dfs -ls normalizationOutput

# print the output 
echo -e "\nDataDivideByUser directory:"
mkdir /root/src/RecommenderSystem_Output/
hdfs dfs -get DataDivideByUserOutput/part-r-00000 /root/src/RecommenderSystem_Output
mv /root/src/RecommenderSystem_Output/part-r-00000 /root/src/RecommenderSystem_Output/datadividebyuser_1

# print the output 
echo -e "\nCo-Occurrence Matrix directory:"
hdfs dfs -get coOccurrenceMatrixOutput/part-r-00000 /root/src/RecommenderSystem_Output
mv /root/src/RecommenderSystem_Output/part-r-00000 /root/src/RecommenderSystem_Output/coOccurrenceMatrixOutput_2

# print the output 
echo -e "\nNormalization directory:"
hdfs dfs -get normalizationOutput/part-r-00000 /root/src/RecommenderSystem_Output
mv /root/src/RecommenderSystem_Output/part-r-00000 /root/src/RecommenderSystem_Output/normalizationOutput_3


hdfs dfs -rm -r DataDivideByUserOutput/
hdfs dfs -rm -r coOccurrenceMatrixOutput/
hdfs dfs -rm -r normalizationOutput/