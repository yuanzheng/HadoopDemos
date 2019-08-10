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
                                                                     normalizationOutput \
                                                                     MultiplicationOutput \
                                                                     sumOutput

echo -e "\nTest RecommenderSystem-1.0:"

# print the input files
echo -e "\nUserMovieListOutput:"
hdfs dfs -ls DataDivideByUserOutput
echo -e "\ncoOccurrenceMatrixOutput:"
hdfs dfs -ls coOccurrenceMatrixOutput
echo -e "\nnormalizationOutput:"
hdfs dfs -ls normalizationOutput
echo -e "\nMultiplicationOutput:"
hdfs dfs -ls MultiplicationOutput
echo -e "\nsumOutput:"
hdfs dfs -ls sumOutput

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

# print the output 
echo -e "\nMultiplication directory:"
hdfs dfs -get MultiplicationOutput/part-r-00000 /root/src/RecommenderSystem_Output
mv /root/src/RecommenderSystem_Output/part-r-00000 /root/src/RecommenderSystem_Output/MultiplicationOutput_4

# print the output 
echo -e "\nSum directory:"
hdfs dfs -get sumOutput/part-r-00000 /root/src/RecommenderSystem_Output
mv /root/src/RecommenderSystem_Output/part-r-00000 /root/src/RecommenderSystem_Output/sumOutput_5

hdfs dfs -rm -r DataDivideByUserOutput/
hdfs dfs -rm -r coOccurrenceMatrixOutput/
hdfs dfs -rm -r normalizationOutput/
hdfs dfs -rm -r MultiplicationOutput/
hdfs dfs -rm -r sumOutput/
