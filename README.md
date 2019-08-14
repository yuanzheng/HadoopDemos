# HadoopDemos
## Big Data &amp; Hadoop Ecosystem

This repository contains the samples that demonstrate several system designs on Hadoop Ecosystem. It contains the usage of MapReduce programming model, MySQL, Hbase and ... [TODO]

Development Environment: Linux + Docker + Hadoop, build up Docker containers for Hadoop clusters.

### Text Prediction ###
Implement n-gram model to predict following possible phrases for the current input. (Check TextPrediction directory for details)

### Page Rank ###
Based on the basic theory behind PageRank algorithm, I implemented PageRank with MapReduce.

### Recommender System ###
This project applies "Item Collaborative Filtering (Item CF) algorithm on Netflix data to calculate possible rating score on which user may rates.

### Running and Testing Environment ###
MacOS 10.14 + Docker + Hadoop 2.7.2 + MySQL
Memory allocated to docker is 6GB
4 CPUs
 
One master container and two slave containers

### Unit Test ###
Apache MRUnit ™ is chosen to build unit test for Hadoop map reduce jobs. For running unit test, we use junit. The hadoop-minicluster library contains the “mini-” clusters that are for testing with Hadoop clusters running in a single JVM.

Here, in MRUnit test artifacts, using Classifier as hadoop2 has been chosen, because testing and running environment of all projects are in Hadoop 2 version.

### 基于docker搭建Hadoop集群 ###
从dockerhub上下载用于搭建集群的镜像

1. Pull Docker Image
   ```Bash
   docker pull joway/hadoop-cluster
   ```
   或者从github上拉取相关代码：git clone https://github.com/joway/hadoop-cluster-docker
   
   在这里，有很大的可能Docker hub被墙。解决方案如下：
   用DaoCloud的镜像加速。https://www.daocloud.io/mirror, 按照DaoCloud的教程来添加镜像至Docker，然后重启Docker，重新pull镜像。
   如果你有VPN，开启VPN之后便可以解决此问题

   
2. Create Hadoop Network
   ```Bash
   docker network create --driver=bridge hadoop #搭建一个bridge供hadoop各节点通信
   ```
   
3. Start Container
   ```Bash
   sudo ./start-container.sh  #执行这条命令后就是进入了 docker 容器
   ```
   
   It starts 3 containers with 1 master and 2 slaves. After these steps, we get into the /root directory of hadoop-master container.
   
4. Start Hadoop
   ```Bash
   ./start-hadoop.sh  #在docker容器里启动hadoop
   ```
   
5. Sync Source folder
   我们从本地上传可执行文件，或需要被处理的文件，就需要一个Sync folder
   本地目录: ~/src/
   hadoop master 目录 : /root/src/
    
   将自己的代码copy到本地目录 src 中, 但凡在该目录下的任何操作都会自动实时映射到 hadoop master 容器中的/root/src/中。之后只要在本地~/src/中修改编辑代码, 在容器内执行命令即可。



