# HadoopDemos
## Big Data &amp; Hadoop Ecosystem


This repository contains the samples that demonstrate several system designs on Hadoop Ecosystem. It contains the usage of MapReduce programming model, MySQL, Hbase and ... [TODO]

Development Environment: Linux + Docker + Hadoop, 基于docker搭建Hadoop集群

### Text Prediction ###
TODO
### Page Rank ###
TODO

**TODO**
应该还有一个project， 暂时还没构思出要以什么方式呈现。

*过去的工作用到Hadoop的项目并不多，学到的、用到的技术通过几个projects方式记录*


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



