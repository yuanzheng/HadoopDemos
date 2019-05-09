# Text Prediction #

After a user types a word, it gives the following words for several options. For example:


It adopts the N-Gram Model in this project.
It will do: Based on the first N words, it will give the following N words.


## Coding Plans ##

1. Driver
   * Write output into DataBase
     * 确保 the configuration of MySQL正确
     * 确保 HDFS_path_to_MySQL_connector 正确

2. DBOutputWritable

3. Build N-Gram Library

4. Build Language Model
   * Based on first N words to predict the following word.

5. Build UI for rendering [**TODO**]


## DataBase ##

Install MySQL database locally or AWS/Azure
1. Download and install XAMPP

2. Run MySQL database

3. Create a new database for this project

4. Create a table (starting_phrase, following_word, count)

5. Setup/replace Password (Access from Hadoop remotely by using mysql-connector-java-5.1.39-bin.jar)
   ```shell
   GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'password' WITH GRANT OPTION;
   FLUSH PRIVILEGES;
   ```


## Docker ##

1. Install Docker

2. Download Hadoop Image (from dockerhub)
   * sudo docker pull joway/hadoop-cluster
   * 修改源提高下载速度 [http://www.daocloud.io/mirror](http://www.daocloud.io/mirror)
   * git clone https://github.com/joway/hadoop-cluster-docker
   * sudo docker network create --driver=bridge hadoop 搭建一个bridge供hadoop各节点通信
   
3. Start container
   * /start-hadoop.sh 启动啦，这是有2个 slaves的Hadoop system
   * Docker installation creates a 'src' which is a shared directory of hadoop and local host
   
## Run Text Prediction ##
   1. Download [mysql-connector-java-5.1.39-bin.jar](http://dev.mysql.com/downloads/connector/j/). Move it to 
     'src'
   2. Copy TextPrediction.sh to ~/src
   3. Run TextPrediction.sh (MySQL database should be setup before you run it.)


   
   
