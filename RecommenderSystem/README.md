# Movies Recommender System #

There're two types of recommendation system:
1. Product Recommendation<br>
	Systems that attempt to predict item that users may be interested in
2. Search Engine<br>
    Systems that help people to find information that may interests them

In this project, I implemented the "Item Collaborative Filtering (Item CF)” algorithm that is one of important algorithms 
in the product recommendation (Users do not have a clear search target). 

Item-CF algorithm is implemented based on 5 MapReduce Jobs that adopt the pipeline of 
1. Building co-occurrence matrix
2. Normalizing co-occurrence matrix
3. Building rating matrix
4. Multiplying co-occurrence matrix and rating matrix
5. Generating recommendation list

This project is designed to process 1GB Netflix movie data to calculate possible rating score.


Sparse matrix我们可以采用稀疏矩阵的存储方式，只存储那些非零的数值。
#### References ####
https://www.codeproject.com/Articles/620717/Building-A-Recommendation-Engine-Machine-Learning
http://blog.fens.me/hadoop-mapreduce-matrix/
http://blog.fens.me/hadoop-mapreduce-recommend/

## Theory ##

有两种推荐算法
* User Collaborative Filtering (User CF)  基于用户的协同过滤
* Item Collaborative Filtering (Item CF) 基于物品的协同过滤

#### User CF ####

基于用户的相似性
1. 基于用户什么点
   * 用户的相似性，例如user's rating!
    
    适合于：新闻推荐，因为新闻数多于用户数

#### Item CF ####

基于物品的相似性进行推荐
1. 比较物 与 物之间的相似性<br>
   例如：User A,B,C 都给 Movie1 打了高分，而且userA 喜欢movie3，所以可以推断userC 也会喜欢movie3。所以给user C 推荐movie3.

    适合用在电影推荐，因为电影数 远远小于 用户数

为什么选item CF?
* The number of users weighs more than number of products
* Item will not change frequently, lowering calculation （Item 的属性是固定的！而人变化性很高的，改变频率高。降低计算频率）
* Using a user’s historical data, more convincing (都是根据本人的历史纪录进行推荐)


How to define relationship between different movies?

Based on movie's info
* Movie category
* Movie producer

不要限制找物品相似性的渠道，也可以把user profile考虑在内
* Watching history
* Rating history
* Favorite list

这里我排除几个可能性：
1. 避免水军
2. 不考虑专业做电影的人

的确会有些噪音，但数据量大就不回造成影响！
多维比较物品相似性

在这个项目中只考虑一维： users' rating history to build relationship between movies.


## Implementation 原理 ##
#### Build co-occurrence matrix ####



#### Normalization(归一化处理) ####


#### Build Rating Matrix ####


#### Recommend Movie for Users ####





### 延伸阅读 ###
#### 推荐算法分类：

按数据使用划分：

* 协同过滤算法：UserCF, ItemCF, ModelCF
* 基于内容的推荐: 用户内容属性和物品内容属性
* 社会化过滤：基于用户的社会网络关系

按模型划分：

* 最近邻模型:基于距离的协同过滤算法
* Latent Factor Mode(SVD)：基于矩阵分解的模型
* Graph：图模型，社会网络图模型

Reference :
https://blog.csdn.net/wenyusuran/article/details/26560723
http://blog.fens.me/hadoop-mapreduce-recommend/