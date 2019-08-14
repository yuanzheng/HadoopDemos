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
共生矩阵：A co-occurrence matrix is a matrix that is defined over an image to be the distribution of co-occurring pixel
 values (grayscale values, or colors) at a given offset.
 
要表现物品的相似性，同时是两两相似性，matrix就是一种方法（in graph）。多少用户同时rate当前电影的这种方法来表示当前这两部电影的相似性是多少！


|   User   |    M1     |    M2     |    M3     |    M4     |    M5    |
|   ----   |   ----    |   ----    |   ----    |   ----    |   ----   |
|   A      |     1     |     1     |   ----    |     1     |   ----   |
|   B      |     1     |     1     |     1     |   ----    |   ----   |
|   C      |   ----    |     1     |     1     |   ----    |     1    |
|   D      |   ----    |     1     |   ----    |     1     |   ----   |


同时被多少人rate过，就可以说2部电影的关系是什么。

共生矩阵 -- 表示相同的人打过分

#### Normalization(归一化处理) ####
（通过归一化将属性数据按照比例缩放，这样就可以将数值落入一个特定的区间内）



M 代表 Movie.
思考：根据上图，（M1，M2）=2， （M2，M1）=2 这两个2的分量一样吗？
对M1而言，与M1、M2、M3、M4 有关联性
对M2而言，与M1、M2、M3、M4 、M5 有关联性
抛开相对值，而只考虑绝对值是不对的，要在关系网中整体讨论！！！
怎样引入M1 与 其他所有电影的关系网呢？
M1与其他所有的电影关系总和是 2 + 2 + 1 + 1 = 6 ，所以对M1而言M2与他关系是6分关系网中的2份，即分量是2/6。M1与M2 的关系中，在他寥寥无几的关联性当中，比较重要的了

M2 与其他所有的电影关系总和是 2 + 4 + 2 + 2 + 1 = 11 ，所以在这11个关系里M2与 M1 的关系是 11个关系中的2分，即分量是 2/11（所以对于M2来说，它与M1 的关联性在的关系网中就不是那么重要了）

(TODO graph)

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