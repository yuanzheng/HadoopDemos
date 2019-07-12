# Movies Recommender System #

* 此项目我将运用Netflix数据, 给用户推荐他们之前喜欢的电影的相似电影。
* 运用基于物品的协同过滤算法，从 Netflix 的数据中构得到用户对电影的评分矩阵，再得到电影的同现矩阵（也就是电影之间的相似度矩阵）， 最后合并同现矩阵和评分矩阵，得到推荐列表。
* 此项目我们将实现5个Map Reduce Job连接所有的流程, 实现最重要的Map Reduce 版本矩阵相乘。


Sparse matrix我们可以采用稀疏矩阵的存储方式，只存储那些非零的数值。
## References ##
https://www.codeproject.com/Articles/620717/Building-A-Recommendation-Engine-Machine-Learning
http://blog.fens.me/hadoop-mapreduce-matrix/
http://blog.fens.me/hadoop-mapreduce-recommend/