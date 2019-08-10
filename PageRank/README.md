# Page Rank #

PageRank is an algorithm used by Google Search to rank websites in their search engine results.

This demo is based on the basic theory behind PageRank:

- More important websites are likely to receive more links from other websites.
- Website with higher PageRank will pass higher weight.

And it includes the multiplication of two large matrices with MapReduce.

测试数据来源
http://www.limfinity.com/ir/

Reference pagers:

- [谷歌背后的数学](https://www.changhai.org/articles/technology/misc/google_math.php)
- [The Anatomy of a Large-Scale Hypertextual Web Search Engine](http://infolab.stanford.edu/~backrub/google.html)
- [The PageRank Citation Ranking: Bringing Order to the Web](http://ilpubs.stanford.edu:8090/422/1/1999-66.pdf)
- [Convergence Analysis of an Improved PageRank Algorithm](https://projects.ncsu.edu/crsc/reports/ftp/pdf/crsc-tr04-02.pdf) 

## 本地 Build and Run ##

这是JAVA maven project, 需要创建 JAR 在 Hadoop 中运行。pom.xml 不需要修改，在Intellij中按照下面步骤创建JAR:


  * Click tool bar: "maven projects"
    * Expand jar-building-using-maven, Lifecycle
	    * Clean
		* Install
	    * All should success
    * Build a JAR file using Maven assembly plugin
		* Double click “assembly:assembly”
		* Build success
	* JAR file is in the target folder

在target下可以找到 jar。

因为在创建hadoop master container时，我们在Docker image和宿主机上做了映射，只要 JAR 放在宿主机~/src目录下，
进入Docker后就会自动映射到Docker中的root/src目录下

请按照首页中Docker 和 Hadoop的安装、运行说明将Hadoop 运行起来。

为了简化在hadoop中的操作，我建了一个Shell script 一系列运行操作。请把PageRank.sh复制到宿主机~/src目录下，在hadoop。


## Theory ##
如何做网页评分/投票?

* 数量假设：More important websites are likely to receive more links from other websites
* 质量假设：Website with higher PageRank will pass higher weight

<b>数量假设</b>,当前的网站的ULR 出现在很多其他网页上（接收其他网站的跳转！和论文的 references一样的道理）<br>

<b>质量假设</b>，如果一个网站的质量很高（权重高，知名度高），通过这个网站去到的另一个网站（例如，权重低的）。就会把这个权重低的网站的质量提升起来。


### 数量假设 (从网页跳转) ###

Build a Transition Matrix (一定是N x N，因为A->B,也可以B->A )

(转移矩阵：矩阵各元素都是非负的，并且各行元素之和等于1，各元素用概率表示，在一定条件下是互相转移的，故称为转移概率矩阵。如用于市场决策时，矩阵中的元素是市场或顾客的保留、获得或失去的概率。P^(k)表示k步转移矩阵。)

初始化值的设定我是按平均分配的方式，例如：

A -> B, C, D 概率是1/3 <br>
B -> A, D    概率是1/2 <br>
C -> A       概率是1   <br>
D -> B, C    概率是1/2 <br>

里是将page rank 工程简化，因为目前无法做到Google 团队的那些额外的工作。所以，这里只能默认都给统一值，均等的（不是主观判断！！！）。

### 质量假设（权重转移）

WebSite with higher pagerank will pass higher weight! However，How to represent the importance of each website? 

需要考虑每个网页的重要性！

注意不要被 第三方 操纵！要公平 ！ 给一个初始值，计算一轮后就有的网页重要性值了。

初始值如何给？

这里给的是均等值（1/4，1，或者是100都可以，只要ABCD… 网页间的相对值一样就可以），整个PageRank的计算最终结果是收敛的。
（数学证明：一个矩阵几次迭代之后值是收敛的，所以初始值不会对最终结果产生影响的。）


### How to calculate PR1 ###

Transition matrix 乘以 PR0 = PR1

例如：

B -> A 从网页B 有1/2的可能性跳转到 网页A （概率有 1/2）。网页 B 的重要性（权重）是 1/4，意味着在从网页 B 跳转的时候，会有1/4的重要性转移。

这个公式把 “数量假设”和 “质量假设”结合在一起了：


    PR2 = Transition Matrix * PR1
    …
    PRn = Transition Matrix * PR(n-1)

（注意： Transition Matrix 是不会发生变化的，PR matrix才是变化的！所以，在MapReduce 中的Mapper，在Transition Matrix 
每次读入的数据都一样，这是无法避免的！）

但，并不是无限的循环下去！
参考：https://www.ncsu.edu/crsc/reports/ftp/pdf/crsc-tr04-02.pdf

两种趋势：
重要的网页的PRn值趋于1，相反PRn趋于0。

很快就分出来了！

论文中总结出 迭代30 ～ 50次就可以了（最多50次）！


PRn = Transition Matrix * PR(n-1)，要求是一定可以从当前网页跳转到其他网页上去！

那么有没有那种情况 这个公式不适用呢！请看下面的edge cases:


### Edge Cases ###

1. 建很多网站，都链接到同一个网页！那么这个网页被跳转的次数很多。（Google 有专门的antispam 团队在做）

    * 不可以做到1，要求这些众多被建立的网站也在搜索范围内！（与这个同一个链接的网页是 同一个搜索范围内呀，例如都是关于bigdata）
    * 不可以做到2, 如果这些众多被建立的网站的权重都很低的话，最终的权重转移也是非常小的。
	* 不可以做到3，用户的feedback很低（例如 这些众多网站的用户跳转率很低，或者跳转后的停留时间很短）
	
    根据这些反馈，也可以判断模型的好坏（给用户的推荐，用户点击率很高，停留时间很长。那么模型很好）
	
2. Dead ends，某个网站是个断点（哪都不指向）
















