# Page Rank #

PageRank is an algorithm used by Google Search to rank websites in their search engine results.

This demo is based on the basic theory behind PageRank:

- More important websites are likely to receive more links from other websites.
- Website with higher PageRank will pass higher weight.

And it includes the multiplication of two large matrices with MapReduce.


如何做网页评分/投票?

* 数量假设：More important websites are likely to receive more links from other websites
* 质量假设：Website with higher PageRank will pass higher weight

<b>数量假设</b>,当前的网站的ULR 出现在很多其他网页上（接收其他网站的跳转！和论文的 references一样的道理）<br>

<b>质量假设</b>，如果一个网站的质量很高（权重高，知名度高），通过这个网站去到的另一个网站（例如，权重低的）。就会把这个权重低的网站的质量提升起来。


### 数量假设 (从网页跳转) ###

Step 1: Build a Transition Matrix (一定是N x N，因为A->B,也可以B->A )

(转移矩阵：矩阵各元素都是非负的，并且各行元素之和等于1，各元素用概率表示，在一定条件下是互相转移的，故称为转移概率矩阵。如用于市场决策时，矩阵中的元素是市场或顾客的保留、获得或失去的概率。P^(k)表示k步转移矩阵。)


测试数据来源
http://www.limfinity.com/ir/

Reference pagers:

- [谷歌背后的数学](https://www.changhai.org/articles/technology/misc/google_math.php)
- [The Anatomy of a Large-Scale Hypertextual Web Search Engine](http://infolab.stanford.edu/~backrub/google.html)
- [The PageRank Citation Ranking: Bringing Order to the Web](http://ilpubs.stanford.edu:8090/422/1/1999-66.pdf)
- [Convergence Analysis of an Improved PageRank Algorithm](https://projects.ncsu.edu/crsc/reports/ftp/pdf/crsc-tr04-02.pdf) 
