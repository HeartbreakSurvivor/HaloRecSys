package content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

/**
 * @program: HaloRecSys
 * @description: ${Use TF-IDF algorithm for movie recommendation}
 * @author: HaloZhang
 * @create: 2021-04-30 14:50
 **/

/*
* 电影数据样例类
* 	movieId,title,genres
* */
case class Movie(mid:Int, name:String, genres:String)

/*
* 定义MongoDB数据库相关配置信息样例类
* */
case class MongoConfig(uri:String, db:String)

//定义基础的推荐数据结构
case class Recommendation(mid: Int, score: Double)

//基于电影内容提取出的特征向量的电影相似度列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object ContentRecommender {
	val MONGODB_MOVIE_COLLECTION = "Movie"
	// 基于TF-IDF算法得到的电影推荐内容存储表名称
	val TFIDF_MOVIE_RECS = "ContentMovieRecs"
	// 电影相似度阈值
	val MOVIE_SIM_THRESHOLD = 0.6

	def main(args: Array[String]): Unit = {
		val config = Map(
			"spark.cores" -> "local[*]",
			//数据库url
			"mongo.uri" -> "mongodb://localhost:27017/recommender",
			//数据库名称
			"mongo.db" -> "recommender"
		)

		val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender")

		val spark = SparkSession.builder().config(sparkConf).getOrCreate()

		// 隐式转换相关的包
		import spark.implicits._

		//创建MongoDB配置
		implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

		// 加载电影数据并预处理

		val movieTagsDF = spark.read
		  .option("uri", mongoConfig.uri)
		  .option("collection", MONGODB_MOVIE_COLLECTION)
		  .format("com.mongodb.spark.sql")
		  .load()
		  .as[Movie]
		  .map(
			  x => (x.mid, x.name, x.genres.map( c => if(c == '|') ' ' else c)) // 这里将类别的分隔符 | 换成 空格
		  )
		  .toDF("mid", "name", "genres") //设置DF的列名
		  .cache() //缓存

		// TODO: 使用TF-IDF算法从电影信息中提取特征向量
		val movieFeats = null

		//定义分词器
		val tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")
		// 用分词器对原始数据做转化，转换成bag of word 模型
		val wordsData = tokenizer.transform(movieTagsDF)
		wordsData.printSchema()
		wordsData.show(5, truncate = false)

		// 引入HashingTF工具，可以把一个词语序列转换成对应的词频
		val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
		val featurizedData = hashingTF.transform(wordsData)
		featurizedData.show(5, truncate = false)

		// 引入IDF模型
		val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
		// 训练IDF模型，得到每个词的逆文档频率
		val idfModel = idf.fit(featurizedData)
		// 用模型对元数据进行处理，得到文档中每个词的TF-IDF，作为新的特征向量
		val rescaledData = idfModel.transform(featurizedData)
		rescaledData.show(5, truncate = false)

		val movieFeatures = rescaledData.map(
			row => (row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray ))
			  .rdd
			  .map(
				  x => (x._1, new DoubleMatrix(x._2))
			  )
		// movieFeatures.collect().foreach(println)

		// 所有电影特征向量之间做笛卡尔积
		val movieRecs = movieFeatures.cartesian(movieFeatures)
		  .filter{
			  case (a,b) => a._1 != b._1 //筛选出自己与自己配对的情况，因为这样算相似度是为1的。
		  }
		  .map{
			  case (a, b) => {
				  val simScore = this.consinSim(a._2, b._2) //计算两个向量之间的余弦相似度
				  (a._1, (b._1, simScore)) // 即 movieA和movieB的相似度值
			  }
		  }
		  .filter(_._2._2 > MOVIE_SIM_THRESHOLD)
		  .groupByKey() //按照mid进行分组
		  .map{
			  case (mid, items) => MovieRecs(mid, items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
		  }
		  .toDF()
		//将电影相似度矩阵写入MongDB中
		movieRecs.write
		  .option("uri", mongoConfig.uri)
		  .option("collection", TFIDF_MOVIE_RECS)
		  .mode("overwrite")
		  .format("com.mongodb.spark.sql")
		  .save()

		spark.stop()
	}

	def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
		movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
	}
}
