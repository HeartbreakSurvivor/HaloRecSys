package src.main.scala.embedding

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/*
* 定义MongoDB数据库相关配置信息样例类
* */
case class MongoConfig(uri:String, db:String)

object Embedding {
	// 获取用户评分文件路径
	val rawRatingDataPath = this.getClass().getResource("/MovieLens/ratings.csv").getPath
	// Embedding 向量的维度
	val EMBEDDING_DIM = 10
	// 选取窗口大小，表示当前词与预测词在一个句子中的最大距离
	val WINDOW_SIZE = 5
	// 训练迭代次数
	val NUMBER_ITER = 1

	//Mongo 表
	// 定义word2vec模型计算出来的电影embedding向量
	val MONGODB_MOVIE_EMBEDDING = "Word2Vec_Movie_Embedding"
	val MONGODB_USER_EMBEDDING = "Word2Vec_User_Embedding"

	val GLOBAL_CONFIG = Map(
		"spark.cores" -> "local[*]",
		//数据库url
		"mongo.uri" -> "mongodb://localhost:27017/recommender",
		//数据库名称
		"mongo.db" -> "recommender"
	)

	/**
	 * @Description: process user ratings history to item sequence
	 * @Param: [sparkSession, ratingsPath]
	 * @return: org.apache.spark.rdd.RDD<scala.collection.Seq<java.lang.String>>
	 * @Author: Halo.Zhang
	 * @Date: 4/27/21
	 */
	 def processItemSequence(sparkSession: SparkSession, ratingsPath: String): RDD[Seq[String]] = {
		 val ratingSamples = sparkSession.read.format("csv").option("header", "true").load(ratingsPath)

		 ratingSamples.printSchema()
		 // 定义UDF，实现按照时间戳排序
		 val sortUdf: UserDefinedFunction = udf( (rows: Seq[Row]) => {
			 rows.map{
				 case Row(movieId: String, timestamp: String) => (movieId, timestamp)
			 }.sortBy{
				 case (_, timestamp) => timestamp //根据时间戳来排序
			 }.map {
				 case (movieId, _) => movieId //最后只使用了movieId这个数据
			 }
		 })

		 //process rating data then generate user watched movie sequence
		 val userSeq = ratingSamples
		   .where(col("rating") >= 3.5)
		   .groupBy("userId") // 根据用户ID进行聚合
		   .agg(sortUdf(collect_list(struct("movieId", "timestamp"))) as "movieIds") //这里是经过GroupBy之后，将userId对应的元素转换成一个list
		   .withColumn("movieIdStr", array_join(col("movieIds"), " ")) //将用户观影记录通过空格分隔，转换成一个字符串

		 userSeq.printSchema()
		 println("show the first 5 userSeq items")
		 userSeq.show(5, truncate = false)
		 // 将用户观影序列转换成一个字符串
		 userSeq.select(col="movieIdStr").rdd.map(r => r.getAs[String]("movieIdStr").split(" ").toSeq)
	}

	/***
	* @Description:  使用word2vec模型训练用户的观影序列数据，产生电影的embedding向量
	* @Param: [sc, samples]
	* @return: org.apache.spark.mllib.feature.Word2VecModel
	* @Author: Halo.Zhang
	* @Date: 4/27/21
	*/
	def trainItem2vec(spark: SparkSession, samples: RDD[Seq[String]]): (DataFrame, Word2VecModel) = {
		// 创建word2vec模型
		val word2vec = new Word2Vec()
		  .setVectorSize(EMBEDDING_DIM)
		  .setWindowSize(WINDOW_SIZE)
		  .setNumIterations(NUMBER_ITER)

		// 训练样本
		val model = word2vec.fit(samples)

//		// 查找一个单词的同义词，范围大小定义为20， 这里是158号电影
//		val synonyms = model.findSynonyms("158", 20)
//		println("同义词 余弦相似度")
//		for ((synonym, cosineSimilarity) <- synonyms) {
//			println(s"$synonym $cosineSimilarity")
//		}

		// 创建Movie RDD
		// 格式：
		//  movieId | embedding
		val movieEmb = new ListBuffer[(String, Array[Float])]()
		for (movieId <- model.getVectors.keys) {
			movieEmb += Tuple2(movieId, model.getVectors(movieId))
		}
		val movieEmbDF = spark.createDataFrame(movieEmb).toDF("movieId", "Embedding")
		movieEmbDF.printSchema()
		movieEmbDF.show(5, truncate = false)

		(movieEmbDF, model)
	}

	def generateUserEmbedding(spark: SparkSession, ratingsPath: String, word2vec: Word2VecModel): DataFrame = {
		val ratingSamples = spark.read.format("csv").option("header", "true").load(ratingsPath)
		ratingSamples.printSchema()
		ratingSamples.show(5, truncate = false)

		val userEmbeddings = new ArrayBuffer[(String, Array[Float])]()

		// 这里其实是使用用户的观影记录的embedding 来代表user embedding
		ratingSamples.collect().groupBy(_.getAs[String]("userId")) // 先以userId进行分组聚合
		  .foreach(
			  user => {
				  val userId = user._1
				  // 数据量太大，这里会导致本地JVM堆内存溢出
				  var userEmb = new Array[Float](EMBEDDING_DIM) // 对每个用户先创建一个空的Embedding向量数组

				  // 关于foldleft 和 foldright 的用法
				  // https://stackoverflow.com/questions/40827710/scala-fold-right-and-fold-left
				  userEmb = user._2.foldRight[Array[Float]](userEmb) (
					  (row, newEmb) => {
						  val movieId = row.getAs[String]("movieId") //取到用户观看的一部电影
						  val movieEmb = word2vec.getVectors.get(movieId) //从word2vec模型中找到对应的Embedding向量
						  if (movieEmb.isDefined) {
							  newEmb.zip(movieEmb.get).map{case (x, y) => x + y} //把movie embedding进行累加
						  } else {
							  newEmb
						  }
					  }
				  )
				  userEmbeddings.append((userId, userEmb))
			  }
		  )

		// 创建User RDD
		// 格式：
		//  userId | embedding
		val userEmbDF = spark.createDataFrame(userEmbeddings).toDF("userId", "Embedding")
		userEmbDF.printSchema()
		userEmbDF.show(5, truncate = false)

		userEmbDF
	}

	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setMaster(GLOBAL_CONFIG("spark.cores")).setAppName("Embedding")
		val spark = SparkSession.builder.config(sparkConf).getOrCreate()

		// 将用户评分数据转换成观影历史列表
		val movieSeq = processItemSequence(spark, rawRatingDataPath)
		// 使用word2vec模型训练样本数据,产生movie的embedding向量
		val (movieEmbDF, word2vec) = trainItem2vec(spark, movieSeq)
		// 根据用户的观影序列，产生用户的embedding向量表示
		val userEmbDF = generateUserEmbedding(spark, rawRatingDataPath, word2vec)

		//MongoDB 隐式参数
		implicit val mongoConfig = MongoConfig(GLOBAL_CONFIG("mongo.uri"), GLOBAL_CONFIG("mongo.db"))
		// store movie embedding mongoDB
		storeDataToMongoDB(movieEmbDF, userEmbDF)
	}

	def storeDataToMongoDB(movieEmbDF: DataFrame, userEmbDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
		//新建到MomgoDB的连接
		val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

		//先删除MongoDB中对应的数据库
		mongoClient(mongoConfig.db)(MONGODB_MOVIE_EMBEDDING).drop()
		mongoClient(mongoConfig.db)(MONGODB_USER_EMBEDDING).drop()

		movieEmbDF.write
		  .option("uri", mongoConfig.uri)
		  .option("collection", MONGODB_MOVIE_EMBEDDING)
		  .mode("overwrite")
		  .format("com.mongodb.spark.sql")
		  .save()

		userEmbDF.write
		  .option("uri", mongoConfig.uri)
		  .option("collection", MONGODB_USER_EMBEDDING)
		  .mode("overwrite")
		  .format("com.mongodb.spark.sql")
		  .save()

		//对数据表建索引
		mongoClient(mongoConfig.db)(MONGODB_MOVIE_EMBEDDING).createIndex(MongoDBObject("movieId" -> 1))
		mongoClient(mongoConfig.db)(MONGODB_USER_EMBEDDING).createIndex(MongoDBObject("userId" -> 1))

		mongoClient.close()
	}
}
