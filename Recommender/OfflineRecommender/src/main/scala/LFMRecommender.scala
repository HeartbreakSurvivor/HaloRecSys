import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

// 定义基础样例类
/*
* 评分数据样例类
* 基于评分数据的LFM，只需要Rating数据
* 	userId,movieId,rating,timestamp
* */
case class MovieRating(uid:Int, mid:Int, score:Double, timestamp:Int)

/*
* 定义MongoDB数据库相关配置信息样例类
* */
case class MongoConfig(uri:String, db:String)

//定义基础的推荐数据结构
case class Recommendation(mid: Int, score: Double)

//定义基于评分的用户推荐列表，即每一个用户，对应一个电影推荐列表
case class UserRecs(uid: Int, recs: Seq[Recommendation])

//定义基于电影相似度的电影推荐列表，记录与当前电影较为相似的电影列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

//定义与当前用户相似的用户列表
case class UserSimilarRec(uid: Int, score: Double)
case class SimUsers(uid: Int, sims: Seq[UserSimilarRec])

object LFMRecommender {
	//Mongo中保存的用户评分Rating表
	val MONGODB_RATING_COLLECTION = "Ratings"

	//LFM模型输出结果写入MongoDB
	// 每个用户的评分输出表
	val LFM_USER_RECS = "LFM_USER_RECS"
	// 每部电影的相似度矩阵表
	val LFM_MOVIE_RECS = "LFM_MOVIE_RECS"
	// 每个用户的相似度推荐
	val LFM_USER_SIM_RECS = "LFM_USER_SIM_RECS"

	val USER_SIM_THRESHOLD = 0.6 //定义用户相似度阈值
	val MOVIE_SIM_THRESHOLD = 0.6 //定义电影相似度阈值
	val USER_MAX_RECOMMENDATION = 10 //给每个用户推荐的电影的个数
	val USER_MAX_SIM_RECS = 10 //给每个用户推荐的相似用户的个数

	def main(args: Array[String]): Unit = {
		val config = Map(
			"spark.cores" -> "local[*]",
			//数据库url
			"mongo.uri" -> "mongodb://localhost:27017/recommender",
			//数据库名称
			"mongo.db" -> "recommender"
		)

		val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("LFM_Recommender")
		val spark = SparkSession.builder().config(sparkConf).getOrCreate()

		// 隐式转换相关的包
		import spark.implicits._

		//创建MongoDB配置
		implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

		//加载数据
		//从Mongodb中读取相关数据
		val ratingRDD = spark.read
		  .option("uri", mongoConfig.uri)
		  .option("collection", MONGODB_RATING_COLLECTION)
		  .format("com.mongodb.spark.sql") //对应的表名
		  .load()
		  .as[MovieRating] //转换成MovieRating格式
		  .rdd
		  .map( rating => (rating.uid, rating.mid, rating.score)) //去掉时间戳，转换成指定的格式
		  .cache() //数据较大，先缓存在内存中，提高速度

		// 从rating数据中=筛选出所有评过分的用户以及所有被评过分的电影，均需要去重
		val userRDD = ratingRDD.map(_._1).distinct()
		val movieRDD = ratingRDD.map(_._2).distinct()

		//训练LFM模型
		// case class Rating @Since("0.8.0") (user: Int, product: Int,rating: Double) Spark.Mlib中定义的Rating样例类
		val trainData = ratingRDD.map( x => Rating(x._1, x._2, x._3)) //把Rating数据转换成LFM模型需要的Rating类型
		val (rank, iter, lambda) = (200, 5, 0.1) //定义ALS模型训练的参数
		val lfm = ALS.train(trainData, rank, iter, lambda)

		//形成user-item的矩阵M，矩阵元素M[i][j]代表的含义是Useri 对 Moviej的评分
		val userMovies = userRDD.cartesian(movieRDD) // 笛卡尔积，形成U x M 大小的矩阵，
		//基于LFM模型，计算user对movie的评分
		val predRatings = lfm.predict(userMovies) //这个predict函数接受一个元素(uid, mid)，这里传入了一个RDD，里面包含了很多元祖

		val userRecs = predRatings.filter(_.rating > 0) //过滤出评分大于0的所有结果
			.map(rating => (rating.user, (rating.product, rating.rating))) //转换格式，每一行换成[user, (movie, score)]
			.groupByKey //按照uid进行分组
			.map{
				// GroupByKey之后，这里得到的recs是一个RDD，先转换成列表，再排序，再去前K个，再转换成Recommendation样例类的格式
				case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
			}
			.toDF()
		userRecs.printSchema()
		println(userRecs.count())
		// 写入对应的表中
		userRecs.write
			.option("uri", mongoConfig.uri)
			.option("collection", LFM_USER_RECS)
			.mode("overwrite")
			.format("com.mongodb.spark.sql")
			.save()

		//根据LFM模型得到的user隐特征，计算两个用户之间的相似度，并且保存起来
		val userFeats = lfm.userFeatures.map{ //userFeatures返回的是每个用户的隐向量
			case (uid, features) => (uid, new DoubleMatrix(features)) //将隐向量构造成矩阵的形式，方便做点积之类的运算
		}
		// 用户隐向量与本身做笛卡尔积
		val userSim = userFeats.cartesian(userFeats)
		  .filter{
			case (a, b) => a._1 != b._1 //筛选出与自身不同的用户
			}
		  .map{
			case (a , b) => {
				val simScore = this.consinSim(a._2, b._2) //计算两个向量之间的余弦相似度
				(a._1, (b._1, simScore))
			}
		  }
		  .filter(_._2._2 > USER_SIM_THRESHOLD)
		  .groupByKey()
		  .map{
			  case (uid, items) => SimUsers(uid, items.toList.sortWith(_._2 > _._2).take(USER_MAX_SIM_RECS).map( x => UserSimilarRec(x._1, x._2)))
		  }
		  .toDF()
		//将用户相似列表写入MongDB中
		userSim.write
		  .option("uri", mongoConfig.uri)
		  .option("collection", LFM_USER_SIM_RECS)
		  .mode("overwrite")
		  .format("com.mongodb.spark.sql")
		  .save()

		//基于Movie的隐式特征，计算每两个电影之间的相似度，然后保存
		val movieFeats = lfm.productFeatures.map{ //productfeatures返回的是每个电影对应的隐向量
			case (mid, features) => (mid, new DoubleMatrix(features)) //把电影对应的隐向量构造一个矩阵
		}
		// 电影本身与自己做笛卡尔积
		val movieRecs = movieFeats.cartesian(movieFeats)
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
		  .option("collection", LFM_MOVIE_RECS)
		  .mode("overwrite")
		  .format("com.mongodb.spark.sql")
		  .save()

		spark.stop()
	}

	def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
		movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
	}
}
