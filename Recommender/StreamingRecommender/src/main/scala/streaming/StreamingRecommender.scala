package streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.spark.sql.fieldTypes.Timestamp
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

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

// 定义一个连接助手，实现序列化接口
object ConnHelper extends Serializable {
	lazy val jedis = new Jedis("localhost")
	lazy val mongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))
}

object StreamingRecommender {
	val MAX_USER_RATINGS_NUM = 20 //获取用户最近的评分个数
	val MAX_SIM_MOVIES_NUM = 20
	val MONGODB_STREAM_RECS_COLLECTION = "StreamingRecs" // 实时推荐列表
	val MONGODB_RATING_COLLECTION = "Rating"
	val MONGODB_MOVIE_RECS_COLLECTION = "LFM_MOVIE_RECS" //电影相似度矩阵

	def main(args: Array[String]):Unit = {
		val config = Map(
			"spark.cores" -> "local[*]",
			//数据库url
			"mongo.uri" -> "mongodb://localhost:27017/recommender",
			//数据库名称
			"mongo.db" -> "recommender",
			//Kafka topics配置
			"kafka.topic" -> "recommender"
		)

		val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommender")

		// 创建一个SparkSession
		val spark = SparkSession.builder().config(sparkConf).getOrCreate()

		// 拿到streaming context
		val sc = spark.sparkContext
		//val ssc = new StreamingContext(sparkConf, Seconds(1))
		val ssc = new StreamingContext(sc, Seconds(2))    // batch duration

		import spark.implicits._

		implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

		// 加载电影相似度矩阵数据,把它广播出去，针对大数据，每个Executor上只保存一份副本
		val simMovieMatrix = spark.read
		  .option("uri", mongoConfig.uri)
		  .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
		  .format("com.mongodb.spark.sql")
		  .load()
		  .as[MovieRecs]
		  .rdd
		  .map{
			  // 这里将Seq迭代对象中的元素先转换成元祖，然后再转换成Map，方便查询
			  movieRecs => (movieRecs.mid, movieRecs.recs.map( x => (x.mid, x.score)).toMap )
		  }.collectAsMap() // 再转换成Map

		val simMovieMatrixBroadCast = sc.broadcast(simMovieMatrix)

		// 定义Kafka连接参数
		val kafkaParam = Map(
			"bootstrap.servers" -> "localhost:9092",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "recommender",
			"auto.offset.reset" -> "latest"
		)
		// 通过kafka创建一个DStream
		val kafkaStream = KafkaUtils.createDirectStream[String, String]( ssc,
			LocationStrategies.PreferConsistent,
			ConsumerStrategies.Subscribe[String, String]( Array(config("kafka.topic")), kafkaParam )
		)

		// 把原始数据UID|MID|SCORE|TIMESTAMP 转换成评分流
		// 这个数据代表的含义是 用户UID在时刻TIMESTAMP对电影MID做了评价，评分为SCORE。
		val ratingStream = kafkaStream.map{
			msg =>
				val attr = msg.value().split("\\|")
				( attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt )
		}

		// 这个ratingStream是一个时间窗口之类的一组数据，这里的时间设置的是2秒，即2秒内产生的所有数据
		ratingStream.foreachRDD{
			rdds => rdds.foreach{
				case (uid, mid, score, timestamp) => {
					println("user rating data coming.....")
					// 1. 从redis中获取当前用户最近的K次评分，保存成数组，这里相当于电商中获取用的历史数据，不要取太久远的，参考价值不大
					val userRecentlyRatings = getUserRecentlyRating( MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis )

					// 2. 从相似度矩阵中取出当前电影最相似的N个电影，作为备选列表，Array[mid]
					val candidateMovies = getTopSimMovies( MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value )

					// 3. 对每个备选电影，计算推荐优先级，得到当前用户的实时推荐列表，Array[(mid, score)]
					val streamRecs = computeMovieScores( candidateMovies, userRecentlyRatings, simMovieMatrixBroadCast.value )

					// 4. 把推荐数据保存到mongodb
					saveDataToMongoDB( uid, streamRecs )
				}
			}
		}

		// 开始接收和处理数据
		ssc.start()

		println(">>>>>>>>>>>>>>> streaming started!")
		ssc.awaitTermination()
	}

	// redis操作返回的是java类，为了用map操作需要引入转换类
	import scala.collection.JavaConversions._

	def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
		// 从redis读取数据，用户评分数据保存在 uid:UID 为key的队列里，value是 MID:SCORE
		// redis 中的数据是前端捕获到用户评分之后写入的，数据格式为：
		// key: uid:id  value: mid:score
		jedis.lrange("uid:" + uid, 0, num-1)
		  .map{
			  item => // 具体每个评分又是以冒号分隔的两个值
				  val attr = item.split("\\:")
				  ( attr(0).trim.toInt, attr(1).trim.toDouble )
		  }
		  .toArray
	}

	/**
	 * 获取跟当前电影做相似的num个电影，作为备选电影
	 * @param num       相似电影的数量
	 * @param mid       当前电影ID
	 * @param uid       当前评分用户ID
	 * @param simMovies 相似度矩阵
	 * @return          过滤之后的备选电影列表
	 */
	def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
					   (implicit mongoConfig: MongoConfig): Array[Int] ={
		// 1. 从相似度矩阵中拿到所有相似的电影
		val allSimMovies = simMovies(mid).toArray

		// 2. 从mongodb中查询用户已看过的电影
		val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
		  .find( MongoDBObject("uid" -> uid) )
		  .toArray
		  .map{
			  item => item.get("mid").toString.toInt
		  }

		// 3. 把看过的过滤，得到输出列表
		allSimMovies.filter( x => ! ratingExist.contains(x._1) ) //推荐的电影是否用户已经看过
		  .sortWith(_._2 > _._2) // 根据电影相似度来评分，降序排列
		  .take(num) // 选取前num项
		  .map(x=>x._1) // 只保留mid，去掉score项
	}

	def computeMovieScores(candidateMovies: Array[Int],
						   userRecentlyRatings: Array[(Int, Double)],
						   simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Array[(Int, Double)] ={
		// 定义一个ArrayBuffer，用于保存每一个备选电影的基础得分
		val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
		// 定义一个HashMap，保存每一个备选电影的增强减弱因子
		val increMap = scala.collection.mutable.HashMap[Int, Int]()
		val decreMap = scala.collection.mutable.HashMap[Int, Int]()

		for( candidateMovie <- candidateMovies; userRecentlyRating <- userRecentlyRatings) {
			// 拿到备选电影和最近评分电影的相似度
			val simScore = getMoviesSimScore( candidateMovie, userRecentlyRating._1, simMovies )

			if(simScore > 0.7){
				// 计算备选电影的基础推荐得分
				scores += ( (candidateMovie, simScore * userRecentlyRating._2) )
				if( userRecentlyRating._2 > 3 ) {
					// 增强因子，即用户评分大于3的电影个数先保存起来
					increMap(candidateMovie) = increMap.getOrDefault(candidateMovie, 0) + 1
				} else {
					// 减弱因子，即用于评分小于3的电影个数保存起来
					decreMap(candidateMovie) = decreMap.getOrDefault(candidateMovie, 0) + 1
				}
			}
		}
		// 根据备选电影的mid做groupby，根据公式去求最后的推荐评分
		scores.groupBy(_._1).map{
			// groupBy之后得到的数据 Map( mid -> ArrayBuffer[(mid, score)] )
			case (mid, scoreList) =>
				( mid, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(mid, 1)) - log(decreMap.getOrDefault(mid, 1)) )
		}.toArray.sortWith(_._2>_._2)
	}

	// 获取两个电影之间的相似度
	def getMoviesSimScore(mid1: Int, mid2: Int, simMovies: scala.collection.Map[Int,
	  scala.collection.immutable.Map[Int, Double]]): Double = {
		simMovies.get(mid1) match { // 这里为了处理异常情况，因为可能拿不到值，所以做一个模式匹配
			case Some(sims) => sims.get(mid2) match {
				case Some(score) => score //匹配到值就直接返回
				case None => 0.0 // 匹配为空，则返回为0
			}
			case None => 0.0
		}
	}

	// 求一个数的对数，利用换底公式，底数默认为10
	def log(m: Int): Double = {
		val N = 10
		math.log(m)/ math.log(N)
	}

	def saveDataToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit ={
		// 定义到StreamRecs表的连接
		val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

		// 如果表中已有uid对应的数据，则删除
		streamRecsCollection.findAndRemove( MongoDBObject("uid" -> uid) )
		// 将streamRecs数据存入表中，插入的数据都要转换成MongoDBObject对象，方便查找
		streamRecsCollection.insert( MongoDBObject( "uid"->uid,
			// 先根据Score进行降序排列
			"recs"-> streamRecs.sortWith(_._2 > _._2).map(x=>MongoDBObject( "mid"->x._1, "score"->x._2 )) ) )
	}

}
