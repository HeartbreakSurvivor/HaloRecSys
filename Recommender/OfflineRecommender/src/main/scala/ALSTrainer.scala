
import breeze.numerics.sqrt
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

object ALSTrainer {
	//Mongo中保存的用户评分Rating表
	val MONGODB_RATING_COLLECTION = "Rating"

	//LFM模型输出结果写入MongoDB
	// 每个用户的评分输出表
	val LFM_USER_RECS = "LFM_USER_RECS"
	// 每部电影的相似度矩阵表
	val LFM_MOVIE_RECS = "LFM_MOVIE_RECS"

	val SIM_THRESHOLD = 0.6 //定义电影相似度阈值
	val USER_MAX_RECOMMENDATION = 10 //给每个用户推荐的电影的个数

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
//		val ratingRDD = spark.read
//		  .option("uri", mongoConfig.uri)
//		  .option("collection", MONGODB_RATING_COLLECTION)
//		  .format("com.mongodb.spark.sql") //对应的表名
//		  .load()
//		  .as[MovieRating] //转换成MovieRating格式
//		  .rdd
//		  .map( rating => Rating(rating.uid, rating.mid, rating.score)) //去掉时间戳，转换成指定的格式
//		  .cache() //数据较大，先缓存在内存中，提高速度
//
//		// 随机切分数据集，生成训练集和测试集
//		val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
//		val trainRDD = splits(0)
//		val testRDD = splits(1)
//
//		//模型超参数选择，输出最优参数
//		adjustALSParam(trainRDD, testRDD)
//
//		spark.close()
	}

	def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]): Unit ={
		val result = for( rank <- Array(50, 100, 200, 300); lambda <- Array( 0.001, 0.01, 0.1, 1 ))
			yield {
				val model = ALS.train(trainData, rank, 5, lambda)
				// 计算当前参数对应模型的rmse，返回Double
				val rmse = getRMSE( model, testData )
				( rank, lambda, rmse )
			}
		// 控制台打印输出最优参数
		println(result.minBy(_._3))
	}

	def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
		//计算预测评分
		val userProducts = data.map( item => (item.user, item.product))
		val predictRating = model.predict(userProducts)

		// 以uid、mid作为外键，join实际观测值和预测值
		val observed = data.map( item => ((item.user, item.product), item.rating) )
		val predict = predictRating.map(item => ( (item.user, item.product), item.rating) )

		//计算预测值与实际值的 RMSE误差
		sqrt(observed.join(predict).map{
				case ( (uid, mid), (actual, pred)) =>
					val err = actual - pred
					err * err //这里直接返回误差的平方
			}.mean() //对误差的平方求平均数
		) //对误差平均数再开根号
	}
}
