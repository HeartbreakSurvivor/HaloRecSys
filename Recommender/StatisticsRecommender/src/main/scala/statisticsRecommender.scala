import java.util.Date
import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
//先创建样例类，定义离线统计推荐需要的数据结构
/*
* 主要要实现四个离线推荐模块
* 1. 历史热门电影统计推荐 --> 根据用户对电影的评论数来筛选
* 2. 近期热门电影统计推荐 --> 对历史热门电影推荐的结果中筛选一定时间段
* 3. 电影平均评分统计推荐 --> 根据所有电影评分高低排序，来进行推荐
* 4. 各类别Top10优质电影推荐 --> 分类别并且评分最靠前的前10个电影进行推荐
* */

//定义原始数据的样例类,DataLoader中已经有相关定义了
/*
* 电影数据样例类
* 	movieId,title,genres
* */
case class Movie(mid:Int, name:String, genres:String)

/*
* 评分数据样例类
* 	userId,movieId,rating,timestamp
* */
case class Rating(uid:Int, mid:Int, score:Double, timestamp:Int)

/*
* 定义MongoDB数据库相关配置信息样例类
* */
case class MongoConfig(uri:String, db:String)

//推荐数据样例类
case class Recommendation(mid: Int, score: Double)

//定义电影类别Top10样例类，即每一个类别，对应一个推荐列表
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

object statisticsRecommender {
	//Mongo数据库中的保存Movie信息的表名称
	val MONGODB_MOVIE_COLLECTION = "Movies"
	val MONGODB_RATING_COLLECTION = "Ratings"

	//离线统计推荐的结果保存的表名称
	val RATE_MOST_MOVIES = "RateMostMovies" //总体评分次数最多的电影
	val RATE_MOST_RECENTLY_MOVIES = "RateMostRecentlyMovies" //最近评分次数最多的电影
	val AVERAGE_RATINGS_MOVIES = "AverageScoreMovies" //每部电影的平均评分
	val GENRES_TOP_N_MOVIES = "GenresTopMovies" //按类别划分评分最高的电影

	val TOP_N = 10 //取前多少个

	def main(args: Array[String]): Unit = {
		val config = Map(
			"spark.cores" -> "local[*]",
			//数据库url
			"mongo.uri" -> "mongodb://localhost:27017/recommender",
			//数据库名称
			"mongo.db" -> "recommender"
		)

		val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")

		val spark = SparkSession.builder().config(sparkConf).getOrCreate()

		// 隐式转换相关的包
		import spark.implicits._

		//创建MongoDB配置
		implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

		//从Mongodb中读取相关数据
		val ratingDF = spark.read
		  .option("uri", mongoConfig.uri)
		  .option("collection", MONGODB_RATING_COLLECTION)
		  .format("com.mongodb.spark.sql") //对应的表名
		  .load()
		  .as[Rating] //转换成Rating格式

		val movieDF = spark.read
		  .option("uri", mongoConfig.uri)
		  .option("collection", MONGODB_MOVIE_COLLECTION)
		  .format("com.mongodb.spark.sql") //对应的表名
		  .load()
		  .as[Movie] //转换成Rating格式

		//编写SQL语句，进行相关筛选
		//先创建一个TempView用于SQL查询
		ratingDF.createOrReplaceTempView("ratings")
		// 一共要统计四种数据

		// 1、 历史热门推荐，按照电影的评价数目降序排列
		val rateMostMoviesDF = spark.sql("select mid, count(mid) as count from ratings group by mid order by count desc")
		rateMostMoviesDF.show(20, truncate = false)
		storeDFtoMongoDB(rateMostMoviesDF, RATE_MOST_MOVIES)

		// 2、近期热门统计，按照年月日选取最近的评分数据，统计评分个数
		val simpleDataFormat = new SimpleDateFormat("yyyyMM")
		// 注册UDF，将时间戳转换成年月的格式
		spark.udf.register("changeDate", (x: Int) => simpleDataFormat.format(new Date(x * 1000L)).toInt) //转换成Long格式，否则会溢出
		val ratingOfYearMonth = spark.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")
		ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth") //创建视图，方便Sql查询

		//这句Sql语句的含义，先对ratingOfMonth按照年月聚合，接着再按照mid聚合，然后按照年月降序排列，即筛选的是最近一段时间的，同理，还要对count进行排序，即时间最近并且出现的次数最多的排的越靠前
		val rateMostRecentlyMoviesDF = spark.sql("select mid, count(mid) as count, yearmonth from ratingOfMonth group by yearmonth, mid order by yearmonth desc, count desc")
		rateMostRecentlyMoviesDF.show(20, truncate = false)
		storeDFtoMongoDB(rateMostRecentlyMoviesDF, RATE_MOST_RECENTLY_MOVIES)

		// 3、优质电影统计，统计每部电影的平均评分,并且根据降序排列
		//val averageRatingsDF = spark.sql("select mid, avg(score) as avg from ratings group by mid")
		val averageRatingsDF = spark.sql("select mid, avg(score) as avg from ratings group by mid order by avg desc")
		averageRatingsDF.show(20, truncate = false)
		storeDFtoMongoDB(averageRatingsDF, AVERAGE_RATINGS_MOVIES)

		// 4、各类别Top20电影统计，根据评分高低排名
		//定义电影的所有类别,一共有18个类别
		val genres = List("Action", "Adventure", "Animation", "Children's", "Comedy", "Crime","Documentary","Drama","Fantasy","Film-Noir","Horror","Musical", "Mystery","Romance","Sci-Fi","Thriller","War","Western")
		//首先将每部电影的平均评分加到Movie表中，相当于加一列, inner join
		val movieWithScore = movieDF.join(averageRatingsDF, "mid") //默认是inner join

		//为了做笛卡尔积，将genres转换成RDD
		val genresRDD = spark.sparkContext.makeRDD(genres)

		//计算类别Top10， 首先对类别和电影做笛卡尔积
		val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd)
		  .filter{
			  //条件过滤，找出movie表中genres字段中包含当前genre的所有电影
			  case (genre, row) => row.getAs[String]("genres").toLowerCase.contains( genre.toLowerCase)
		  }
		  .map{
			  // 这一步将每行数据映射成指定的 "genre mid avg" 形式，
			  case (genre, movieRow) => (genre, (movieRow.getAs[Int]("mid"), movieRow.getAs[Double]("avg")))
		  }
		  .groupByKey() //根据类别进行聚合，即同一个类别的所有电影放到一起，GroupByKey的返回值是一个RDD
		  .map{
			  // 这里的RDD需要先转换成list之后才能进行排序操作
			  case (genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2 > _._2).take(TOP_N)
				  .map(item => Recommendation(item._1, item._2))) //将List中的每个元素封装成Recommendation样例类
		  }
		  .toDF()

		storeDFtoMongoDB(genresTopMoviesDF, GENRES_TOP_N_MOVIES)

		spark.stop()
	}

	def storeDFtoMongoDB(df: DataFrame, collectionName: String)(implicit mongoConfig: MongoConfig) : Unit = {
		df.write
		  .option("uri", mongoConfig.uri)
		  .option("collection", collectionName)
		  .mode("overwrite")
		  .format("com.mongodb.spark.sql")
		  .save()
	}

}