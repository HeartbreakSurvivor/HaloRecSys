import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{collect_set, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient


//定义原始数据的样例类
/*
* 电影数据样例类
* 	movieId,title,genres
* */
case class Movie(mid:Int, name:String, genres:String)

/*
* 评分数据样例类
* 	userId,movieId,rating,timestamp
* */
case class Rating(uid:String, mid:Int, score:Double, timestamp:Int)

/*
*  影片链接数据样例类
* 	movieId,imdbId,tmdbId
* */
case class Link(mid:Int, imdbId:String, tmdbId:String)

/*
* 标签数据样例类
* 	userId,movieId,tag,timestamp
* */
case class Tag(uid:String, mid:Int, tag:String, timestamp:Int)

/*
* 用户数据样例类
* 	userId,password,id
* */
case class User(username:String, password:String, id:Int)

/*
* 定义MongoDB数据库相关配置信息样例类
* */
case class MongoConfig(uri:String, db:String)

/*
* 定义ES相关配置信息样例类
* @param httpHosts http主机列表
* @param transportHosts transportHosts主机列表
* @param index 需要操作的索引
* @param clustername 集群名称
* */
case class ESConfig(httpHosts:String, transportHosts:String, index:String, clusterName:String)

object DataLoader {
	// 定义电影数据路径常量
	val MOVIE_DATA_PATH = "/Volumes/Study/RS/Projects/HaloRecSys/Recommender/DataLoader/src/main/resources/MovieLens/movies.csv"
	val RATING_DATA_PATH = "/Volumes/Study/RS/Projects/HaloRecSys/Recommender/DataLoader/src/main/resources/MovieLens/ratings.csv"
	val TAG_DATA_PATH = "/Volumes/Study/RS/Projects/HaloRecSys/Recommender/DataLoader/src/main/resources/MovieLens/tags.csv"
	val LINK_DATA_PATH = "/Volumes/Study/RS/Projects/HaloRecSys/Recommender/DataLoader/src/main/resources/MovieLens/links.csv"

	val RECOMMENDATION_DB_NAME = "recommender"
	val MONGODB_MOVIE_COLLECTION = "Movies"
	val MONGODB_RATING_COLLECTION = "Ratings"
	val MONGODB_LINK_COLLECTION = "Links"
	val MONGODB_TAG_COLLECTION = "Tags"
	val MONGODB_USER_COLLECTION = "Users" // 记录所有用户以及密码的表

	val ES_MOVIE_INDEX = "ES_Movie"

	def main(args: Array[String]): Unit = {
		val config = Map(
			"spark.cores" -> "local[*]",
			//数据库url
			"mongo.uri" -> "mongodb://localhost:27017/".concat(RECOMMENDATION_DB_NAME),
			//数据库名称
			"mongo.db" -> "recommender",
			"es.httpHosts" -> "localhost:9200",
			"es.transportHosts" -> "localhost:9300",
			"es.index" -> RECOMMENDATION_DB_NAME,
			"es.cluster.name" -> "es-cluster"
		)

		// 解决运行错误"availableProcessors is already set to [8], rejecting [8]"问题，
		// https://www.cnblogs.com/sxdcgaq8080/p/10214919.html
		System.setProperty("es.set.netty.runtime.available.processors", "false")

		val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")

		val spark = SparkSession.builder().config(sparkConf).getOrCreate()

		// 隐式转换相关的包
		import spark.implicits._

		//读取相关数据并转换成对应的数据结构
		//这里有个坑，因为原始数据带了表头，所以这里需要先将表头去掉
		val movieRDDwithHead = spark.sparkContext.textFile(MOVIE_DATA_PATH)
		val movieHead = movieRDDwithHead.first() //先读取表头
		val movieRDD = movieRDDwithHead.filter(_ != movieHead) //使用filter算子过滤
		val movieDF = movieRDD.map(
			item => {
				// can't handle some exception
				//val attr = item.split(",")
				//Movie(attr(0).toInt, attr(1).trim, attr(2).trim)
				
				val idx1 = item.indexOf(",")
				val idx2 = item.lastIndexOf(",")

				val mid = item.substring(0, idx1).toInt
				val name = item.substring(idx1+1, idx2).trim
				val genres = item.substring(idx2+1).trim
				Movie(mid, name, genres)
			}
		).toDF()

		val ratingRDDwithHead = spark.sparkContext.textFile(RATING_DATA_PATH)
		val ratingHead = ratingRDDwithHead.first() //先读取表头
		val ratingRDD = ratingRDDwithHead.filter(_ != ratingHead) //使用filter算子过滤
		val ratingDF = ratingRDD.filter(_.split(",").length == 4).map( // 筛选出不符合格式的数据
			item => {
				val attr = item.split(",")
				// 这里给所有用户前面都加上一个字符串"User"
				Rating("User".concat(attr(0).trim), attr(1).toInt, attr(2).toDouble, attr(3).toInt)
			}
		).toDF()

		// 构造用户数据表
		val userDF = ratingRDD.map(
			item => {
				val attr = item.split(",")
				(attr(0), "123456")
			}
		  )
		  .distinct() // 去重 + 编号
		  .zipWithIndex()
		  .map(
			item => {
				val username = "User".concat(item._1._1)
				User(username, item._1._2, item._2.toInt)  // 改名为 "Userxxx"
			}
		).toDF()

		userDF.printSchema()
		userDF.first()

		val linkRDDwithHead = spark.sparkContext.textFile(LINK_DATA_PATH)
		val linkHead = linkRDDwithHead.first()
		val linkRDD = linkRDDwithHead.filter(_ != linkHead)
		val linkDF = linkRDD.filter(_.split(",").length == 3)
		  .map(
			item => {
				val attr = item.split(",")
				Link(attr(0).toInt, attr(1).trim, attr(2).trim)
			}
		).toDF()

		val tagRDDwithHead = spark.sparkContext.textFile(TAG_DATA_PATH)
		//另外一种方式来去掉第一行
		//不过这样做的前提是，第一行永远是存在分区0的，否则会出问题
		val tagRDD = tagRDDwithHead.mapPartitionsWithIndex( (idx, iter) => if (idx == 0) iter.drop(1) else iter)

		// 这里会有问题，因为Tags数据集不是标准的，使用','分割的话，因为有些数据中tag一栏，也是使用','分割的，会导致这里出错，所以这里加一个容错措施
		// 这里是先做了一个筛选，然后再做的map操作，有重复操作，可以优化，不过目前还没想到具体的方法
		val tagDF = tagRDD.filter(_.split(",") == 4)
		  .map(
			item => {
				val attr = item.split(",")
				Tag(attr(0).trim, attr(1).toInt, attr(2).trim, attr(3).toInt)
			}
		).toDF()

		//隐式参数
		implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

		// 保存到MongoDB中
		storeDataToMongoDB(movieDF, ratingDF, linkDF, tagDF, userDF)

		//数据预处理，把Movie对应的Tag信息加到Movie里面去，加一个新列，名为"tags",格式为 tag1|tag2|tag3|...
		// 方便ElasticSearch
		// $"columnName"               // Scala short hand for a named column.
		/*
		* tags: tag1|tag2|tag3|...
		* */
//		val newTag = tagDF.groupBy($"mid")
//			.agg( concat_ws("|", collect_set($"tag")).as("tags") ) //提取用户对电影打的标签
//			.select("mid", "tags") //只需要mid和tags这两列的数据
//
//		//让newTag和movie进行左外连接,因为movie中的某些movie就没有对应的tag，故要以movie中的mid作为基准来进行左外连接
//		val movieWithTagsDF = movieDF.join(newTag, Seq("mid"), "left")
//
//		implicit val esConfig = ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))
//		storeDataToES(movieWithTagsDF)
	}

	def storeDataToMongoDB(movieDF:DataFrame, ratingDF:DataFrame, linkDF:DataFrame, tagDF:DataFrame, userDF:DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
		//新建到MomgoDB的连接
		val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

		//先删除MongoDB中对应的数据库
		mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).drop()
		mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).drop()
		mongoClient(mongoConfig.db)(MONGODB_LINK_COLLECTION).drop()
		mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).drop()
		mongoClient(mongoConfig.db)(MONGODB_USER_COLLECTION).drop()

		movieDF.write
		  .option("uri", mongoConfig.uri) //定义Mongo的url
		  .option("collection", MONGODB_MOVIE_COLLECTION) //往哪张表中写入
		  .mode("overwrite")
		  .format("com.mongodb.spark.sql")
		  .save()

		ratingDF.write
		  .option("uri", mongoConfig.uri)
		  .option("collection", MONGODB_RATING_COLLECTION)
		  .mode("overwrite")
		  .format("com.mongodb.spark.sql")
		  .save()

		linkDF.write
		  .option("uri", mongoConfig.uri)
		  .option("collection", MONGODB_LINK_COLLECTION)
		  .mode("overwrite")
		  .format("com.mongodb.spark.sql")
		  .save()

		tagDF.write
		  .option("uri", mongoConfig.uri)
		  .option("collection", MONGODB_TAG_COLLECTION)
		  .mode("overwrite")
		  .format("com.mongodb.spark.sql")
		  .save()

		userDF.write
		  .option("uri", mongoConfig.uri)
		  .option("collection", MONGODB_USER_COLLECTION)
		  .mode("overwrite")
		  .format("com.mongodb.spark.sql")
		  .save()

		//对数据表建索引
		mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
		mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
		mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
		mongoClient(mongoConfig.db)(MONGODB_LINK_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
		mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
		mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
		mongoClient(mongoConfig.db)(MONGODB_USER_COLLECTION).createIndex(MongoDBObject("uid" -> 1))

		mongoClient.close()
	}

	def storeDataToES(movieDF: DataFrame)(implicit esConfig: ESConfig): Unit = {
		// 新建es配置
		val settings: Settings = Settings.builder().put("cluster.name", esConfig.clusterName).build()

		// 新建一个es客户端
		val esClient = new PreBuiltTransportClient(settings)

		//利用正则表达式来筛选主机名，ip:端口
		val REGEX_HOST_PORT = "(.+):(\\d+)".r
		esConfig.transportHosts.split(",").foreach {
			// 进行模式匹配，拿到对应的IP和端口
			case REGEX_HOST_PORT(host: String, port: String) => {
				esClient.addTransportAddress(new InetSocketTransportAddress( InetAddress.getByName(host), port.toInt ))
			}
		}

		// 先清理遗留的数据,即删除原先存在的库
		if( esClient.admin().indices().exists( new IndicesExistsRequest(esConfig.index) )
		  .actionGet()
		  .isExists
		) {
			esClient.admin().indices().delete( new DeleteIndexRequest(esConfig.index) )
		}

		//创建新的表
		esClient.admin().indices().create( new CreateIndexRequest(esConfig.index) )

		movieDF.write
		  .option("es.nodes", esConfig.httpHosts)
		  .option("es.http.timeout", "100m")
		  .option("es.mapping.id", "mid") //指定映射的主键
		  .mode("overwrite")
		  .format("org.elasticsearch.spark.sql")
		  .save(esConfig.index + "/" + ES_MOVIE_INDEX)
	}
}
