
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object FeatureEngineering {
	/**
	 * One-hot encoding example function
	 * @param samples movie samples dataframe
	 */
	def oneHotEncoderExample(samples:DataFrame): Unit = {
		// 先将movieId这一列转换成 由字符串转换为Int类型
		val samplesWithIdNumber = samples.withColumn("movieIdNumber", col("movieId").cast(sql.types.IntegerType))

		val oneHotEncoder = new OneHotEncoderEstimator()
		  .setInputCols(Array("movieIdNumber"))
		  .setOutputCols(Array("movieIdVector"))
		  .setDropLast(false)

		val oneHotEncoderSamples = oneHotEncoder.fit(samplesWithIdNumber).transform(samplesWithIdNumber)
		oneHotEncoderSamples.printSchema()
		oneHotEncoderSamples.show(5, truncate = false)
	}

	// sparse vector向量有三个参数: (size, sorted array, filled number)
	val array2vec: UserDefinedFunction = udf { (a: Seq[Int], length: Int) => org.apache.spark.ml.linalg.Vectors.sparse(length, a.sortWith(_ < _).toArray, Array.fill[Double](a.length)(1.0)) }

	/**
	 * Multi-hot encoding example function
	 * @param samples movie samples dataframe
	 */
	def multiHotEncoderExample(samples:DataFrame): Unit ={
		// 每部电影包含多个genres，根据genres来生成一部电影的multi-Hot emnbedding
		val samplesWithGenre = samples.select(col("movieId"), col("title"),explode(split(col("genres"), "\\|").cast("array<string>")).as("genre"))
		samplesWithGenre.printSchema()
		samplesWithGenre.show(10, truncate = false)

		val genreIndexer = new StringIndexer().setInputCol("genre").setOutputCol("genreIndex")

		val stringIndexerModel : StringIndexerModel = genreIndexer.fit(samplesWithGenre)

		val genreIndexSamples = stringIndexerModel.transform(samplesWithGenre)
		  .withColumn("genreIndexInt", col("genreIndex").cast(sql.types.IntegerType))
		genreIndexSamples.printSchema()
		genreIndexSamples.show(10, truncate = false)

		val indexSize = genreIndexSamples.agg(max(col("genreIndexInt"))).head().getAs[Int](0) + 1

		val processedSamples =  genreIndexSamples
		  .groupBy(col("movieId")).agg(collect_list("genreIndexInt").as("genreIndexes"))
		  .withColumn("indexSize", typedLit(indexSize))
		processedSamples.printSchema()
		processedSamples.show(10, truncate = false)

		val finalSample = processedSamples.withColumn("vector", array2vec(col("genreIndexes"),col("indexSize")))
		finalSample.printSchema()
		finalSample.show(10, truncate = false)
	}

	// 将一个值转换成一个稠密向量，其实就是一个数组
	val double2vec: UserDefinedFunction = udf { (value: Double) => org.apache.spark.ml.linalg.Vectors.dense(value) }

	/**
	 * Process rating samples
	 * @param samples rating samples
	 */
	def ratingFeatures(samples:DataFrame): Unit ={
		samples.printSchema()
		samples.show(10, truncate = false)

		//calculate average movie rating score and rating count
		// 计算电影的平均评分 以及 评分的数量
		val movieFeatures = samples.groupBy(col("movieId"))
		  .agg(count(lit(1)).as("ratingCount"),
			  avg(col("rating")).as("avgRating"),
			  variance(col("rating")).as("ratingVar"))
		  .withColumn("avgRatingVec", double2vec(col("avgRating")))

		movieFeatures.printSchema()
		movieFeatures.show(5, truncate = false)

		//bucketing 解决特征值分布极不均匀的问题，因为人们的评分都集中在3分以上
		//分桶处理，创建QuantileDiscretizer进行分桶，将打分次数这一特征分到100个桶中
		val ratingCountDiscretizer = new QuantileDiscretizer()
		  .setInputCol("ratingCount")
		  .setOutputCol("ratingCountBucket")
		  .setNumBuckets(100)

		//Normalization
		//归一化处理，创建MinMaxScaler进行归一化，将平均得分进行归一化
		val ratingScaler = new MinMaxScaler()
		  .setInputCol("avgRatingVec")
		  .setOutputCol("scaleAvgRating")

		//创建一个pipeline，依次执行两个特征处理过程
		val pipelineStage: Array[PipelineStage] = Array(ratingCountDiscretizer, ratingScaler)
		val featurePipeline = new Pipeline().setStages(pipelineStage)

		val movieProcessedFeatures = featurePipeline.fit(movieFeatures).transform(movieFeatures)
		movieProcessedFeatures.printSchema()
		movieProcessedFeatures.show(10, truncate = false)
	}

	def main(args: Array[String]): Unit = {
		Logger.getLogger("org").setLevel(Level.ERROR)

		val conf = new SparkConf()
		  .setMaster("local")
		  .setAppName("featureEngineering")
		  .set("spark.submit.deployMode", "client")

		val spark = SparkSession.builder.config(conf).getOrCreate()
		val movieResourcesPath = this.getClass.getResource("/MovieLens/movies.csv")
		val movieSamples = spark.read.format("csv").option("header", "true").load(movieResourcesPath.getPath)

		println("Raw Movie Samples:")
		movieSamples.printSchema()
		movieSamples.show(5, truncate = false)

		println("OneHotEncoder Example:")
		oneHotEncoderExample(movieSamples)

		println("MultiHotEncoder Example:")
		multiHotEncoderExample(movieSamples)

		println("Numerical features Example:")
		val ratingsResourcesPath = this.getClass.getResource("/MovieLens/ratings.csv")
		val ratingSamples = spark.read.format("csv").option("header", "true").load(ratingsResourcesPath.getPath)
		ratingFeatures(ratingSamples)
	}
}
