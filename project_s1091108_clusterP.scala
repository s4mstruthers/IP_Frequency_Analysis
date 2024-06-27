import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.net.InetAddress
import java.net.UnknownHostException

val spark = SparkSession.builder()
  .appName("GeoIP Analysis")
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .getOrCreate()

// Load GeoLite2-City.parquet into a DataFrame, selecting only necessary columns
val geoDataParquetPath = "hdfs:///user/s1091108/GeoLite2-City.parquet"
val geoDataDF = spark.read.parquet(geoDataParquetPath)
  .select("network", "latitude", "longitude")

// Extract IP addresses from WARC records and create IP DataFrame
val warcPath = "hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00000.warc.gz"
val warcData = spark.sparkContext.newAPIHadoopFile(
  warcPath,
  classOf[TextInputFormat],
  classOf[LongWritable],
  classOf[Text]
).map(_._2.toString)

val ipPattern = """(?:\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b|\b[0-9a-fA-F:]+\b)""".r
val ipAddresses = warcData.flatMap(record => ipPattern.findAllIn(record).toList)

val ipSchema = StructType(Array(StructField("ip", StringType, nullable = false)))
val ipDF = spark.createDataFrame(ipAddresses.map(Row(_)), ipSchema)

// Define a UDF to convert IP addresses to network prefixes
def ipToNetwork(ip: String): String = {
  try {
    val inetAddress = InetAddress.getByName(ip)
    if (inetAddress.getAddress.length == 4) { // IPv4
      inetAddress.getHostAddress.split('.').take(3).mkString(".")
    } else { // IPv6
      inetAddress.getHostAddress.split(':').take(4).mkString(":")
    }
  } catch {
    case e: UnknownHostException => ""
  }
}

val ipToNetworkUDF = udf(ipToNetwork _)

val ipDFWithNetwork = ipDF.withColumn("network", ipToNetworkUDF(col("ip")))

val broadcastGeoDataDF = broadcast(geoDataDF)

// Perform the join operation
val joinedDF = ipDFWithNetwork.join(broadcastGeoDataDF, "network")
  .select("ip", "latitude", "longitude")

// Filter out rows with null latitude or longitude
val filteredDF = joinedDF.filter(col("latitude").isNotNull && col("longitude").isNotNull)

// Group by ip, latitude, and longitude, and count occurrences (frequency)
val aggregatedDF = filteredDF.groupBy("ip", "latitude", "longitude")
  .agg(count("*").alias("frequency"))

// Save the aggregated data to a Parquet file
val aggregatedDataPath = "hdfs:///user/s1091108/rubigdata/IP_Frequency_Data"
aggregatedDF.write
  .mode("overwrite")
  .option("header", "true")
  .csv(aggregatedDataPath)

println(s"Aggregated data successfully saved to $aggregatedDataPath")

spark.stop()