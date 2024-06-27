import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.net.{InetAddress, UnknownHostException}
import java.math.BigInteger
import org.apache.spark.sql.expressions.Window

val spark = SparkSession.builder()
  .appName("GeoIP Analysis")
  .config("spark.sql.shuffle.partitions", "1000") // Increase shuffle partitions for better parallelism
  .config("spark.executor.memory", "16g") // Adjust these values based on your cluster
  .config("spark.executor.cores", "2")
  .config("spark.driver.memory", "16g")
  .config("spark.driver.cores", "2")
  .config("spark.sql.shuffle.spill", "true") // Enable spill to disk
  .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

// UDF to convert IPv4 to binary
def ipv4ToBinary(ip: String): String = {
  try {
    InetAddress.getByName(ip).getAddress.map(byte => f"${byte & 0xFF}%8s".replace(' ', '0')).mkString
  } catch {
    case _: UnknownHostException => ""
  }
}

// UDF to convert IPv6 to binary
def ipv6ToBinary(ip: String): String = {
  try {
    val bytes = InetAddress.getByName(ip).getAddress
    val bigInt = new BigInteger(1, bytes)
    String.format("%0128d", new BigInteger(bigInt.toString(2)))
  } catch {
    case _: UnknownHostException => ""
  }
}

// Register UDFs
val ipv4ToBinaryUDF = udf(ipv4ToBinary _)
val ipv6ToBinaryUDF = udf(ipv6ToBinary _)

// Load GeoLite2-City data
val geoDataParquetPath = "/opt/hadoop/rubigdata/GeoLite2-City.parquet"
val geoDataDF = spark.read.parquet(geoDataParquetPath)
  .withColumn("is_ipv4", col("network").contains("."))
  .withColumn("binary_network", when(col("is_ipv4"), ipv4ToBinaryUDF(split(col("network"), "/").getItem(0)))
    .otherwise(ipv6ToBinaryUDF(split(col("network"), "/").getItem(0))))
  .withColumn("geo_prefix_length", split(col("network"), "/").getItem(1).cast(IntegerType))
  .select("binary_network", "geo_prefix_length", "latitude", "longitude")
  .cache()

// Load WARC file and extract IP addresses
val warcPath = "/opt/hadoop/rubigdata/course.warc.gz"
val warcData = spark.sparkContext.textFile(warcPath)

val ipPattern = """(?:\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b|\b(?:[0-9a-fA-F]{1,4}:){1,7}[0-9a-fA-F]{1,4}\b)""".r
val ipAddresses = warcData.flatMap(record => ipPattern.findAllIn(record).toList)

val ipSchema = StructType(Array(StructField("ip", StringType, nullable = false)))
val ipDF = spark.createDataFrame(ipAddresses.map(Row(_)), ipSchema)
  .withColumn("is_ipv4", col("ip").contains("."))
  .withColumn("binary_ip", when(col("is_ipv4"), ipv4ToBinaryUDF(col("ip")))
    .otherwise(ipv6ToBinaryUDF(col("ip"))))
  .repartition(1000) // Repartition to improve parallelism

// Use a broadcast join for smaller dataset (Geo Data)
val broadcastGeoDataDF = broadcast(geoDataDF)

// Perform the join using broadcast join strategy
val joinedDF = ipDF.alias("ip")
  .join(broadcastGeoDataDF.alias("geo"), expr("substring(ip.binary_ip, 1, geo.geo_prefix_length) = geo.binary_network"))
  .withColumn("row_number", row_number().over(Window.partitionBy("ip.ip").orderBy(col("geo.geo_prefix_length").desc)))
  .filter(col("row_number") === 1)
  .select("ip.ip", "geo.latitude", "geo.longitude")

// Show top 20 rows for verification
joinedDF.show(20, false)

// Debugging: Count the number of joined records
println(s"Number of joined records: ${joinedDF.count()}")

// Group by ip, latitude, and longitude, and count occurrences (frequency)
val aggregatedDF = joinedDF.groupBy("ip", "latitude", "longitude")
  .agg(count("*").alias("frequency"))

// Save the aggregated data to a CSV file
val aggregatedDataPath = "/opt/hadoop/rubigdata/IP_Frequency_Data"
aggregatedDF.coalesce(1).write
  .mode("overwrite")
  .option("header", "true")
  .csv(aggregatedDataPath)

println(s"Aggregated data successfully saved to $aggregatedDataPath")

spark.stop()