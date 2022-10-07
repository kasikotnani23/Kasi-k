package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.io.Source
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.hive.HiveContext
import java.time.{ZonedDateTime,ZoneId}
import java.time.format.DateTimeFormatter

object sparkproject1 {

	def main(args:Array[String]):Unit={
			println("====started=====")
			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			val spark = SparkSession
			.builder()
			.config(conf)
			.getOrCreate()
			import spark.implicits._

			val day = ZonedDateTime.now(ZoneId.of("UTC"))
			val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
			val today = formatter format day
			
			println("====================="+today)

			println("===============step1===================================================")

			val avrodf = spark.read.format("avro")
			.load(s"file:///D:/DATA/spark_project/$today")

			avrodf.show(10) 

			println("===============step2===================================================")

			val html = Source.fromURL("https://randomuser.me/api/0.8/?results=500")
			val urldata= html.mkString
			val rdd = sc.parallelize(List(urldata))
			val urldf = spark.read.json(rdd)
			urldf.show()
			urldf.printSchema()
			println("===============step3===================================================")

			val flattendf = urldf.withColumn("results",explode(col("results")))
			.select(
					col("nationality"),
					col("results.user.cell"),
					col("results.user.dob"),
					col("results.user.email"),
					col("results.user.gender"),
					col("results.user.location.*"),
					col("results.user.md5"),
					col("results.user.name.*"),
					col("results.user.password"),
					col("results.user.phone"),
					col("results.user.picture.*"),
					col("results.user.registered"),
					col("results.user.salt"),
					col("results.user.sha1"),
					col("results.user.sha256"),
					col("results.user.username"),
					col("seed"),
					col("version"))

			flattendf.show(10)
			flattendf.printSchema()

			println("===============step4 removing numericals ===================================================")

			val rmdf = flattendf.withColumn("username",regexp_replace(col("username"),  "([0-9])", ""))
			rmdf.show(10)

			println("===============step5 joinig to data frames===================================================")

			val joindf = avrodf.join(rmdf, Seq("username"), "left")
			joindf.show(10)

			println("===============step6 nationality is not null===================================================")

			val availabledf = joindf.filter(col("nationality").isNotNull)
			availabledf.show(10)
			joindf.persist()

			println("===============step7 nationality is null ===================================================")

			val unavailabledf = joindf.filter(col("nationality").isNull)
			unavailabledf.show(10)

			println("===============step8 replacing null values ===================================================")

			val modifydf = unavailabledf.na.fill("Not Available")
			.na.fill(0)
			modifydf.show(10)	

			println("===============step9 adding current date ===================================================")

			val finaldf1 = availabledf.withColumn("current-date",current_date)

			val finaldf2 = modifydf.withColumn("current-date",current_date)

			finaldf1.show(10)
			finaldf2.show(10)





	}

}