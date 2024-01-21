import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.util.Properties

object App {
	def main(args: Array[String]) {

		// Initialisation de Spark
		val spark = SparkSession.builder
			.appName("ETL")
			.master("local[4]")
			// .config("spark.executor.memory", "8g")
			.getOrCreate()

		// Chargement des données
		

		// Arrêt de la session Spark
		spark.stop()
	}
}
