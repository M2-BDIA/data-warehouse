import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.util.Properties

object App {
	def main(args: Array[String]) {

		/************************************************
			Paramètres du programme et initialisation
		************************************************/

		// Chargement des variables d'environnement depuis le fichier .env
		val env = scala.collection.mutable.Map[String, String]()
		scala.io.Source.fromFile(".env").getLines.foreach { line =>
			val splits = line.split("=")
			if (splits.length == 2) {
				env += (splits(0) -> splits(1))
			}
		}

		// Paramètres de connexion à la base de données Postgres 
		// pour la récupération les données
		// Class.forName("org.postgresql.Driver")
		val urlPostgres = env("POSTGRES_DATABASE_URL")
		val connectionPropertiesPostgres = new Properties()
		connectionPropertiesPostgres.setProperty("driver", "org.postgresql.Driver")
		connectionPropertiesPostgres.setProperty("user", env("POSTGRES_DATABASE_USER"))
		connectionPropertiesPostgres.setProperty("password", env("POSTGRES_DATABASE_PASSWORD"))

		// Paramètres de connexion à la base de données Oracle 
		// pour stocker le résultat de l'ETL
		// Class.forName("oracle.jdbc.driver.OracleDriver")
		val urlOracle = env("ORACLE_DATABASE_URL")
		val connectionPropertiesOracle = new Properties()
		connectionPropertiesOracle.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
		connectionPropertiesOracle.setProperty("user", env("ORACLE_DATABASE_USER"))
		connectionPropertiesOracle.setProperty("password", env("ORACLE_DATABASE_PASSWORD"))

		// Initialisation de Spark
		val spark = SparkSession.builder
			.appName("ETL")
			.master("local[4]")
			// .config("spark.executor.memory", "8g")
			.getOrCreate()


		/*****************************************************************
			Récupération de données depuis la base de données postgres
		******************************************************************/

		// Recupération des données de la table review
		/*
		Après vérification, même si les attributs review_id et business_id ne sont pas définis comme clé primaire / étrangère 
		dans la base, ils ne sont jamais null
		*/
		val query =
		"""
			|SELECT review_id, user_id, business_id, stars, date
			|FROM yelp.review
			|WHERE business_id is not null
			|LIMIT 10
		""".stripMargin

		// val reviews = spark.read.jdbc(urlPostgres, query, connectionPropertiesPostgres)
		val reviews = spark.read.jdbc(urlPostgres, s"($query) as review", connectionPropertiesPostgres)

		// affichage du schéma
		reviews.printSchema()

		// recuperation de données
		//reviews.limit(10).show()



		/**************************************************************
			Récupération de données depuis le fichier business.json 
		***************************************************************/

		// Accès local au fichier
		val businessFile = env("BUSINESS_FILE_PATH")

		// Chargement du fichier JSON
		val business = spark.read.json(businessFile).cache()

		// affichage du schéma
		business.printSchema()


		/**************************************************************
			Récupération de données depuis le fichier checkin.json 
		***************************************************************/

		// Accès local au fichier
		val checkinsFile = env("CHECKIN_FILE_PATH")

		// Chargement du fichier JSON
		val checkins = spark.read.json(checkinsFile).cache()

		// affichage du schéma
		checkins.printSchema()

		// Extraction des business_id
		// var business_id = users.select("business_id")



	
		/*********************************************************
			Récupération de données depuis le fichier tip.csv 
		**********************************************************/

		// Accès local au fichier
		val tipsFile = env("TIP_FILE_PATH")

		// Chargement du fichier CSV
		val tips = spark.read.format("csv").option("header", "true").load(tipsFile).cache()

		// affichage du schéma
		tips.printSchema()




		/***************************************************************
			Ajout de données à la base de données Oracle eluard2023
		****************************************************************/

		// Enregistrement du DataFrame reviews dans la table "reviews"
		reviews.write
			.mode(SaveMode.Overwrite).jdbc(urlOracle, "reviews", connectionPropertiesOracle)


		


		// Arrêt de la session Spark
		spark.stop()


	}
}
