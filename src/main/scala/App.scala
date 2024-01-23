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
		val queryGetReviews =
		"""
			|SELECT review_id, user_id, business_id, stars, date
			|FROM yelp.review
			|WHERE business_id is not null
			|LIMIT 10
		""".stripMargin

		var reviews = spark.read.jdbc(urlPostgres, s"($queryGetReviews) as review", connectionPropertiesPostgres)

		// affichage du schéma
		// reviews.printSchema()

		// recuperation de données
		//reviews.limit(10).show()


		// Recupération des données de la table "user"
		val queryGetUsers =
		"""
			|SELECT user_id, review_count, yelping_since
			|FROM yelp."user"
			|WHERE user_id is not null
			|LIMIT 10
		""".stripMargin

		// val users = spark.read.jdbc(urlPostgres, s"($queryGetUsers) as review", connectionPropertiesPostgres)

		// affichage du schéma
		// users.printSchema()

		// recuperation de données
		// users.limit(10).show()


		/**************************************************************
			Récupération de données depuis le fichier business.json 
		***************************************************************/

		// Accès local au fichier
		val businessFile = env("BUSINESS_FILE_PATH")

		// Chargement du fichier JSON
		var business = spark.read.json(businessFile).cache()

		// affichage du schéma
		// business.printSchema()


		/**************************************************************
			Récupération de données depuis le fichier checkin.json 
		***************************************************************/

		// Accès local au fichier
		val checkinsFile = env("CHECKIN_FILE_PATH")

		// Chargement du fichier JSON
		val checkins = spark.read.json(checkinsFile).cache()

		// affichage du schéma
		// checkins.printSchema()

		/*
		 * Le fichier checkin.json contient une colonne "business_id" avec l'id du business
		 * et une colonne "date" qui contient une liste de dates de visites sous la forme d'un string.
		 * Nous avons besoin d'extraire ces dates de la string. 
		 */

		// Extraction des dates, qui formeront une table "business_id - date_id"
		// val first_checkins = checkins.limit(10)
		// val visitDates = first_checkins.withColumn("date", explode(org.apache.spark.sql.functions.split(col("date"), ",")))

		// Formatage des dates pour ne pas avoir plus de précision que le jour (YYYY-MM-DD) tout en gardant le business_id
		// val visitDatesReformat = visitDates.withColumn("date", regexp_replace(col("date"), "\\d{2}:\\d{2}:\\d{2}", ""))

		// On affiche les 10 premières lignes
		// visitDatesReformat.show(10)

		// On compte le nombre de valeurs (visites) par combinaison business_id - date 
		// val nb_visites_by_date_and_business = visitDatesReformat.groupBy("business_id", "date").count()
		// 										.withColumnRenamed("count", "nb_visites")

		// Et le nombre de visites par business_id
		// val nb_visites_by_business = nb_visites_by_date_and_business.groupBy("business_id").sum("nb_visites")
		// 										.withColumnRenamed("sum(nb_visites)", "nb_visites")

		// Top 10
		// nb_visites_by_date_and_business.orderBy(desc("nb_visites")).show(10)
		// nb_visites_by_business.orderBy(desc("nb_visites")).show(10)


		/*********************************************************
			Récupération de données depuis le fichier tip.csv 
		**********************************************************/

		// Accès local au fichier
		val tipsFile = env("TIP_FILE_PATH")

		// Chargement du fichier CSV
		// val tips = spark.read.format("csv").option("header", "true").load(tipsFile).cache()

		// On ne garde que la colonne business_id
		// val tips = tips.select("business_id")

		// affichage du schéma
		// tips.printSchema()

		// recuperation de données
		// tips.limit(10).show()

		// On compte le nombre de tips par business_id
		// val nb_tips_by_business = tips.groupBy("business_id").count()

		// Top 10
		// nb_tips_by_business.orderBy(desc("count")).show(10)

		// Nombre total de tips	-> 1 363 162
		// val nb_tips_total = tips.count()
		// println(s"Nombre total de tips : $nb_tips_total")




		/***********************************
			Transformations des données
		************************************/

		/*
		 * A partir des reviews
		 * Peu de transformations à faire, il faut juste remplacer la colonne "date" par une colonne "date_id"
		 * et stocker la date dans une table de dimension "date"
		*/

		// On crée une table de dimension "date" à partir de la colonne "date" de la table "reviews"
		// et on ajoute une colonne "date_id" qui sera la clé primaire de la table
		var dates = reviews.select("date").distinct()
		dates = dates.withColumn("date_id", monotonically_increasing_id())

		// On modifie la colonne "date" de la table "reviews" pour qu'elle contienne la clé primaire de la table "dates"
		reviews = reviews.join(dates, reviews("date") === dates("date"))
						.drop(reviews("date"))
						.drop(col("date"))

		// Pour supprimer les lignes sans identifiant - normalement il n'y en a pas
		reviews = reviews.filter(col("review_id").isNotNull)
						.filter(col("user_id").isNotNull)
						.filter(col("business_id").isNotNull)
						.filter(col("date_id").isNotNull)

		// On affiche les 10 premières lignes
		// reviews.show(10)


		// On compte le nombre de review par business_id
		// Normalement c'est ce qui est contenu dans l'attribut "review_count" de la table "business"
		// val nb_reviews_by_business = reviews.groupBy("business_id").count()

		// Top 10
		// nb_reviews_by_business.orderBy(desc("count")).show(10)



		/*
		 * A partir des business
		 * Le plus dur est de déterminer la catégorie principale du business, et la ou les catégories secondaires
		 * Cela se fait en fonction de la colonne "categories" qui contient une liste de catégories séparées par une virgule
		 * et de la colonne "attributes" qui contient un objet JSON avec des attributs et leurs valeurs
		*/

		// Extraction de l'information de l'ouverture du business par jour
		// La colonne "hours" contient un objet JSON avec les jours de la semaine et les horaires d'ouverture
		var openDays = business.select("business_id", "hours")
		// On crée une colonne par jour de la semaine qui contient un booléen indiquant si le business est ouvert ce jour là
		openDays = openDays.withColumn("is_open_monday", col("hours.Monday").isNotNull)
				.withColumn("is_open_tuesday", col("hours.Tuesday").isNotNull)
				.withColumn("is_open_wednesday", col("hours.Wednesday").isNotNull)
				.withColumn("is_open_thursday", col("hours.Thursday").isNotNull)
				.withColumn("is_open_friday", col("hours.Friday").isNotNull)
				.withColumn("is_open_saturday", col("hours.Saturday").isNotNull)
				.withColumn("is_open_sunday", col("hours.Sunday").isNotNull)
				.drop(col("hours"))

		// On affiche les 10 premières lignes
		openDays.show(10)




		/******************************************************************
			Ajout de données au data warehouse - base de données Oracle
		*******************************************************************/

		// Enregistrement du DataFrame reviews dans la table "reviews"
		// reviews.write.mode(SaveMode.Overwrite).jdbc(urlOracle, "reviews", connectionPropertiesOracle)


		


		// Arrêt de la session Spark
		spark.stop()


	}
}
