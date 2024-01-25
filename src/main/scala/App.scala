import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.util.Properties

object App {

	// Fonction qui prend en entrée une liste de strings et une liste de mots
	// et retourne true si au moins un des mots est présent dans au moins une des chaînes de caractères
	def isWordsInStrings(strings: Seq[String], words: Seq[String]): Boolean = {
		for (str <- strings) {
			if(words.exists(word => str.contains(word))) {
				return true
			}
		}
		return false
	}

	// Fonction qui prend en entrée un string contenant des valeurs séparées par une virgule
	// et retourne une des valeurs sous forme de string
	def getMainCategorie(str_categories: String): String = {
		// Récupération des catégories dans un tableau en supprimant les majuscules
		var categories = str_categories.toLowerCase().split(",")

		// On retourne une chaîne vide si la liste est vide
		if (categories.length == 0) {
			return ""
		}

		// Recherche de catégories prédéfinies
		// Il faut que les catégories soient dans l'ordre de priorité...
		// Les mots peuvent être présents avec des suffixes ou des préfixes

		// On cherche si une des catégories est relative à la restauration / vente de nourriture ou de boissons
		var motsRestauration = Seq("restau", "food", "drink", "bar", "cafe", "coffee", "tea", "breakfast", "brunch", "lunch", "dinner", "bistro", "pub", "brewery", "beer", "wine", "bakery", "cuisine", "caterer")
		// if(isWordsInStrings(categories, motsRestauration)) {
		// 	return "restaurant"
		// }
		for (categorie <- categories) {
			if(motsRestauration.exists(mot => categorie.contains(mot))) {
				return "restauration"
			}
		}

		// On cherche si une des catégories est relative à la vente et au commerce
		val motsCommerce = Seq("shop", "store", "market", "market", "boutique", "commerce", "commercial", "retail", "sale", "buy", "purchase", "sell", "vendor", "dealer", "mall", "grocery", "pharmacy", "drugstore", "bookstore", "library", "florist")
		// if(isWordsInStrings(categories, motsCommerce)) {
		// 	return "shopping"
		// }
		for (categorie <- categories) {
			if(motsCommerce.exists(mot => categorie.contains(mot))) {
				return "shopping"
			}
		}

		// On cherche si une des catégories est relative à la santé / médecine
		val motsSante = Seq("health", "medica", "salon", "clinic", "hospital", "doctor", "dentist", "pharma", "medicin")
		// if(isWordsInStrings(categories, motsSante)) {
		// 	return "medical"
		// }
		for (categorie <- categories) {
			if(motsSante.exists(mot => categorie.contains(mot))) {
				return "medical"
			}
		}
		

		// On cherche si une des catégories est relative au soin du corps / beauté
		val motsSoins = Seq("beauty", "spa", "hair", "nail", "tattoo", "piercing", "wax", "massage", "salon", "life")
		// if(isWordsInStrings(categories, motsSoins)) {
		// 	return "beauty"
		// }
		for (categorie <- categories) {
			if(motsSoins.exists(mot => categorie.contains(mot))) {
				return "beauty"
			}
		}

		// On cherche si une des catégories est relative à l'automobile / réparation
		val motsAutomobile = Seq("auto", "moto", "car", "repair", "garage", "mechanic", "tire", "wheel")
		// if(isWordsInStrings(categories, motsAutomobile)) {
		// 	return "automobile"
		// }
		for (categorie <- categories) {
			if(motsAutomobile.exists(mot => categorie.contains(mot))) {
				return "automobile"
			}
		}

		// On cherche si une des catégories est relative aux lieux de culte
		val motsReligion = Seq("church", "temple", "mosque", "synagogue", "religio", "spiritual", "faith", "worship", "pray")
		// if(isWordsInStrings(categories, motsReligion)) {
		// 	return "religion"
		// }
		for (categorie <- categories) {
			if(motsReligion.exists(mot => categorie.contains(mot))) {
				return "religion"
			}
		}

		// On cherche si une des catégories est relative aux loisirs / divertissements
		val motsLoisirs = Seq("entertainment", "fun", "leisure", "recreation", "game", "play", "sport", "gym", "hobby", "activity", "event", "party", "dance", "music", "concert", "movie", "film", "theater", "cinema", "show", "amusement", "park", "zoo", "museum", "art", "gallery", "exhibit", "exhibition", "tourism", "tourist", "travel", "voyage", "vacation", "holiday", "trip", "visit", "sightseeing", "sightsee", "attraction", "tour", "excursion", "cruise", "cruising", "carnival", "festival")
		// if(isWordsInStrings(categories, motsLoisirs)) {
		// 	return "entertainment"
		// }
		for (categorie <- categories) {
			if(motsLoisirs.exists(mot => categorie.contains(mot))) {
				return "entertainment"
			}
		}

		// On cherche si une des catégories est relative aux services
		val motsService = Seq("service", "event", "planning", "photo", "repair", "mobile", "electronics", "computer", "clean", "wash", "laundry", "dry", "cleaning", "delivery", "shipping", "transport", "moving", "storage", "logistic")
		// if(isWordsInStrings(categories, motsService)) {
		// 	return "service"
		// }
		for (categorie <- categories) {
			if(motsService.exists(mot => categorie.contains(mot))) {
				return "service"
			}
		}


		// Si on ne trouve pas de catégorie parmi celles prédéfinies, on retourne la première
		return categories(0)
	}

	def main(args: Array[String]) {

		/************************************************
			Paramètres du programme et initialisation
		************************************************/

		// Création de la fonction UDF qui permettra d'extraire la catégorie principale d'un business
		val getMainCategorieUDF = udf(getMainCategorie _)

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


		// Les données de la table "user" sont récupérées dans la partie "Ajout de données au data warehouse - base de données Oracle"
		// car on n'effectue pas de transformation dessus


		/**************************************************************
			Récupération de données depuis le fichier business.json 
		***************************************************************/

		// Accès local au fichier
		val businessFile = env("BUSINESS_FILE_PATH")

		// Chargement du fichier JSON
		var business = spark.read.json(businessFile).cache()

		// On ne conserve que les 200 premières lignes pour tester
		// business = business.limit(200)

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

		// Extraction des dates, qui formeront une table "date"
		val first_checkins = checkins.limit(10)
		var visitDates = first_checkins.withColumn("date", explode(org.apache.spark.sql.functions.split(col("date"), ",")))

		// Formatage des dates pour ne pas avoir plus de précision que le jour (YYYY-MM-DD) tout en gardant le business_id
		visitDates = visitDates.withColumn("date", regexp_replace(col("date"), "\\d{2}:\\d{2}:\\d{2}", ""))

		// On affiche les 10 premières lignes
		// visitDates.show(10)

		// On compte le nombre de valeurs (visites) par combinaison business_id - date 
		val nb_visites_by_date_and_business = visitDates.groupBy("business_id", "date").count()
												.withColumnRenamed("count", "nb_visites")

		// Et le nombre de visites par business_id
		val nb_visites_by_business = nb_visites_by_date_and_business.groupBy("business_id").sum("nb_visites")
												.withColumnRenamed("sum(nb_visites)", "nb_visites")

		// Top 10
		// nb_visites_by_date_and_business.orderBy(desc("nb_visites")).show(10)
		// nb_visites_by_business.orderBy(desc("nb_visites")).show(10)


		/*********************************************************
			Récupération de données depuis le fichier tip.csv 
		**********************************************************/

		// Accès local au fichier
		val tipsFile = env("TIP_FILE_PATH")

		// Chargement du fichier CSV
		var tips = spark.read.format("csv").option("header", "true").load(tipsFile).cache()

		// On ne garde que la colonne business_id
		tips = tips.select("business_id")

		// affichage du schéma
		// tips.printSchema()

		// recuperation de données
		// tips.limit(10).show()

		// On compte le nombre de tips par business_id
		val nb_tips_by_business = tips.groupBy("business_id").count().withColumnRenamed("count", "nb_tips")

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
		val nb_reviews_by_business = reviews.groupBy("business_id").count().withColumnRenamed("count", "nb_reviews")

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


		// Extraction des catégories
		var business_categories = business.select("business_id", "categories")

		// Suppression des lignes sans catégories (pose problème pour la fonction getMainCategorieUDF)
		business_categories = business_categories.filter(col("categories").isNotNull)
		
		// On crée une colonne "main_categorie" qui contient la catégorie principale du business
		var business_main_categorie = business_categories.withColumn("main_categorie", getMainCategorieUDF(col("categories")))
											.drop(col("categories"))

		// Affichage des 10 premières lignes
		// business_main_categorie.show(100)


		// Suppression des colonnes inutiles dans le dataframe "business"
		business = business.drop(col("categories"))
							.drop(col("attributes"))
							.drop(col("hours"))

				
		// On joint les DataFrames "business" et "business_main_categorie" pour ajouter la colonne "main_categorie" à "business"
		// Jointure externe pour ne pas perdre les business qui n'ont pas de catégorie principale
		business = business.join(business_main_categorie, business("business_id") === business_main_categorie("business_id"), "left_outer")
							.drop(business_main_categorie("business_id"))

		// On libère la mémoire
		business_categories.unpersist()
		business_main_categorie.unpersist()


		// On joint les DataFrames "business" et "nb_visites_by_business" pour ajouter la colonne "nb_visites" à "business"
		// Jointure externe pour ne pas perdre les business qui n'ont pas de nombre de visites
		business = business.join(nb_visites_by_business, business("business_id") === nb_visites_by_business("business_id"), "left_outer")
							.drop(nb_visites_by_business("business_id"))

		// On libère la mémoire
		nb_visites_by_business.unpersist()


		// On joint les DataFrames "business" et "openDays" pour ajouter les colonnes "is_open_xxx" à "business"
		// Jointure externe pour ne pas perdre les business qui n'ont pas d'information sur les jours d'ouverture
		business = business.join(openDays, business("business_id") === openDays("business_id"), "left_outer")
							.drop(openDays("business_id"))

		// On renomme la colonne "stars" en "avg_stars" pour plus de clarté
		business = business.withColumnRenamed("stars", "avg_stars")

		// On libère la mémoire
		openDays.unpersist()


		// On joint les DataFrames "business" et "nb_reviews_by_business" pour ajouter la colonne "nb_reviews" à "business"
		// Jointure externe pour ne pas perdre les business qui n'ont pas d'information sur les jours d'ouverture
		business = business.join(nb_reviews_by_business, business("business_id") === nb_reviews_by_business("business_id"), "left_outer")
							.drop(nb_reviews_by_business("business_id"))

		// On joint les DataFrames "business" et "nb_tips_by_business" pour ajouter la colonne "nb_tips" à "business"
		// Jointure externe pour ne pas perdre les business qui n'ont pas d'information sur les jours d'ouverture
		business = business.join(nb_tips_by_business, business("business_id") === nb_tips_by_business("business_id"), "left_outer")
							.drop(nb_tips_by_business("business_id"))


		// Pour tester :
		// business.show(20)

		// On libère la mémoire
		nb_reviews_by_business.unpersist()
		nb_tips_by_business.unpersist()



		/******************************************************************
			Ajout de données au data warehouse - base de données Oracle
		*******************************************************************/

		// Enregistrement du DataFrame date dans la table "date"
		// dates.write.mode(SaveMode.Overwrite).jdbc(urlOracle, "date", connectionPropertiesOracle)
		// Libération de la mémoire
		// dates.unpersist()


		// Enregistrement du DataFrame reviews dans la table "review"
		// reviews.write.mode(SaveMode.Overwrite).jdbc(urlOracle, "review", connectionPropertiesOracle)
		// Libération de la mémoire
		// reviews.unpersist()


		// Recupération des données de la table "user" depuis la base de données Postgres
		// et enregistrement dans la table "users" de la base de données Oracle
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

		// Enregistrement du DataFrame user dans la table "user"
		// users.write.mode(SaveMode.Overwrite).jdbc(urlOracle, "user", connectionPropertiesOracle)
		// Libération de la mémoire
		// users.unpersist()



		// Arrêt de la session Spark
		spark.stop()


	}
}
