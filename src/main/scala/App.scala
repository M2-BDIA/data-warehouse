import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}

import java.util.Properties

object App {

	// Implémentation de la classe JdbcDialect pour le dialecte Oracle
	// Pour réfinir le mapping entre les types de données Spark et les types de données Oracle
	class OracleDialect extends JdbcDialect {
		override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
			case DoubleType => Some(JdbcType("NUMBER(20,10)", java.sql.Types.NUMERIC))
			// NUMBER(20,10) -> 20 chiffres au total, 10 chiffres après la virgule
			// besoin d'autant de chiffres avant la virgule pour stocker des valeurs de latitude et longitude
			case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.NUMERIC))
			case IntegerType => Some(JdbcType("NUMBER(10)", java.sql.Types.INTEGER))
			case LongType => Some(JdbcType("NUMBER(19)", java.sql.Types.BIGINT))
			case FloatType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.FLOAT))
			case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.SMALLINT))
			case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.SMALLINT))
			case StringType => Some(JdbcType("VARCHAR2(255)", java.sql.Types.VARCHAR))
			case DateType => Some(JdbcType("DATE", java.sql.Types.DATE))
			case _ => None
		}
		override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle")
		override def quoteIdentifier(colName: String): String = s""""$colName""""
	}

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

	// Fonction qui effectue une recherche de sous-chaînes de caractères parmis celles données en entrée. 
	// En entrée : un string contenant des valeurs separees par une virgule et retourne une valeur sous forme de string
	def getMainCategory(str_categories: String): String = {
		// Récupération des catégories dans un tableau en supprimant les majuscules
		var categories = str_categories.toLowerCase().split(",")

		// On retourne une chaîne vide si la liste est vide
		if (categories.length == 0) {
			return ""
		}

		// Recherche de catégories prédéfinies
		// Il faut que les catégories soient dans l'ordre de priorité...
		// Les mots peuvent être présents avec des suffixes ou des préfixes

		// On cherche si une des catégories contient le mot "service"
		var motsService = Seq("service")
		if(isWordsInStrings(categories, motsService)) {
			return "service"
		}

		// On cherche si une des catégories contient le mot "pet"
		var motsAnimaux = Seq("pet")
		if(isWordsInStrings(categories, motsAnimaux)) {
			return "animal"
		}	

		var motsAutomobile = Seq("automotive")
		if(isWordsInStrings(categories, motsAutomobile)) {
			return "automotive"
		}

		// On cherche si une des catégories est relative à la restauration / vente de nourriture ou de boissons
		var motsRestauration = Seq("restau", "food", "drink", "cafe", "coffee", "tea", "breakfast", "brunch", "lunch", "dinner", "bistro", "pub", "brewery", "beer", "wine", "bakery", "cuisine", "caterer")
		if(isWordsInStrings(categories, motsRestauration)) {
			return "restaurant"
		}

		// On cherche si une des catégories est relative à la santé / médecine
		val motsSante = Seq("health", "medica", "sante", "clinic", "clinique", "hospital", "doctor", "dentist", "pharma", "medicin")
		if(isWordsInStrings(categories, motsSante)) {
			return "medical"
		}

		// On cherche si une des catégories est relative à l'enseignement / éducation
		val motsEcole = Seq("school", "university", "college", "campus", "ecole", "academy", "educat", "stud")
		if(isWordsInStrings(categories, motsEcole)) {
			return "education"
		}

		// On cherche si une des catégories est relative à la vente et au commerce
		val motsCommerce = Seq("shop", "store", "market", "market", "boutique", "commerce", "commercial", "retail", "sale", "buy", "purchase", "sell", "vendor", "dealer", "mall", "grocery", "pharmacy", "drugstore", "bookstore", "library", "florist")
		if(isWordsInStrings(categories, motsCommerce)) {
			return "shopping"
		}

		// On cherche si une des catégories est relative aux lieux de culte
		val motsReligion = Seq("church", "temple", "mosque", "synagogue", "religio", "spiritual", "faith", "worship")
		if(isWordsInStrings(categories, motsReligion)) {
			return "religion"
		}

		// On cherche si une des catégories est relative aux loisirs / divertissements
		var motsLoisirs = Seq("entertainment", "karaoke", "fun", "leisure", "recreation", "game", "hotel", "event", "play", "sport", "fitness", "gym", "hobby", "activity", "event", "party", "dance", "music", "concert", "movie", "film", "theater", "cinema", "show", "amusement", "park", "zoo", "museum", "art", "gallery", "exhibit", "exhibition", "tourism", "tourist", "travel", "voyage", "vacation", "holiday", "trip", "visit", "sightseeing", "sightsee", "attraction", "tour", "excursion", "cruise", "cruising", "carnival", "festival")
		if(isWordsInStrings(categories, motsLoisirs)) {
			return "entertainment"
		}

		// On cherche si une des catégories est relative au soin du corps / beauté
		val motsSoins = Seq("beauty", "spa", "hair", "barber", "nail", "tattoo", "piercing", "wax", "massage")
		if(isWordsInStrings(categories, motsSoins)) {
			return "beauty"
		}

		// On cherche si une des catégories est relative à l'automobile
		motsAutomobile = Seq("auto", "moto", "car", "garage", "mechanic", "tire", "wheel")
		if(isWordsInStrings(categories, motsAutomobile)) {
			return "automotive"
		}

		// On cherche si une des catégories est relative aux services (sans dire expréssement "service")
		motsService = Seq("event", "planning", "photo", "repair", "mobile", "electronics", "computer", "clean", "wash", "laundry", "dry", "cleaning", "delivery", "shipping", "transport", "moving", "storage", "logistic")
		if(isWordsInStrings(categories, motsService)) {
			return "service"
		}

		// On cherche si une des catégories est relative aux animaux
		motsAnimaux = Seq("pet", "animal", "veterinar", "zoo", "dog", "cat")
		if(isWordsInStrings(categories, motsAnimaux)) {
			return "animal"
		}	

		// On cherche si une des catégories contient le mot "life"
		motsLoisirs = Seq("life")
		if(isWordsInStrings(categories, motsLoisirs)) {
			return "entertainment"
		}	

		// Si on ne trouve pas de catégorie parmi celles prédéfinies, on retourne la chaîne "other"
		// Cela permettra de récupérer facilement les business qui n'ont pas de catégorie principale
		return "other"
	}


	def main(args: Array[String]) {

		/************************************************
			Paramètres du programme et initialisation
		************************************************/

		// Définition du dialecte Oracle
		// utilisé par la suite pour écrire les données dans la base de données Oracle
		// uniquement pour la table de dimension "business"
		val dialect = new OracleDialect

		// Création de la fonction UDF (User-Defined Function) qui permettra d'extraire la catégorie principale d'un business
		val getMainCategoryUDF = udf(getMainCategory _)

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

		val queryGetReviews =
		"""
			|SELECT	review_id, user_id, business_id, stars, date
			|FROM 	yelp.review
			|WHERE 	business_id is not null
		""".stripMargin

		var reviews = spark.read
			// .option("numPartitions", 50)
			.jdbc(urlPostgres, s"($queryGetReviews) as review", connectionPropertiesPostgres)

		// affichage du schéma
		// reviews.printSchema()

		
		// Recupération des données de la table "user"
		// On ne garde que les utilisateurs qui ont laissé au moins une review

		val queryGetUsers =
		"""
			|SELECT	DISTINCT u.user_id, review_count, yelping_since
			|FROM	yelp.user as u INNER JOIN yelp.review as r
			|ON		r.user_id = u.user_id
		""".stripMargin

		var users = spark.read
			// .option("numPartitions", 50)
			.jdbc(urlPostgres, s"($queryGetUsers) as users", connectionPropertiesPostgres)

		// affichage du schéma
		// users.printSchema()


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
		var checkins = spark.read.json(checkinsFile).cache()

		// affichage du schéma
		// checkins.printSchema()



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



		/***********************************
			Transformations des données
		************************************/

		/*
		 * A partir des checkins
		 * Le fichier checkin.json contient une colonne "business_id" avec l'id du business
		 * et une colonne "date" qui contient une liste de dates de visites sous la forme d'un string.
		 * Nous avons besoin d'extraire ces dates de la string. 
		 */

		// Extraction des dates de visites
		var visits = checkins.withColumn("date", explode(org.apache.spark.sql.functions.split(col("date"), ",")))

		// Formatage des dates pour ne pas avoir plus de précision que le jour (YYYY-MM-DD) tout en gardant le business_id
		visits = visits.withColumn("date", regexp_replace(col("date"), "\\d{2}:\\d{2}:\\d{2}", ""))

		// Supression des espaces si il y en a
		visits = visits.withColumn("date", regexp_replace(col("date"), " ", ""))

		// On extrait les dates de visites uniques pour les stocker dans une table de dimension "date" 
		val dates_visits = visits.select("date").distinct()

		// On poursuivra la transformation en créant une table de fait "visit" 
		// qui contiendra le nombre de visites par business_id et par date
		// après avoir récupéré les dates des review et génére les clés primaires de la table "date"


		/*
		 * A partir des reviews
		 * Peu de transformations à faire, il faut juste remplacer la colonne "date" par une colonne "date_id"
		 * et stocker la date dans une table de dimension "date"
		*/

		// On crée une table de dimension "date" à partir de la colonne "date" de la table "reviews"
		val dates_review = reviews.select("date").distinct()

		// On fait l'union des dataframes "dates_visits" et "dates_review" pour avoir une table de dimension "date" complète
		var dates = dates_visits.union(dates_review).distinct()

		// On ajoute une colonne "date_id" qui sera la clé primaire de la table "date"
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


		/*
		 * Retour aux checkins
		 * On compte le nombre de visites par business_id et par date pour créer la table de fait "visit"
		 * et on remplace la colonne "date" par une colonne "date_id" qui est une clé étrangère vers la table "date"
		*/

		// On modifie la colonne "date" de la table "visits" pour qu'elle contienne la clé primaire de la table "dates"
		visits = visits.join(dates, visits("date") === dates("date"))
						.drop(visits("date"))
						.drop(col("date"))

		// On compte le nombre de valeurs (visites) par combinaison business_id - date 
		val nb_visits_by_date_and_business = visits.groupBy("business_id", "date_id")
													.count()
													.withColumnRenamed("count", "nb_visits")

		// Et le nombre de visites par business_id pour la table de dimension "business"
		val nb_visits_by_business = nb_visits_by_date_and_business.groupBy("business_id")
																	.sum("nb_visits")
																	.withColumnRenamed("sum(nb_visits)", "nb_visits")

		// Suppression des doublons
		visits = visits.distinct()

		// On ajoute une colonne "visit_id" qui sera la clé primaire de la table "visits"
		// Cet identifiant technique est facultatif, la clé primaire peut être union des clés étrangères des tables de dimension
		visits = visits.withColumn("visit_id", monotonically_increasing_id())


		// On joint les DataFrames "visits" et "nb_visits_by_date_and_business" pour ajouter la colonne "nb_visits" à la table "visits"
		// Jointure externe pour ne pas perdre les visites qui n'ont pas de nombre de visites 
		// (normalement il y en au moins une sinon la valeur n'apparaîtrait pas dans la table "visits")
		visits = visits.join(
			nb_visits_by_date_and_business, 
			visits("business_id") === nb_visits_by_date_and_business("business_id") && 
			visits("date_id") === nb_visits_by_date_and_business("date_id"),
			"left_outer"
		)
		.drop(nb_visits_by_date_and_business("business_id"))
		.drop(nb_visits_by_date_and_business("date_id"))
				
		// Pour supprimer les lignes sans identifiant - normalement il n'y en a pas
		visits = visits.filter(col("visit_id").isNotNull)
						.filter(col("business_id").isNotNull)
						.filter(col("date_id").isNotNull)
		
		// // Affichage des 10 premières lignes
		// visits.show(10)

		// On libère la mémoire
		nb_visits_by_date_and_business.unpersist()


		/*
		 * A partir des tips
		 * On compte simplement le nombre de tips par business_id
		*/

		val nb_tips_by_business = tips.groupBy("business_id").count().withColumnRenamed("count", "nb_tips")

		// Affichage du top 10
		// nb_tips_by_business.orderBy(desc("count")).show(10)


		/*
		 * A partir des business
		 * Le plus dur est de déterminer la catégorie principale du business, et la ou les catégories secondaires
		 * Cela se fait en fonction de la colonne "categories" qui contient une liste de catégories séparées par une virgule
		 * et de la colonne "attributes" qui contient un objet JSON avec des attributs et leurs valeurs
		*/

		// Suppression de la casse de la colonne "city"
		business = business.withColumn("city", lower(col("city")))

		// Suppression des espaces dans la colonne "city"
		business = business.withColumn("city", regexp_replace(col("city"), " ", ""))


		// Pour supprimer les enregistrements sans valeur dans la colonne "name"
		// Il y en a qu'un seul dans le fichier business.json
		// On peut quand même le garder car utiliser son type de commerce peut servir
		// business = business.filter(col("name").isNotNull)


		// Certains business n'ont pas certaines informations de localisation
		// 8679 business n'ont pas d'adresse, 2 n'ont pas de ville, 509 n'ont pas de code postaux
		// Est-ce qu'on essaye de retrouver ces informations à partir de la latitude et de la longitude ?
		// C'est a priori faisable à partir de l'API de Google Maps en faisant un géocodage inversé, mais
		// c'est coûteux en temps, le nombre de requêtes est limité pour une version gratuite
		// On peut se contenter des villes pour les requêtes d'analyse


		// Gestion des doublons
		// Certains commerce ont le même nom et la même adresse, ou encore le même nom, la même latitude et la même longitude
		// Pour autant leur identifiant est différent et ils n'ont pas forcément le même nombre de reviews, la même note moyenne, etc.
		// Alors que faire ? 
		// On peut les fusionner en un seul commerce, mais quels critères prendre en compte pour choisir les valeurs à conserver ?
		// moyenne ? valeur la plus fréquente ? les valeurs du tuple dont le nombre d'avis est le plus élevé ?
		// Pour ce qui est des catégories, on peut les fusionner en une seule chaîne de caractères séparée par des virgules
		// On peut aussi les supprimer, mais on perd de l'information

		// Avec la fusion des commerces un problème survient : si un avis est émis ou une visite est effectuée sur un commerce que l'on a fusionné 
		// avec un autre car étant un doublon, il faut réaffecter l'identifiant du commerce que l'on conserve... pas simple !

		// Au final on décide de ne pas fusionner les commerces, on les laisse tels quels
		// Ce serait trop couûteux en temps et en ressources pour un résultat satisfaisant
		// D'autant que le nombre de doublons est très faible par rapport au nombre total de commerces (0.1% environ)

		// Doublons sur le nom et l'adresse si elle est renseignée
		// business = business.where("address is not null")
		// 					.groupBy("name", "address")
		// 					.agg(	
		// 							first("business_id") as "business_id", 
		// 							first("city") as "city",
		// 							first("state") as "state",
		// 							first("postal_code") as "postal_code",
		// 							first("latitude") as "latitude",
		// 							first("longitude") as "longitude",
		// 							avg("stars") as "stars",
		// 							avg("review_count") as "review_count",
		// 							first("is_open") as "is_open",
		// 							first("attributes") as "attributes",
		// 							concat_ws(",", collect_list("categories")) as "categories", 	// on concatène les catégories en une seule chaîne de caractères
		// 							first("hours") as "hours"
		// 						)

		// Doublons sur le nom, la latitude et la longitude
		// business = business.groupBy("name", "latitude", "longitude")
		// 					.agg(	
		// 							first("business_id") as "business_id", 
		// 							first("address") as "address",
		// 							first("city") as "city",
		// 							first("state") as "state",
		// 							first("postal_code") as "postal_code",
		// 							avg("stars") as "stars",
		// 							avg("review_count") as "review_count",
		// 							first("is_open") as "is_open",
		// 							first("attributes") as "attributes",
		// 							concat_ws(",", collect_list("categories")) as "categories", 	// on concatène les catégories en une seule chaîne de caractères
		// 							first("hours") as "hours"
		// 						)


		// Extraction de l'information de l'ouverture du business par jour
		// La colonne "hours" contient un objet JSON avec les jours de la semaine et les horaires d'ouverture
		var openDays = business.select("business_id", "hours")
		// On crée une colonne par jour de la semaine qui contient 1 si le business est ouvert ce jour là et 0 sinon
		openDays = openDays.withColumn("is_open_monday", when(col("hours.Monday").isNotNull, 1).otherwise(0))
							.withColumn("is_open_tuesday", when(col("hours.Tuesday").isNotNull, 1).otherwise(0))
							.withColumn("is_open_wednesday", when(col("hours.Wednesday").isNotNull, 1).otherwise(0))
							.withColumn("is_open_thursday", when(col("hours.Thursday").isNotNull, 1).otherwise(0))
							.withColumn("is_open_friday", when(col("hours.Friday").isNotNull, 1).otherwise(0))
							.withColumn("is_open_saturday", when(col("hours.Saturday").isNotNull, 1).otherwise(0))
							.withColumn("is_open_sunday", when(col("hours.Sunday").isNotNull, 1).otherwise(0))
							.drop(col("hours"))

		// Extraction des catégories
		var business_categories = business.select("business_id", "categories")

		// Suppression des lignes sans catégories (pose problème pour la fonction getMainCategorieUDF)
		business_categories = business_categories.filter(col("categories").isNotNull)
		
		// On crée une colonne "main_categorie" qui contient la catégorie principale du business
		var business_main_category = business_categories.withColumn("main_category", getMainCategoryUDF(col("categories")))


		// Pour la vérification des catégories principales -> écriture dans des fichiers json
		// business_main_category.select("categories", "main_category")
		// 						.write.mode(SaveMode.Overwrite)
		// 						.json("categories.json")
		

		// Suppression des colonnes inutiles dans le dataframe "business"
		business = business.drop(col("categories"))
							.drop(col("attributes"))
							.drop(col("hours"))

				
		// On joint les DataFrames "business" et "business_main_category" pour ajouter la colonne "main_category" à "business"
		// Jointure externe pour ne pas perdre les business qui n'ont pas de catégorie principale
		business_main_category = business_main_category.withColumnRenamed("business_id", "business_id_2")	// pour éviter les conflits
		business = business.join(business_main_category, business("business_id") === business_main_category("business_id_2"), "left_outer")
							.drop(business_main_category("business_id_2"))

		// Autre façon de faire la jointure, avec une requête SQL
		// business.registerTempTable("business")
		// business_main_category.registerTempTable("business_main_category")
		// business = spark.sql(
		// 	"""
		// 		SELECT	b.*, bmc.main_category
		// 		FROM	business b LEFT OUTER JOIN business_main_category bmc
		// 		ON		b.business_id = bmc.business_id
		// 	""")
			
		// On libère la mémoire
		business_categories.unpersist()
		business_main_category.unpersist()


		// On joint les DataFrames "business" et "nb_visits_by_business" pour ajouter la colonne "nb_visites" à "business"
		// Jointure externe pour ne pas perdre les business qui n'ont pas de nombre de visites
		business = business.join(nb_visits_by_business, business("business_id") === nb_visits_by_business("business_id"), "left_outer")
							.drop(nb_visits_by_business("business_id"))

		// On remplace les valeurs nulles par 0
		business = business.na.fill(0, Seq("nb_visits"))

		// On libère la mémoire
		nb_visits_by_business.unpersist()


		// On joint les DataFrames "business" et "openDays" pour ajouter les colonnes "is_open_xxx" à "business"
		// Jointure externe pour ne pas perdre les business qui n'ont pas d'information sur les jours d'ouverture
		business = business.join(openDays, business("business_id") === openDays("business_id"), "left_outer")
							.drop(openDays("business_id"))

		// On renomme la colonne "stars" en "avg_stars" pour plus de clarté
		business = business.withColumnRenamed("stars", "avg_stars")

		// On libère la mémoire
		openDays.unpersist()

		// On joint les DataFrames "business" et "nb_tips_by_business" pour ajouter la colonne "nb_tips" à "business"
		// Jointure externe pour ne pas perdre les business qui n'ont pas d'information sur le nombre de tips
		business = business.join(nb_tips_by_business, business("business_id") === nb_tips_by_business("business_id"), "left_outer")
							.drop(nb_tips_by_business("business_id"))

		// On remplace les valeurs nulles par 0
		business = business.na.fill(0, Seq("nb_tips"))


		// Affichage du schéma
		// business.printSchema()

		// On libère la mémoire
		// nb_reviews_by_business.unpersist()
		nb_tips_by_business.unpersist()


		/*
		 * Transformation de la table de dimension "date"
		 * Ajout des colonnes "year", "month", "day", "week_number" et "day_of_week" 
		*/

		dates = dates.withColumn("year", year(col("date")))
					.withColumn("month", month(col("date")))
					.withColumn("day", dayofmonth(col("date")))
					.withColumn("week_number", weekofyear(col("date")))
					.withColumn("day_of_week", dayofweek(col("date")))	// 1 = dimanche, 2 = lundi, etc.

		// Affichage des 10 premières lignes
		// dates.show(10)


		/******************************************************************
			Ajout de données au data warehouse - base de données Oracle
		*******************************************************************/

		// Enregistrement du DataFrame date dans la table "date_detail" car "date" est un mot réservé en Oracle
		dates.write.mode(SaveMode.Overwrite).jdbc(urlOracle, "date_detail", connectionPropertiesOracle)
		// Libération de la mémoire
		dates.unpersist()


		// Enregistrement du DataFrame reviews dans la table "review"
		reviews.write.mode(SaveMode.Overwrite).jdbc(urlOracle, "review", connectionPropertiesOracle)
		// Libération de la mémoire
		reviews.unpersist()


		// Enregistrement du DataFrame visits dans la table "visit"
		visits.write.mode(SaveMode.Overwrite).jdbc(urlOracle, "visit", connectionPropertiesOracle)
		// Libération de la mémoire
		visits.unpersist()


		// Enregistrement du dialecte Oracle
		JdbcDialects.registerDialect(dialect)

		// Réorganisation des colonnes du DataFrame business
		business = business.select("business_id", "name", "main_category", "address", "postal_code", "city", "state", "latitude", "longitude", "avg_stars", "review_count", "is_open", "nb_visits", "nb_tips", "is_open_monday", "is_open_tuesday", "is_open_wednesday", "is_open_thursday", "is_open_friday", "is_open_saturday", "is_open_sunday")
		// Enregistrement du DataFrame business dans la table "business"
		business.write.mode(SaveMode.Overwrite).jdbc(urlOracle, "business", connectionPropertiesOracle)
		// Libération de la mémoire
		business.unpersist()

		// Désenregistrement du dialecte Oracle
		JdbcDialects.unregisterDialect(dialect)


		// Enregistrement du DataFrame user dans la table "consumer" (user est un mot réservé en Oracle)
		users.write.mode(SaveMode.Overwrite).jdbc(urlOracle, "consumer", connectionPropertiesOracle)
		// Libération de la mémoire
		users.unpersist()


		// Arrêt de la session Spark
		spark.stop()

	}

}
