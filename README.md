# Création d'un Data Warehouse avec Spark

## Table des matières
- [Description](#description)
- [Structure du projet](#structure-du-projet)
- [Compilation](#compilation)
- [Exécution](#exécution)
- [Création des index](#création-des-index)
- [Exécution des requêtes](#exécution-des-requêtes)
- [Auteurs](#auteurs)


## Description

Ce projet constitue un travail pratique de l'UE Informatique Décisionnelle. L'objectif est de créer un data warehouse à partir de données réelles provenant du site Yelp permettant d'attribuer des avis et des notes à des établissements. Nous avons pour cela créer un processus ETL pour récupérer les données à partir des différentes sources, transformer les données et les charger dans une base de données commune.


## Structure du projet

Le projet est structuré de la manière suivante :

- `DataWarehouse/`: code source de l'ETL permettant de construire le data warehouse
    - `src/`: code source Spark avec Scala
    - `.env`: contient les variables d'environnement utilisées pour l'accès aux bases de données et fichiers de données   
- `creation-index.sql`: commandes permettant de créer les index une fois le data warehouse construit
- `queries.md`: les requêtes SQL d'analyse utilisées dans les tableaux de bord


## Compilation

Pour compiler le programme ETL, vous devez utiliser le compilateur sbt. Pour cela, il faut se placer dans le dossier DataWarehouse/ et exécuter la commande suivante :

```bash
sbt clean compile
```

## Exécution

Avant d'exécuter le programme, vous devez fournir les variables d'environnement permettant la connexion aux bases de données et l'accès aux fichiers de données. Les variables sont les suivantes :

```bash
POSTGRES_DATABASE_URL=
POSTGRES_DATABASE_USER=
POSTGRES_DATABASE_PASSWORD=

ORACLE_DATABASE_URL=
ORACLE_DATABASE_USER=
ORACLE_DATABASE_PASSWORD=

BUSINESS_FILE_PATH=
CHECKIN_FILE_PATH=
TIP_FILE_PATH=
```


Pour exécuter l'ETL, vous devez également vous placer dans le dossier DataWarehouse/ et exécuter la commande suivante :

```bash
sbt run
```

Vous pouvez choisir d'allouer plus de RAM à l'exécution du projet. Dans ce cas vous devez exécuter la commande suivante (exemple avec 8go) :

Sur Linux :
```bash
sbt -J-Xmx8G run
```

Sur Windows :
```bash
sbt -mem 8192 run
```

## Création des index

Une fois le programme exécuté, vous pouvez ajouter des index aux tables. Les commandes se trouvent dans le fichier **creation-index.sql**.


## Exécution des requêtes

Nous n'avons pas réussi à partager les tableaux de bords Metabase en dehors d'une version pdf. En revanche vous retrouverez dans le fichier **queries.sql** l'ensemble des requêtes d'analyses présentes dans les tableaux de bords. 


## Auteur

- [Maxime Dupont](https://github.com/maxime-dupont01)
- [Adrien Desbrosses](https://github.com/bvzopa)