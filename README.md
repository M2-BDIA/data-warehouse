# Création d'un Data Warehouse avec Spark

### Pour compiler le projet
sbt clean compile

### Pour exécuter le projet
sbt run

### Pour exécuter le projet avec plus de RAM (exemple avec 8go)

Sur Linux :
```bash
sbt -J-Xmx8G run
```

Sur Windows :
```bash
sbt -mem 8192 run
```