# Akka streams patterns

Code du talk "Patterns pour akka streams" 

https://drive.google.com/open?id=1-b1V4ennk13rs77c8eAZ6jCPK-RhpIEUqdYknY_h138

## Csv upload 

Api d'upload d'un csv avec ingestion dans elastic search. 

## Csv download 

Download des données présentes dans elastic avec utilisation du scroll. 

## Load balancing 

Utilisation du composant "balance" pour faire du load balancing entre les noeuds ES

## Ecriture concurrente 

Utilisation du composant "partition" pour faire du sharding et éviter les écritures concurrentes

## Usage

## Démarrage de l'application 

```
sbt run 
```


## Démarrage elastic 
```
docker run -p 9200:9200 -p 9300:9300 elasticsearch
```


