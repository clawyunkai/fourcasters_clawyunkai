# Audit de sobriété numérique sur le  projet Fourcaster

## Contexte
Ce document a pour but de réaliser un audit interne sur le projet "Fourcasters" consistant à quantifier l'impact du changement climatique sur certaines conséquences sur la société française.

## Constat - Observation

**A la source (l'API)** : 
    - Les données météo sont récupérées sur l'api "open-meteo", avec des données mises à jour tous les jours avec un retard de 5 jours.

**Au stockage (le bucket)** :
    - Pour le stockage de certaines données, nous passons par le bucket afin de faciliter la création de table ou bien des procédures.

**Aux requêtes (Big Query)** :
   - Pour le stockage des tables, nous pourrions le faire en local sur notre machine mais nous avons  la solution "google cloud" qui permet d'avoir la puissance et les performances du cloud avec une utilisation à la demande. 

**Au machine learning** :
    - Pas encore arrivé à cette étape

**A l'application / dataviz** :
    - Pas encore arrivé à cette étape

## Gaspillage repéré

**A la source (l'API)** :
    - Actuellement on fait la mise à jour tous les jours.

**Au stockage (le bucket)** :
    - Après avoir utilisé le bucket comme moyen intermédiaire pour importer des tables, notamment la table des données météos historiques contenant 3 500 000 lignes pour les données du 01/01/2000 au 31/07/2026 pour 360 villes réparties sur toute la France métropolitaine.

**Aux requêtes (Big Query)** :
    - Nous paramétrons les tables avec du partitionnement pour améliorer les performances des processus de requêtes.

**Au machine learning** :
    - Pas encore arrivé à cette étape

**A l'application / dataviz** :
    - Pas encore arrivé à cette étape

## Action proposées

**A la source (l'API)** :
    - Jusqu'à aujourd'hui, on faisait la mise à jour tous les jours mais comme nous nous intéressons à l'évolution du climat et non pas de la météo, on pourrait faire la mise à jour une fois par semaine afin de ne pas avoir une sur-utilisation du script

**Au stockage (le bucket)** :
    - Après avoir utilisé le bucket comme moyen intermédiaire pour importer des tables, une fois la table créée sur "Big Query", nous devons nous assurer de supprimer la donnée

**Aux requêtes (Big Query)** :
    - 

**Au machine learning** :
    - Pas encore arrivé à cette étape

**A l'application / dataviz** :
    - Pas encore arrivé à cette étape

## Conclusion

