# SmartFlow Analytics Platform

Plateforme Big Data développée avec Apache Spark pour l'analyse distribuée d'un système de vélos connectés.

## Objectifs

- Ingestion de données multi-sources
- Nettoyage et validation des données
- Architecture Bronze / Silver / Gold
- Analyse distribuée avec Spark SQL
- Optimisation des performances Spark
- Machine Learning avec Spark MLlib

## Architecture

data/raw → bronze → silver → gold

## Technologies

- Python
- Apache Spark
- Spark SQL
- Spark MLlib
- Parquet
- Git

## Résultats

### Données traitées

| Dataset | Lignes |
|----------|---------:|
| Cyclistes | 4 868 396 |
| Réparateurs | 58 188 |
| Vélos | 17 529 |
| Stations | 2 472 |
| Villes | 1 083 |

### Machine Learning

| Modèle | Accuracy |
|----------|---------:|
| Logistic Regression | 99.96% |
| Random Forest | 100.00% |
| Decision Tree | 99.82% |

## Structure du projet

```text
src/
├── ingestion
├── cleaning
├── analytics
└── ml

Lancement
pip install -r requirements.txt
python main.py

