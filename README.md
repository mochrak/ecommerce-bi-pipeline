# 🎯 Pipeline Décisionnel E-Commerce sur GCP

Pipeline BI cloud-native utilisant Google Cloud Platform pour ingérer, 
transformer et analyser les données e-commerce en temps quasi-réel.

## 📊 Architecture

Cloud Storage → Dataflow → BigQuery → Looker Studio (Ingestion) (Traitement) (Stockage) (Visualisation)

Code

## 🚀 Technologies

- **Cloud**: Google Cloud Platform (GCP)
- **Ingestion**: Cloud Storage
- **Traitement**: Dataflow (Apache Beam)
- **Stockage**: BigQuery
- **Visualisation**: Looker Studio
- **Langage**: Python 3.9, SQL

## ✨ Fonctionnalités

✅ Ingestion automatique de 4 fichiers CSV  
✅ Transformation et nettoyage avec Dataflow  
✅ Stockage dans BigQuery (4 tables + 6 vues)  
✅ Dashboard Looker Studio interactif  
✅ 7 requêtes SQL optimisées  
✅ Rapport BI avec recommandations  

## 📈 Résultats Obtenus

- Identification des clients VIP (10% = 80% CA)
- Détection de 30% de clients inactifs
- Pages populaires identifiées
- Incidents par catégorie analysés

## 🏗️ Structure du Projet

ecommerce-bi-pipeline/ ├── src/ │ ├── dataflow_pipeline.py │ ├── sql_queries.sql │ └── requirements.txt ├── docs/ │ ├── ARCHITECTURE.md │ ├── SETUP.md │ ├── GCP_SETUP.md │ └── QUERIES.md ├── README.md ├── LICENSE └── .gitignore

Code

## 🚀 Démarrage Rapide

### Installation

```bash
pip install -r src/requirements.txt
Configuration GCP
bash
gcloud auth login
gcloud config set project ecommerce-pipeline-493309
export GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"
Exécuter le Pipeline
bash
python src/dataflow_pipeline.py
