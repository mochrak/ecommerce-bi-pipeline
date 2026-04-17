import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.transforms import Map, FlatMap, ParDo
import csv
import json
from datetime import datetime

# ========== FONCTIONS UTILITAIRES (DÉFINI AVANT !) ==========

def parse_csv(line):
    """Parse une ligne CSV"""
    try:
        # Skip les headers
        if line.startswith('client_id') or line.startswith('commande_id') or \
           line.startswith('incident_id') or line.startswith('session_id'):
            return None
        
        parts = line.strip().split(',')
        return parts if len(parts) > 1 else None
    except Exception as e:
        print(f"Erreur parse_csv: {e}")
        return None

def enrich_orders(order):
    """Transforme les commandes"""
    try:
        if order is None or len(order) < 7:
            return None
        
        return json.dumps({
            'commande_id': order[0],
            'client_id': order[1],
            'montant': order[2] if len(order) > 2 else '0',
            'date': order[3] if len(order) > 3 else '',
            'statut': order[6] if len(order) > 6 else 'Inconnu'
        })
    except Exception as e:
        print(f"Erreur enrich_orders: {e}")
        return None

def calculate_order_metrics(order):
    """Calcule les métriques des commandes"""
    try:
        if order is None or len(order) < 7:
            return None
        
        return json.dumps({
            'commande_id': order[0],
            'client_id': order[1],
            'montant': order[2] if len(order) > 2 else '0',
            'date': order[3] if len(order) > 3 else '',
            'statut': order[6] if len(order) > 6 else 'Inconnu',
            'mois': order[3][:7] if len(order) > 3 else ''
        })
    except Exception as e:
        print(f"Erreur calculate_order_metrics: {e}")
        return None

def clean_incidents(incident):
    """Nettoie les incidents"""
    try:
        if incident is None or len(incident) < 7:
            return None
        
        return json.dumps({
            'incident_id': incident[0],
            'client_id': incident[1],
            'date': incident[2],
            'categorie': incident[3],
            'statut': incident[5] if len(incident) > 5 else 'Inconnu',
            'priorite': incident[6] if len(incident) > 6 else 'Normal'
        })
    except Exception as e:
        print(f"Erreur clean_incidents: {e}")
        return None

# ========== CONFIGURATION DU PIPELINE ==========

options = PipelineOptions(
    project='ecommerce-pipeline-493309',
    runner='DataflowRunner',
    region='europe-west1',
    temp_location='gs://ecommerce-processed-data-20260414/temp',
    staging_location='gs://ecommerce-processed-data-20260414/staging'
)

# Créer le pipeline
p = beam.Pipeline(options=options)

# ========== ÉTAPE 1 : LIRE LES DONNÉES ==========
# ⚠️ REMPLACE LES CHEMINS AVEC TON VRAI BUCKET !

# Lire client.csv (ATTENTION : client.csv sans "s" !)
clients = (
    p 
    | 'ReadClients' >> ReadFromText('gs://ecommerce-raw-data-20260414/client.csv')
    | 'ParseClients' >> Map(parse_csv)
    | 'FilterClients' >> beam.Filter(lambda x: x is not None)
)

# Lire commandes.csv
commandes = (
    p 
    | 'ReadOrders' >> ReadFromText('gs://ecommerce-raw-data-20260414/commandes.csv')
    | 'ParseOrders' >> Map(parse_csv)
    | 'FilterOrders' >> beam.Filter(lambda x: x is not None)
    | 'MetricsOrders' >> Map(calculate_order_metrics)
    | 'FilterMetrics' >> beam.Filter(lambda x: x is not None)
)

# Lire incidents.csv
incidents = (
    p 
    | 'ReadIncidents' >> ReadFromText('gs://ecommerce-raw-data-20260414/incidents.csv')
    | 'ParseIncidents' >> Map(parse_csv)
    | 'FilterIncidents' >> beam.Filter(lambda x: x is not None)
    | 'CleanIncidentsData' >> Map(clean_incidents)
    | 'FilterCleanIncidents' >> beam.Filter(lambda x: x is not None)
)

# Lire pages_vues.csv
pages_vues = (
    p 
    | 'ReadPages' >> ReadFromText('gs://ecommerce-raw-data-20260414/pages_vues.csv')
    | 'ParsePages' >> Map(parse_csv)
    | 'FilterPages' >> beam.Filter(lambda x: x is not None)
)

# ========== ÉTAPE 3 : ÉCRIRE LES RÉSULTATS ==========

# Écrire les métriques des commandes
commandes | 'WriteMetrics' >> WriteToText('gs://ecommerce-processed-data-20260414/metrics/orders')

# Écrire les incidents nettoyés
incidents | 'WriteCleanedIncidents' >> WriteToText('gs://ecommerce-processed-data-20260414/incidents_cleaned/data')

# Écrire les pages vues
pages_vues | 'WritePages' >> WriteToText('gs://ecommerce-processed-data-20260414/pages_vues/data')

# ========== EXÉCUTER LE PIPELINE ==========

result = p.run()
result.wait_until_finish()

print("✅ Pipeline exécuté avec succès !")