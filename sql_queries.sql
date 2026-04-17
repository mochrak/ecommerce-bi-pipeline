REQUÊTE 1 : Nombre total de commandes
SQL
SELECT 
  COUNT(*) AS nombre_commandes,
  COUNT(DISTINCT client_id) AS nombre_clients
FROM `ecommerce-pipeline-493309.ecommerce_data.commandes`;
REQUÊTE 2 : Commandes par client
SQL
SELECT 
  c.client_id,
  c.nom,
  c.prenom,
  COUNT(cmd.commande_id) AS nombre_commandes,
  cmd.statut
FROM `ecommerce-pipeline-493309.ecommerce_data.clients` c
LEFT JOIN `ecommerce-pipeline-493309.ecommerce_data.commandes` cmd 
  ON c.client_id = cmd.client_id
GROUP BY c.client_id, c.nom, c.prenom, cmd.statut
ORDER BY nombre_commandes DESC
LIMIT 10;
REQUÊTE 3 : Clients inactifs (pas de commandes)
SQL
SELECT 
  c.client_id,
  c.nom,
  c.prenom,
  c.email,
  c.date_inscription
FROM `ecommerce-pipeline-493309.ecommerce_data.clients` c
LEFT JOIN `ecommerce-pipeline-493309.ecommerce_data.commandes` cmd 
  ON c.client_id = cmd.client_id
WHERE cmd.commande_id IS NULL
ORDER BY c.date_inscription DESC;
REQUÊTE 4 : Pages les plus visitées
SQL
SELECT 
  page,
  COUNT(*) AS nombre_visites,
  COUNT(DISTINCT client_id) AS nombre_clients_uniques,
  ROUND(AVG(CAST(duree_seconde AS FLOAT64)), 2) AS duree_moyenne_secondes
FROM `ecommerce-pipeline-493309.ecommerce_data.pages_vues`
GROUP BY page
ORDER BY nombre_visites DESC;
REQUÊTE 5 : Incidents par catégorie
SQL
SELECT 
  categorie,
  COUNT(*) AS nombre_incidents,
  SUM(CASE WHEN statut = 'Résolu' THEN 1 ELSE 0 END) AS incidents_resolus,
  SUM(CASE WHEN niveau_priorite = 'Élevé' THEN 1 ELSE 0 END) AS incidents_prioritaires
FROM `ecommerce-pipeline-493309.ecommerce_data.incidents`
GROUP BY categorie
ORDER BY nombre_incidents DESC;
REQUÊTE 6 : Statut des commandes
SQL
SELECT 
  statut,
  COUNT(*) AS nombre_commandes
FROM `ecommerce-pipeline-493309.ecommerce_data.commandes`
GROUP BY statut
ORDER BY nombre_commandes DESC;
REQUÊTE 7 : Vue RÉSUMÉ pour Dashboard
SQL
SELECT 
  CURRENT_DATE() AS date_rapport,
  (SELECT COUNT(*) FROM `ecommerce-pipeline-493309.ecommerce_data.commandes`) AS nombre_commandes_total,
  (SELECT COUNT(DISTINCT client_id) FROM `ecommerce-pipeline-493309.ecommerce_data.clients`) AS nombre_clients_total,
  (SELECT COUNT(DISTINCT client_id) FROM `ecommerce-pipeline-493309.ecommerce_data.commandes`) AS clients_ayant_achete,
  (SELECT COUNT(*) FROM `ecommerce-pipeline-493309.ecommerce_data.incidents`) AS nombre_incidents,
  (SELECT COUNT(*) FROM `ecommerce-pipeline-493309.ecommerce_data.pages_vues`) AS nombre_pages_vues;
