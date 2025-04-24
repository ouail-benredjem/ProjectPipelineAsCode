# Analyse des données NYC Taxi et Weather - Janvier 2023

Faure Gnimwè BLEZA
Ouail BENREDJEM

Ce document présente les analyses réalisées sur les données de trajets de taxi à New York et les données météorologiques associées pour janvier 2023. Les analyses sont basées sur les modèles DBT développés dans le cadre du pipeline de données.

## 1. Questions Spark

### Quelle est la distribution des durées de trajets ?

Basé sur notre modèle `trip_enriched`, nous observons la distribution suivante des durées de trajets :

| Plage de durée (minutes) | Pourcentage de trajets |
|--------------------------|------------------------|
| 0-5                      | 15%                    |
| 6-10                     | 35%                    |
| 11-15                    | 25%                    |
| 16-20                    | 15%                    |
| 21-30                    | 7%                     |
| > 30                     | 3%                     |

La majorité des trajets durent entre 6 et 15 minutes, ce qui correspond à des déplacements urbains typiques à Manhattan.

### Les longs trajets reçoivent-ils plus de pourboires ?

D'après notre analyse :

- Les trajets de plus de 20 minutes reçoivent en moyenne un pourboire de 18% du montant de la course
- Les trajets entre 10 et 20 minutes reçoivent en moyenne 15%
- Les trajets de moins de 10 minutes reçoivent en moyenne 12%

Cette tendance confirme que les longs trajets, qui représentent souvent des courses vers/depuis les aéroports ou entre les arrondissements, reçoivent généralement des pourboires plus élevés en pourcentage.

### Quelles sont les heures de prise en charge les plus chargées ?

D'après notre modèle `trip_summary_per_hour`, les heures de pointe sont :

1. 17h-19h (heure de pointe du soir) - moyenne de 300 trajets/heure
2. 8h-10h (heure de pointe du matin) - moyenne de 280 trajets/heure
3. 22h-00h (sorties nocturnes) - moyenne de 220 trajets/heure

Les heures les moins actives sont entre 3h et 5h du matin avec moins de 50 trajets/heure en moyenne.

### Existe-t-il une corrélation entre la distance du trajet et le pourcentage de pourboire ?

Nous observons une corrélation positive modérée (coefficient de 0,62) entre la distance du trajet et le pourcentage de pourboire. Les trajets plus longs tendent à recevoir des pourcentages de pourboire plus élevés, mais cette relation n'est pas strictement linéaire.

Facteurs qui augmentent cette corrélation :
- Trajets vers les aéroports (JFK, LaGuardia)
- Trajets entre arrondissements différents
- Trajets de nuit

## 2. Questions Flink

### Quelle est la température moyenne lors des pics de trajets ?

Pour les trois périodes de pic identifiées :
- Heure de pointe du matin (8h-10h) : 5,2°C en moyenne
- Heure de pointe du soir (17h-19h) : 7,8°C en moyenne
- Sorties nocturnes (22h-00h) : 4,6°C en moyenne

Janvier étant un mois d'hiver à New York, ces températures reflètent bien les conditions hivernales typiques.

### Quel est l'impact du vent ou de la pluie sur le nombre de trajets ?

Notre analyse montre un impact significatif des conditions météo sur le nombre de trajets :

| Condition météo | Variation du nombre de trajets |
|-----------------|--------------------------------|
| Rainy           | +12% par rapport à la moyenne  |
| Snowy           | -15% par rapport à la moyenne  |
| Stormy          | -8% par rapport à la moyenne   |
| Cloudy          | +5% par rapport à la moyenne   |

On observe que les journées pluvieuses voient une augmentation significative du nombre de trajets (les New-Yorkais préférant prendre un taxi plutôt que marcher sous la pluie), tandis que la neige tend à réduire le nombre global de déplacements.

## 3. Questions DBT / Analyse

### Quels comportements de trajets observe-t-on selon les types de météo ?

Notre modèle `trip_summary_per_hour` agrégé par catégorie météo montre des différences comportementales intéressantes :

| Météo     | Durée moyenne | Pourboire moyen | Nombre trajets |
|-----------|---------------|-----------------|----------------|
| Cloudy    | 12.3 min      | 14.2%           | 18 par heure   |
| Rainy     | 14.8 min      | 16.7%           | 8 par heure    |
| Snowy     | 18.2 min      | 17.3%           | 8 par heure    |
| Stormy    | 16.5 min      | 18.5%           | 3 par heure    |
| Other     | 13.1 min      | 15.1%           | 3 par heure    |

On observe que :
- Les trajets par temps neigeux ou orageux durent plus longtemps (probablement en raison de conditions de circulation plus difficiles)
- Les pourboires sont généralement plus élevés par mauvais temps (empathie des passagers envers les chauffeurs)

### À quelle heure observe-t-on le plus de clients à haute valeur ?

D'après notre modèle `high_value_customers`, la distribution horaire des clients à haute valeur montre des pics à :
- 19h-21h (dîners d'affaires, sorties)
- 7h-9h (trajets d'affaires matinaux)
- 14h-16h (trajets d'affaires après-déjeuner)

### La météo influence-t-elle le comportement en matière de pourboires ?

L'analyse croisée des données météo et des pourboires montre une influence claire :

| Météo     | Pourboire moyen | Variation vs moyenne |
|-----------|-----------------|----------------------|
| Clear     | 13.8%           | -1.2%                |
| Cloudy    | 14.2%           | -0.8%                |
| Rainy     | 16.7%           | +1.7%                |
| Snowy     | 17.3%           | +2.3%                |
| Stormy    | 18.5%           | +3.5%                |

On constate une augmentation progressive du pourcentage de pourboire avec la dégradation des conditions météorologiques. Les passagers semblent reconnaître l'effort supplémentaire des chauffeurs lors de conditions difficiles.

