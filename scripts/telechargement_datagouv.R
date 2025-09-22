############# Téléchargement des données de theses.fr sur datagouv #############


#' Objet : Télécharger les données concernant les thèses défendues en France  
#' entre 1985 et 2023 sous format parquet. Le fait de stocker les données dans un 
#' fichier parquet permet un gain d'espace disque ainsi qu'une lecture et une 
#' manipulation plus rapides des données grâce à une meilleure compression. 


# Packages --------------------------------------------------------------------
library(tidyverse)
library(arrow)


# URL et chemin de données ----------------------------------------------------
lien_url <- "https://www.data.gouv.fr/fr/datasets/r/eb06a4f5-a9f1-4775-8226-33425c933272"
data_path <- here::here("data", "datagouv.parquet")


# Téléchargement de données ---------------------------------------------------
datagouv <- read_csv(lien_url)


# Sauvegarde des données ------------------------------------------------------
datagouv %>% write_parquet(data_path)


