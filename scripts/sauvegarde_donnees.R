################### Téléchargement des données de theses.fr ###################


#' Objet : Télécharger les données concernant les thèses défendues en France  
#' entre 1985 et 2023 sous format parquet. Le fait de stocker les données dans un 
#' fichier parquet permet un gain d'espace disque nécessaire ainsi qu'une lecture 
#' et une manipulation plus rapides des données grâce à une meilleure compression  
#' des données. 


# Packages --------------------------------------------------------------------
library(tidyverse)
library(arrow)


# URL et chemin de données ----------------------------------------------------
lien_url <- "https://object.files.data.gouv.fr/hydra-parquet/hydra-parquet/eb06a4f5-a9f1-4775-8226-33425c933272.parquet"
data_path <- here::here("data", "theses_datagouv.parquet")


# Téléchargement de données ---------------------------------------------------
theses <- read_parquet(lien_url)


# Retrait des variables non requises ------------------------------------------
theses <- 
  theses %>% 
  select(
    -c(
      accessible,
      cas,
      discipline,
      embargo,
      status,
      source, 
      sujets.en, 
      oai_set_specs, 
      resumes.en,
      starts_with("resumes"),
      sujets.en,
      sujets.fr,
      starts_with("sujets.autre"),
      starts_with("langues"), 
      starts_with("membres_jury"),
      starts_with("president_jury"),
      starts_with("rapporteurs"),
      starts_with("ecoles_doctorales")
    )
  )


# Sauvegarde des données ------------------------------------------------------
write_parquet(data_path)
