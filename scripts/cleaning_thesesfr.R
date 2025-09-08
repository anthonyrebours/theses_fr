############## Nettoyage et préparation des données de theses.fr ##############
  

#'

## Packages -------------------------------------------------------------------
library(tidyverse) # Pour manipuler les données 
library(data.table) # Pour une manipulation plus rapide 
library(arrow) # Permet d'importer et exporter des fichiers parquet
library(here) # Création de chemin d'accès 


## Données --------------------------------------------------------------------

# Chemin vers les données
data_path <- here::here("data", "theses_datagouv.parquet")

# Import des données
theses_datagouv <- read_parquet(data_path)
theses_datagouv <- as.data.table(theses_datagouv) # Pour une manipulation plus rapide


## Création de nouveaux jeux de données ---------------------------------------
#' Séparation des données de data.gouv en quatre jeux de données :
#' - `datagouv_metadata`: Core metadata about each thesis.
#' - `datagouv_edge`: Links between theses and associated entities (individuals and institutions).
#' - `datagouv_individual`: Information about individuals involved in the theses.
#' - `datagouv_institution`: Information about institutions linked to the theses.


# Table metadata ----------------------------------------------------------- 
#' Création d'une table métadonnées
datagouv_metadata <- 
  theses_datagouv %>% 
  select(
    nnt,
    date_soutenance,
    langues.0,
    langues.1,
    titres.fr,
    titres.en,
    titres.autre.0,
    resumes.fr,
    resumes.en,
    resumes.autre.0,
    contains("sujets_rameau"),
    discipline,
    accessible
  )


# Sauvegarde de la table métadonnées 
datagouv_metadata %>% write_parquet(here("data", "datagouv_metadata"))

# Nom des colonnes
cols <- names(theses)


# Regroupement de tous les sujets rameau en une seule variable 
rameau <- str_subset(names(theses), "sujets_rameau")
theses <- theses %>% unite(col = "sujets_rameau", rameau, sep = " | ", na.rm = TRUE)

# Fusion des variables nom et prénom des auteurs et directeurs de thèse
  # Extraction des préfixes complets
prefixes <- unique(sub("\\.(nom|prenom)$", "", cols))

  # Pour chaque préfixe, fusionner les colonnes ayant ".nom" et ".prenom" en suffixe
for (prefix in prefixes) {
  nom_col <- paste0(prefix, ".nom")
  prenom_col <- paste0(prefix, ".prenom")
  
  if (nom_col %in% cols && prenom_col %in% cols) {
    theses <- 
      theses %>% 
      unite(
        !!sym(prefix),
        c(!!sym(nom_col), !!sym(prenom_col)),
        sep = ", ",
        na.rm = TRUE
      )
  }
}

theses <- 
  theses %>% 
  mutate(
    across(
      contains("idref"),
      ~ ifelse(
        !is.na(.), 
        paste0("(",.,")"), 
        NA_real_
      )
    )
  )

# Etablissements de soutenance


  


