############## Nettoyage et préparation des données de theses.fr ##############
  

#'

## Packages -------------------------------------------------------------------
library(tidyverse) # Pour manipuler les données 
library(data.table) # Pour une manipulation plus rapide 
library(arrow) # Permet d'importer et exporter des fichiers parquet
library(here) # Création de chemin d'accès 
library(janitor)


## Données --------------------------------------------------------------------
# Chemin vers les données
data_path <- here::here("data", "datagouv.parquet")

# Import des données
datagouv <- read_parquet(data_path) # Pour importer les données .parquet
datagouv <- as.data.table(datagouv) # Pour une manipulation plus rapide


## Pré-traitement des données -------------------------------------------------
# Regroupement de tous les sujets rameau en une seule variable 
rameau <- str_subset(names(datagouv), "sujets_rameau")
datagouv <- datagouv %>% unite(col = "sujets_rameau", rameau, sep = " | ", na.rm = TRUE)

#  Repérage de doublons 
datagouv %>% 
  get_dupes(nnt) %>% 
  select(nnt, accessible, contains("auteur"), cas, source) %>% view()

#' Certains doublons sont essentiellement dû à des différences de saisie entre 
#' STAR et le Sudoc, dans ces cas on 

 
doublons_nnt <- 
  datagouv %>% 
  group_by(nnt) %>% 
  summarise(auteurs_diff = length(unique(auteur.nom))) %>% 
  filter(auteurs_diff > 1) 

datagouv %>% filter(nnt %in% doublons_nnt$nnt) %>% view()

## Séparation en plusieurs jeux de données ------------------------------------
#' Nouvelles tables :
#' - `datagouv_metadata`
#' - `datagouv_individual`
#' - `datagouv_institution`


## Table metadata -------------------------------------------------------------
#' Création d'une table métadonnées
datagouv_metadata <- 
  datagouv %>% 
  select(
    nnt,
    date_soutenance,
    accessible,
    embargo,
    cas, 
    these_sur_travaux,
    langues.0,
    langues.1,
    titres.fr,
    titres.en,
    titres.autre.0,
    resumes.fr,
    resumes.en,
    resumes.autre.0,
    contains("sujets_rameau"),
    discipline
  ) 

# Sauvegarde de la table métadonnées 
datagouv_metadata %>% write_parquet(here("data", "datagouv_metadata.parquet"))


# Fusion nom et prenom --------------------------------------------------------
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


  


