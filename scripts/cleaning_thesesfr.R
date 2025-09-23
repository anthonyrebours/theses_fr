############## Nettoyage et préparation des données de datagouv.##############
  

#' Ce script permet de prétraiter les données sauvegardées depuis datagouv.fr et 
#' de les sauvegarder au format parquet sous différentes tables

## Packages -------------------------------------------------------------------
library(tidyverse) # Pour manipuler les données 
library(data.table) # Pour une manipulation plus rapide 
library(arrow) # Permet d'importer et exporter des fichiers parquet
library(here) # Création de chemin d'accès 
library(janitor) # Pour nettoyer noms de variables et repérer doublons
library(stringi)


## Données --------------------------------------------------------------------
# Chemin vers les données
data_path <- here("data", "datagouv.parquet")

# Import des données
datagouv <- read_parquet(data_path) # Pour importer les données .parquet
datagouv <- as.data.table(datagouv) # Pour une manipulation plus rapide


## Pré-traitement des données -------------------------------------------------
# Harmonisation des formats de variables
datagouv <- 
  datagouv %>% 
  mutate(
    across(
      where(is.logical),
      ~ as.character(.)
    )
  ) 

# Regroupement de tous les sujets rameau en une seule variable 
rameau <- str_subset(names(datagouv), "sujets_rameau")
datagouv <- datagouv %>% unite(col = "sujets_rameau", rameau, sep = " | ", na.rm = TRUE)

# Traitement directeurs de thèse inconnu
datagouv <- 
  datagouv %>% 
  mutate(
    across(
      contains("directeurs"),
      ~ na_if(
        .,
        "Directeur de thèse inconnu"
      )
    )
  ) 

# Vérification identifiants nnt manquants
datagouv %>% filter(is.na(nnt)) %>% view()

#' Nous avons détecter une seule thèses sans identifiant, nous avons choisi
#' d'ajouter manuellement l'id en réutilisant la typologie adopté par 
#' thèses.fr
datagouv <- 
  datagouv %>% 
  mutate(nnt = replace_na(nnt, "2022REN1SBIS")) 

# Harmonisation des noms auteurs
datagouv <- 
  datagouv %>% 
  mutate(auteur.nom = str_to_title(auteur.nom)) %>% 
  mutate(auteur.nom = stri_trans_general(auteur.nom, id = "Latin-ASCII"))


## Corrections des doublons ---------------------------------------------------
# Vérification des doublons
datagouv %>% 
  get_dupes(nnt, auteur.nom) %>% 
  select(nnt, accessible, contains("auteur"), titres.fr, source) 

#' Certains doublons sont de véritables doublons qui sont
#'  essentiellement dû à des différences de saisie entre STAR et le Sudoc, dans
#' ces cas on souhaite privilégier les données de STAR qui sont les plus à jours 
#' et contiennent le plus de métadonnées 
datagouv <- 
  datagouv %>% 
  group_by(nnt, auteur.nom) %>% 
  filter(
    if (any(source == "star", na.rm = TRUE)) {
      source == "star"
    } else {
      TRUE
    }
  ) %>% 
  slice(1) %>% 
  ungroup()

#' Une fois ces premiers cas de doublons éliminés, il nous reste encore quelques 
#' cas pour lesquels seuls les numéros nnt sont en doublons et sont dus à des 
#' erreurs d'écriture 
datagouv %>% 
  get_dupes(nnt) %>% 
  select(nnt, accessible, contains("auteur"), cas, source) 

#' Parmi les doublons restants le cas ci-dessous provient d'une différence 
#' d'écriture du nom et du prénom de l'auteur entre la version sudoc et STAR,
#' on conserve manuellement la version STAR
datagouv  <- datagouv %>% filter(!(nnt == "2022UPASL034" & source == "sudoc"))

#' Il ne reste plus que des doublons de nnt qui correspondent à thèses
#' différentes, c'est-à-dire pour lesquelles la plupart des métadonnées (auteur, 
#' titre, sujets...) sont différentes entre elles. Comme il ne s'agit pas de 
#' véritables doublons on souhaite conserver les deux thèses, au lieu d'éliminer
#' l'une des deux on va donc simplement modifier le numéro nnt d'une des deux 

datagouv <- 
  datagouv %>% 
  group_by(nnt) %>% 
  mutate(
    nnt = ifelse(
      row_number() > 1,
      paste0(nnt, "_BIS", row_number() -1),
      nnt
    )
  ) %>% 
  ungroup()


## Séparation en plusieurs jeux de données ------------------------------------

#' Après avoir prétraiter les données et corrigé certains doublons, nous allons
#' séparer et sauvegarder le jeux de données en différentes tables et sous 
#' format parquet

#' Nouvelles tables :
#' - `datagouv_metadata`
#' - `datagouv_individuals`
#' - `datagouv_institutions`


# Table metadata 
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

datagouv_metadata %>% write_parquet(here("data", "datagouv_metadata.parquet"))

# Table individuals
datagouv %>% 
  select(
    nnt, 
    contains("auteur"),
    contains("directeurs"),
    contains("membres"),
    contains("president"),
    contains("rapporteurs")
  ) %>% 
  write_parquet(here("data", "datagouv_individuals.parquet"))

# Table affiliations
datagouv %>% 
  select(
    nnt, 
    code_etab,
    contains("etablissements"),
    contains("ecole"),
    contains("partenaires")
  ) %>% 
  write_parquet(here("data", "datagouv_affiliations.parquet"))
  


