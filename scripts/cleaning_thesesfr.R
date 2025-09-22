############## Nettoyage et préparation des données de theses.fr ##############
  

#'

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
# Regroupement de tous les sujets rameau en une seule variable 
rameau <- str_subset(names(datagouv), "sujets_rameau")
datagouv <- datagouv %>% unite(col = "sujets_rameau", rameau, sep = " | ", na.rm = TRUE)

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

# Traitement des doublons
datagouv %>% 
  get_dupes(nnt, auteur.nom) %>% 
  select(nnt, accessible, contains("auteur"), titres.fr, source) 

#' Certains doublons sont essentiellement dû à des différences de saisie entre 
#' STAR et le Sudoc, dans ces cas on souhaite privilégier les données de STAR
#' qui sont les plus à jours et contiennent le plus de métadonnées (notamment 
#' sur l'accessibilité en ligne ou le "cas" des thèses)
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

datagouv %>% 
  get_dupes(nnt) %>% 
  select(nnt, accessible, contains("auteur"), cas, source) 

#' Une fois ces premiers cas de doublons éliminés, il nous reste encore  
#' quelques cas qui sont dus à des erreurs d'écriture 

datagouv  <- datagouv %>% filter(!(nnt == "2022UPASL034" & source == "sudoc"))
  

## Séparation en plusieurs jeux de données ------------------------------------
#' Nouvelles tables :
#' - `datagouv_metadata`
#' - `datagouv_individuals`
#' - `datagouv_institutions`


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


  


