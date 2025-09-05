############## Nettoyage et préparation des données de theses.fr ##############
  


# Packages --------------------------------------------------------------------
library(tidyverse) # Pour manipuler les données 
library(data.table) # Pour une manipulation plus rapide 
library(arrow) # Permet d'importer et exporter des fichiers parquet


# Données --------------------------------------------------------------

# Chemin vers les données
data_path <- here::here("data", "theses_fr.parquet")

# Import des données
theses <- read_parquet(data_path)
theses <- as.data.table(theses) # Pour une manipulation plus rapide


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


  


