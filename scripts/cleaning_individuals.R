##%######################################################%##
#                                                          #
####       Traitement données individus datagouv        ####
#                                                          #
##%######################################################%##


library(tidyverse)
library(janitor)

individuals <- arrow::read_parquet(here::here("data", "datagouv_individuals.parquet"))


# Fusion nom et prenom --------------------------------------------------------
# Extraction des préfixes complets
cols <- names(individuals)
prefixes <- unique(sub("\\.(nom|prenom)$", "", cols))

# Pour chaque préfixe, fusionner les colonnes ayant ".nom" et ".prenom" en suffixe
for (prefix in prefixes) {
  nom_col <- paste0(prefix, ".nom")
  prenom_col <- paste0(prefix, ".prenom")
  
  if (nom_col %in% cols && prenom_col %in% cols) {
    individuals <- 
      individuals %>% 
      unite(
        !!sym(prefix),
        c(!!sym(nom_col), !!sym(prenom_col)),
        sep = ", ",
        na.rm = TRUE
      )
  }
}

cols <- names(individuals)
for (prefix in prefixes) {
  idref_col <- paste0(prefix, ".idref")
  individu_col <- prefix
  
  if (idref_col %in% cols && individu_col %in% cols) {
    individuals <- 
      individuals %>% 
      unite(
        !!sym(prefix),
        c(!!sym(idref_col), !!sym(individu_col)),
        sep = "|",
        na.rm = TRUE
      )
  }
}


individuals <- 
  individuals %>% 
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



