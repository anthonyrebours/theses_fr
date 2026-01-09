##%######################################################%##
#                                                          #
####       Traitement données individus datagouv        ####
#                                                          #
##%######################################################%##


# Packages et données -----------------------------------------------------
library(tidyverse)
library(janitor)

individuals <- arrow::read_parquet(here::here("data", "01_datagouv", "processed", "individuals.parquet"))


# Fusion des variables individuelles --------------------------------------
## Extraction des préfixes complets
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

## Nouvelles variables
cols_2 <- names(individuals)

## Fusion nom et .idref
for (prefix in prefixes) {
  idref_col <- paste0(prefix, ".idref")
  individu_col <- prefix
  
  if (idref_col %in% cols_2 && individu_col %in% cols_2) {
    individuals <- 
      individuals %>% 
      unite(
        !!sym(prefix),
        c(!!sym(individu_col), !!sym(idref_col)),
        sep = "|",
        na.rm = TRUE
      )
  }
}

## 
individuals <- 
  individuals %>% 
  mutate(
    across(
      matches("directeurs|membres|president|rapporteurs"),
      ~ na_if(.,"")
    )
  )
  
##  Format long 
individuals <- 
  individuals %>% 
  pivot_longer(
    cols = auteur:rapporteurs.5,
    names_to = "status",
    values_to = "noms",
    values_drop_na = TRUE
  ) 

## Correction nom de variables en tant qu'observations
individuals <- 
  individuals %>%
  mutate(status = str_remove(status, "\\..*")) 

## Separation noms et idref
individuals <-
  individuals %>%
  separate(noms, c("noms", "idref"), sep = "\\|")

# individuals <- 
#   individuals %>% 
#   mutate(
#     across(
#       contains("idref"),
#       ~ ifelse(
#         !is.na(.), 
#         paste0("(",.,")"), 
#         NA_real_
#       )
#     )
#   )
# 


