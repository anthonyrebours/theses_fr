##%######################################################%##
#                                                          #
####            Scraping et nettoyages codes            ####
####           etab de documentation abes.fr            ####
#                                                          #
##%######################################################%##




# Packages ----------------------------------------------------------------
library(rvest)
library(tidyverse)
library(janitor)

# Adresse et données ------------------------------------------------------

## Adresse url de la documentation
url <- "https://documentation.abes.fr/sudoc/regles/CodesUnivEtab.htm"

## Scraping de la page url
url_codes_etab <- read_html(url)


codes_etab <- 
  url_codes_etab %>% 
  html_table(header = TRUE) %>% 
  .[[3]]

## Import des noms et identifiants de données datagouv
list_etab_datagouv <- arrow::read_parquet(here::here("data", "datagouv_affiliations.parquet"))

list_etab_datagouv <- 
  list_etab_datagouv %>% 
  select(
    code_etab,
    etablissements_soutenance.0.nom, 
    etablissements_soutenance.0.idref
  ) %>% 
  distinct()

nom_etab <- list_etab_datagouv %>% 
  distinct(etablissements_soutenance.0.nom)
nom_etab <- nom_etab$etablissements_soutenance.0.nom
  
# Prétraitement des données -----------------------------------------------
## Harmonisation des noms de variables
codes_etab <- codes_etab %>% janitor::clean_names()

## Retrait de lignes vides 
codes_etab <- 
  codes_etab %>% 
  filter(!(code == "" | code == "Code" | code == "haut de page")) 

## Nettoyage variable code
codes_etab <- 
  codes_etab %>% 
  mutate(anciens_noms = ifelse(str_count(code) > 4, str_sub(code, 5,), NA)) %>% 
  mutate(code = str_sub(code, 1, 4)) %>% 
  mutate(across(c(code, anciens_noms), ~ str_trim(.))) 

## Nettoyage variable universites
codes_etab <- 
  codes_etab %>% 
  mutate(universites = str_remove_all(universites, "\\r\\n.*")) 



# Appariemment etablissements abes et datagouv ----------------------------
## Retrait code_etab absents des données de datagouv
codes_existants <- codes_etab %>% 
  filter(code %in% list_etab_datagouv$code_etab) 

codes_etab %>% 
  mutate(is_in_datagouv = ifelse(universites %in% nom_etab, universites, NA)) %>% print(n=233)


# Export des données ------------------------------------------------------
codes_etab %>% rio::export(here::here("data", "codes_etab.xlsx"))

