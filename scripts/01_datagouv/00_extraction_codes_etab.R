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
library(fastLink)

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
codes_etab <- rio::import(here::here("data", "codes_etab.xlsx"))

etab_datagouv <- arrow::read_parquet(here::here("data", "datagouv_affiliations.parquet"))

etab_datagouv <- 
  etab_datagouv %>% 
  select(
    code_etab,
    etablissements_soutenance.0.nom, 
    etablissements_soutenance.0.idref
  ) %>% 
  distinct()

nom_etab <- etab_datagouv %>% distinct(etablissements_soutenance.0.nom)

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


# Export des données ------------------------------------------------------
codes_etab %>% rio::export(here::here("data", "codes_etab.xlsx"))





