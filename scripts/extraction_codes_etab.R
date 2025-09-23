##%######################################################%##
#                                                          #
####     Scraping codes etab documentation abes.fr      ####
#                                                          #
##%######################################################%##




# Packages ----------------------------------------------------------------
library(rvest)
library(tidyverse)


# Adresse et données ------------------------------------------------------

## Adresse url de la documentation
url <- "https://documentation.abes.fr/sudoc/regles/CodesUnivEtab.htm"

## Scraping de la page url
url_codes_etab <- read_html(url)


codes_etab <- 
  url_codes_etab %>% 
  html_table(header = TRUE) %>% 
  .[[3]]


# Prétraitement des données -----------------------------------------------
## Nettoyage des noms de variables
codes_etab <- codes_etab %>% janitor::clean_names()

## Retrait de lignes vides 
codes_etab <- 
  codes_etab %>% 
  filter(!(code == "" | code == "Code" | code == "haut de page")) 

## Nettoyage des valeurs de la variable code*
codes_etab <- 
  codes_etab %>% 
  mutate(anciens_noms = ifelse(str_count(code) > 4, str_sub(code, 5,), NA)) %>% 
  mutate(code = str_sub(code, 1, 4)) %>% 
  mutate(across(c(code, anciens_noms), ~ str_trim(.))) 


# Export des données ------------------------------------------------------
codes_etab %>% rio::export(here::here("data", "codes_etab.xlsx"))

