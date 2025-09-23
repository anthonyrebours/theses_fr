##%######################################################%##
#                                                          #
####    Nettoyage et traitement datagouv_affiliation    ####
#                                                          #
##%######################################################%##



# Packages ----------------------------------------------------------------
library(tidyverse)


# Données -----------------------------------------------------------------
affiliations <- arrow::read_parquet(here::here("data", "datagouv_affiliations.parquet"))
codes_etab <- rio::import(here::here("data", "codes_etab.xlsx"))



# Etablissement de soutenance ---------------------------------------------
## Vérification des code_etab
affiliations %>% count(is.na(code_etab))

affiliations %>% filter(is.na(code_etab)) %>% view()

## Création de code_etab quand manquant
affiliations <- 
  affiliations %>% 
  mutate(
    code_etab = ifelse(
      is.na(code_etab),
      str_sub(nnt, 5, 8),
      code_etab
    )
  ) 

## Création d'une variable etablissement_de_soutenance à partir des code_etab
affiliations %>% 
  left_join(codes_etab, by = c("code_etab" = "code")) %>% 
  janitor::get_dupes(nnt) %>% count(code_etab, etablissements_soutenance.0.nom) %>% view()


names(affiliations)
theses_fr_kr %>% view()

intersect(affiliations$etablissements_soutenance.0.nom, codes_etab$universites)
