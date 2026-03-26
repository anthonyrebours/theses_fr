##%######################################################%##
#                                                          #
####    Nettoyage et traitement datagouv_affiliation    ####
#                                                          #
##%######################################################%##



# Packages ----------------------------------------------------------------
library(tidyverse)


# Données -----------------------------------------------------------------
affiliations <- arrow::read_parquet(here::here("data", "01_datagouv", "processed", "affiliations.parquet"))
codes_etab <- rio::import(here::here("data", "codes_etab.xlsx")) %>% as_tibble()


# Pré-traitement données --------------------------------------------------
## Filtre ecoles veterinaires table codes-etab
codes_etab <- 
  codes_etab %>% 
  filter(
    universite != "Ecole nationale vétérinaire - Toulouse" 
    & universite != "Ecole nationale vétérinaire - Lyon"
    & universite != "Ecole nationale vétérinaire - Nantes"
  ) 

## Restructurations variables

affiliations <- 
  affiliations %>% 
  separate(variables, c("role", "order", "info"), sep = "\\.")

affiliations <- 
  affiliations %>%  
  pivot_wider(
    names_from = "info", 
    values_from = "valeurs" 
  ) 


# Distinction établissements de soutenance/cotutelle ----------------------

#' Dans le dump récupéré auprès de datagouv les données ne permettent pas de 
#' distinguer entre établissements de soutenance et établissements de cotutelle, 
#' pour obtenir cette vérification nous devons utiliser les codes etab des 
#' thèses. Dans un premier temps, on cherche à vérifier que toutes les 
#' institutions disposent bien d'un code etab car cela va nous permettre de 
#' séparer établissements de soutenance vs établissements de co-tutelle plus 
#' tard.  

## Vérification existence code_etab
affiliations %>% count(is.na(code_etab))
affiliations %>% filter(is.na(code_etab)) %>% view()

#' Quand on repère des code_etab manquant on en créé de nouveaux en reprenant 
#' les informations contenues dans le code nnt de la thèse

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

## Vérification code_etab
setdiff(affiliations$code_etab, codes_etab$code)

## Correction code_etab
affiliations <- 
  affiliations %>% 
  mutate(
    code_etab = recode(
      code_etab, 
      "LY02" = "LYO2",
      "MET2" = "METZ", 
      "TO41" = "TOU1", 
      "BLOB" = "GLOB", 
      "HESE" = "HESA"
    )
  )

##
etab_soutenance <- 
  affiliations %>% 
  filter(role == "etablissements_soutenance") %>% 
  mutate(nnt_nbr = n(), .by = nnt)  %>% 
  select(-c(order, type)) %>% 
  left_join(codes_etab, by = c("code_etab" = "code"))
  
##
etab_soutenance %>% count(is.na(idref ))

##
good_idref <- 
  etab_soutenance %>% 
  filter(idref == idref_ancien) %>% 
  distinct(nnt)

wrong_idref <- 
  etab_soutenance %>% 
  filter(!nnt %in% good_idref$nnt)

## Vérification différences codes_etab
etab_soutenance %>% filter(is.na(universites)) %>% view()


etab_soutenance %>% 
  mutate(
    idref = ifelse(
      is.na(idref), 
      case_when(
        nom == universite ~ idref_ancien, 
        TRUE ~ idref
      ), 
      idref
    )
  ) %>% count(is.na(idref))

wrong_idref %>% 
  mutate(
    idref = case_when(
      nom == universite ~ idref_ancien,
      TRUE ~ idref
    )
  ) %>%
  filter(idref == idref_ancien) %>% 
  count()







