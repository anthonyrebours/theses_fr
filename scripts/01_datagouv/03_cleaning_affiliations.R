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
## Restructurations variables
affiliations <- affiliations %>% separate(variables, c("role", "order", "info"), sep = "\\.")
affiliations <- affiliations %>% pivot_wider(names_from = "info", values_from = "valeurs")
affiliations <- affiliations %>% select(-order)

##  Retrait données manquantes lorsque ni nom ni idref disponibles
affiliations <- affiliations %>% filter(!c(is.na(nom) & is.na(idref)))


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

## Création d'une variable etablissement_de_soutenance à partir des code_etab
affiliations <- 
  affiliations %>% 
  left_join(codes_etab, by = c("code_etab" = "code")) 

## Vérification différences codes_etab
affiliations %>% filter(is.na(universites)) %>% view()


univ_missing <- 
  affiliations %>% 
  filter(role == "etablissements_soutenance") %>% 
  setdiff(
    codes_etab$universites
  )


affiliations %>% 
  filter(
    etablissements_soutenance.0.nom %in% univ_missing &
    is.na(etablissements_soutenance.0.idref)
  ) %>% distinct(etablissements_soutenance.0.nom)
  
names(affiliations)
theses_fr_kr %>% view()


setdiff(affiliations$code_etab, codes_etab$code)

## Vérification idref etablissements
affiliations %>% 
  count(nom, idref) %>% view()

# Appariemment etablissements abes et datagouv ----------------------------
## Retrait code_etab absents des données de datagouv
codes_existants <-
  codes_etab %>% 
  filter(code %in% etab_datagouv$code_etab) 

## Appariemment probabiliste sur code etab présents dans données datagouv

codes_existants <- 
  codes_existants %>% 
  rename(code_etab = code) %>% 
  rownames_to_column()

etab_datagouv <- 
  etab_datagouv %>% 
  rename(universites = etablissements_soutenance.0.nom) %>% 
  rownames_to_column()

fl_etab <- 
  fastLink(
    dfA = codes_existants,
    dfB = etab_datagouv,
    varnames = c("code_etab", "universites"),
    stringdist.match = "universites"
  )

codes_corresp <- fl_etab$matches

codes_corresp <- 
  codes_corresp %>% 
  mutate(across(everything(), as.character))

codes_complets <- 
  left_join(codes_existants, codes_corresp, by = c("rowname" = "inds.a"))

codes_complets <- 
  left_join(codes_complets, etab_datagouv, by = c("inds.b" = "rowname"))

codes_manquants <- codes_complets %>% filter(is.na(universites.y)) 

etab_datagouv %>% 
  filter(code_etab %in% codes_manquants$code_etab.x) %>% 
  count(code_etab, universites, etablissements_soutenance.0.idref) %>% view()




