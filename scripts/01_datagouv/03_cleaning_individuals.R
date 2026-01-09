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


## 
individuals <- 
  individuals %>% 
  mutate(
    across(
      matches("directeurs|membres|president|rapporteurs"),
      ~ na_if(.,"")
    )
  )


