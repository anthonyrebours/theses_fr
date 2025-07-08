# Nettoyage et 
  
# Author: Cellule scientométrie - BSU
# Version: 2025-07-07
  
# Packages
library(tidyverse)
library(arrow)

# Données (téléchargées sous format parquet depuis :
# https://www.data.gouv.fr/datasets/theses-soutenues-en-france-depuis-1985/)

theses <- read_parquet(here::here("data", "theses_fr.parquet"))

# ============================================================================

# Retrait des variables non utiles 
theses <- 
  theses %>% 
  select(
    -c(
      accessible,
      cas,
      embargo,
      source, 
      sujets.en, 
      oai_set_specs, 
      resumes.en,
      starts_with("resumes.autre"),
      sujets.en,
      starts_with("sujets.autre"),
      starts_with("langues"), 
      starts_with("membres_jury"),
      starts_with("president_jury"),
      starts_with("rapporteurs"),
      starts_with("ecoles_doctorales")
    )
  )


# Unification de variables
  # sujets_rameau
theses <- 
  theses %>% 
  unite(
    col = "sujets_rameau",
    sujets_rameau.0:sujets_rameau.9, 
    sep = ", ", 
    na.rm = TRUE
  )

  # auteur
theses <- 
  theses %>% 
  unite(
    col = "auteur",
    c("auteur.nom", "auteur.prenom"),
    sep = ", ",
    na.rm = TRUE
  )

theses <- 
  theses %>% 
  mutate(auteur.idref = ifelse(
    !is.na(auteur.idref), 
    str_glue("({auteur.idref})"), 
    NA_real_
    )
  )

theses <- 
  theses %>% 
  unite(
    col = "auteur",
    c("auteur", "auteur.idref"),
    sep = " ", 
    na.rm = TRUE
  )

  # directeurs_theses
theses <- 
  theses %>% 
  unite(
    col = "directeurs_these.0",
    c("directeurs_these.0.nom", "directeurs_these.0.prenom"), 
    sep = ", ",
    na.rm = TRUE
  )

theses <- 
  theses %>% 
  mutate(directeurs_these.0.idref = ifelse(
    !is.na(directeurs_these.0.idref),
    str_glue("({directeurs_these.0.idref})"),
    NA_real_
    )
  )

theses <- 
  theses %>% 
  unite(
    col = "directeurs_these.0",
    c("directeurs_these.0", "directeurs_these.0.idref"), 
    sep = ", ",
    na.rm = TRUE
  )

  


