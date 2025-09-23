


# Fusion nom et prenom --------------------------------------------------------
# Extraction des préfixes complets
prefixes <- unique(sub("\\.(nom|prenom)$", "", cols))

# Pour chaque préfixe, fusionner les colonnes ayant ".nom" et ".prenom" en suffixe
for (prefix in prefixes) {
  nom_col <- paste0(prefix, ".nom")
  prenom_col <- paste0(prefix, ".prenom")
  
  if (nom_col %in% cols && prenom_col %in% cols) {
    theses <- 
      theses %>% 
      unite(
        !!sym(prefix),
        c(!!sym(nom_col), !!sym(prenom_col)),
        sep = ", ",
        na.rm = TRUE
      )
  }
}

theses <- 
  theses %>% 
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

# Etablissements de soutenance