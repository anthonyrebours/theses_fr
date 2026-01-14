##%######################################################%##
#                                                          #
####            Scraping affiliations idref             ####
#                                                          #
##%######################################################%##



# Packages ----------------------------------------------------------------
library(tidyverse)
library(rvest)
library(httr)
library(xml2)
library(progress)
library(glue)


# Liste idrefs ------------------------------------------------------------
affiliations <- arrow::read_parquet(here::here("data", "01_datagouv", "processed", "affiliations.parquet"))

affiliations <- affiliations %>% separate(variables, c("role", "order", "info"), sep = "\\.")
affiliations <- affiliations %>% pivot_wider(names_from = "info", values_from = "valeurs")
affiliations <- affiliations %>% select(-order)

##  Retrait données manquantes lorsque ni nom ni idref disponibles
affiliations <- affiliations %>% filter(!c(is.na(nom) & is.na(idref)))


idrefs <- affiliations %>% filter(!is.na(idref)) %>% distinct(idref) %>% pull()


# Bar de progrès ----------------------------------------------------------
bp <- 
  progress_bar$new(
    format = " Process [:bar] :percent in :elapsed",
    total = length(idrefs)
  )


# Fonction pour lire xml format list --------------------------------------
xml_text_in_list <- function(x) {
  xml_text(x) %>% list()
}

# Boucle for --------------------------------------------------------------
##Initialisation
all_data <- list()

## Boucle
for (idref in idrefs) {
  bp$tick()
  
  tryCatch({
    url <- glue("https://www.idref.fr/{idref}.rdf")
    response <- GET(url, timeout(20))
    
    if(status_code(response) == 404) {
      all_data[[idref]] <- data <- tibble(info = list("erreur 404"))
    }
    
    
    if(status_code(response) == 200) {
      content <- content(response, as = "text", encoding = "UTF-8")
      xml_content <- read_xml(content)
      
      data <- tibble(
        url = url,
        scraped_id = xml_content %>%
          xml_find_all(".//dcterms:identifier") %>%
          xml_text(),
        pref_name = xml_content %>%
          xml_find_all(".//foaf:Organization //skos:prefLabel") %>%
          xml_text(),
        other_labels = xml_content %>%
          xml_find_all(".//foaf:Organization //skos:altLabel") %>%
          xml_text_in_list(),
        country = xml_content %>%
          xml_find_all(".//foaf:Organization //dbpedia-owl:citizenship") %>%
          xml_attr("resource") %>% 
          list(),
        date_of_birth = xml_content %>%
          xml_find_all(".//rdau:P60524") %>%
          xml_text_in_list(),
        date_of_death = xml_content %>%
          xml_find_all(".//rdau :P60525") %>%
          xml_text_in_list(),
        information = xml_content %>%
          xml_find_all(".//rdau:P60492") %>%
          xml_text_in_list(),
        replaced_idref = xml_content %>% 
          xml_find_all(".//dcterms:replaces") %>% 
          xml_attr("resource") %>%
          str_extract(., "\\d+[A-z]*") %>%
          list(),
        predecessor = xml_content %>%
          xml_find_all(".//rdau:P60683 //skos:label") %>%
          xml_text_in_list(),
        predecessor_idref = xml_content %>%
          xml_find_all(".//rdau:P60683 //foaf:Organization") %>% 
          xml_attr("about") %>% 
          str_extract(., "\\d+[A-z]*") %>% 
          list(),
        successor = xml_content %>%
          xml_find_all(".//rdau:P60686 //skos:label") %>%
          xml_text_in_list(),
        successor_idref = xml_content %>%
          xml_find_all(".//rdau:P60686 //foaf:Organization") %>% 
          xml_attr("about") %>% 
          str_extract(., "\\d+[A-z]*") %>% 
          list(),
        subordinated = xml_content %>%
          xml_find_all("//org:hasUnit //skos:label") %>%
          xml_text_in_list(),
        subordinated_idref = xml_content %>%
          xml_find_all("//org:hasUnit //foaf:Organization") %>% 
          xml_attr("about") %>% 
          str_extract(., "\\d+[A-z]*") %>% 
          list(),
        unit_of = xml_content %>%
          xml_find_all("//org:unitOf //skos:label") %>%
          xml_text_in_list(),
        unit_of_idref = xml_content %>%
          xml_find_all("//org:unitOf //foaf:Organization") %>% 
          xml_attr("about") %>% 
          str_extract(., "\\d+[A-z]*") %>% 
          list(),
        other_link = xml_content %>%
          xml_find_all(".//owl:sameAs") %>%
          xml_attr("resource") %>% 
          list()
      )
      
      all_data[[idref]] <- data
      
    }
    
  })
  
}