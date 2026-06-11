library(arrow)
library(dplyr)
library(tidyr)

# Data extraction: ATC and CNK + more information ------------------

# amp:data
amp_data <- read_parquet("amp_data.parquet") %>% 
  distinct()

# amp:ampp:data
ampp_data <- read_parquet("ampp_data.parquet") %>% 
  distinct()

# amp:ampp:dmpp
ampp_dmpp <- read_parquet("ampp_dmpp.parquet") %>% 
  distinct()

# We can map CNK to ATC (and ATC to CNK) by doing a full join on ampp_data and ampp_dmpp
# - on three variables: vmpCode (?), code (?), cti_extended (Unique identification number of each packaging form of a medicine)
ampp <- full_join(ampp_data,ampp_dmpp, amp_data,by = c("amp_vmpCode","amp_code","cti_extended"))

# Do a simular check as before 
cnk_atc_mapping2 <- ampp %>% 
  distinct(atc,cnk) %>% 
  drop_na()
count_cnk_dup2 <- cnk_atc_mapping2 %>% 
  group_by(cnk) %>% 
  summarise(n = n())
cnk_dup2 <- count_cnk_dup2 %>% filter(n >1) %>% pull(cnk)

cnk_atc_mapping2_dup <- cnk_atc_mapping2 %>% filter(cnk %in% cnk_dup2) %>% 
  arrange(cnk) %>% 
  distinct()

test2 <- cnk_atc_mapping2 %>% 
  mutate(nchar = nchar(atc))

# Add prescription names extracted from amp:data to the data frame
names(ampp)
names(amp_data)
amp_data_names <- amp_data %>% 
  distinct(amp_vmpCode, amp_code,official_name,prescription_name_famph_nl,prescription_name_nl) 
  # group_by(amp_vmpCode,amp_code) %>% 
  # nest()

ampp_with_names <- ampp %>% 
  left_join(amp_data_names, by = c("amp_vmpCode", "amp_code"), suffix = c(".ampp",".amp")) %>% 
  distinct()

# For the sole purpose of mapping cnk/atc in intego, we keep columns: 
names(ampp_with_names)
ampp_intego <- ampp_with_names %>% 
  filter(!is.na(atc)&!is.na(cnk)) %>% 
  distinct(atc,cnk,prescription_name_famph_nl.amp,prescription_name_famph_nl.ampp, official_name) %>% 
  group_by(atc,cnk) %>% 
  fill(prescription_name_famph_nl.ampp,prescription_name_famph_nl.amp,official_name,.direction = c("down")) %>% 
  fill(prescription_name_famph_nl.ampp,prescription_name_famph_nl.amp,official_name,.direction = c("up")) %>% 
  ungroup() %>% 
  distinct()

# Add variable indicating number of cnk mapped to and number of atc mapped to 
ampp_intego <- ampp_intego %>% 
  group_by(cnk) %>% 
  mutate(mapped_to_n_atc = n_distinct(atc)) %>% 
  group_by(atc) %>% 
  mutate(mapped_to_n_cnk = n_distinct(cnk)) %>% 
  ungroup()

# Include alteration of atc/ddd: https://atcddd.fhi.no/atc_ddd_alterations__cumulative/atc_alterations/
library(readxl)
library(stringr)
df_alterations <- read_excel("atc_alterations.xlsx")

# Get one-to-one mapping 
df_alterations_unique <- df_alterations %>% 
  group_by(previous_code) %>% 
  summarise_all(~paste(.,collapse = "_")) %>% 
  ungroup() %>% 
  rowwise() %>% 
  mutate(maps_to_n_alterations = str_count(new_code,"_")+1) %>% 
  ungroup() %>% 
  select(-substance_name)

# Remove the one with "maps_to_n_alterations > 1"
# - these still exist (see website ATC/DDD)
# - All one-to-one were replaced 
df_alterations_unique <- df_alterations_unique %>% 
  filter(maps_to_n_alterations ==1) %>% 
  select(-maps_to_n_alterations)

write.csv(df_alterations_unique,"atc_alterations.csv", fileEncoding = "UTF-8", row.names = FALSE)

# Update cnk-to-atc mapping taking into account alterations
ampp_intego_alterations <- ampp_intego %>% 
  left_join(df_alterations_unique, by = c("atc"="previous_code")) %>% 
  mutate(atc_previous = atc) %>% 
  mutate(atc = case_when( 
    is.na(new_code) ~ atc_previous,
    TRUE ~new_code)) %>% 
  distinct(atc,cnk,prescription_name_famph_nl.amp) %>% group_by(cnk) %>% 
  mutate(mapped_to_n_atc = n_distinct(atc)) %>% 
  group_by(atc) %>% 
  mutate(mapped_to_n_cnk = n_distinct(cnk)) %>% 
  ungroup() 

# For cnk with multiple atc codes, collapse them into one string 
ampp_intego_alterations_collapsed <- ampp_intego_alterations %>% 
  group_by(cnk,atc) %>%
  summarise(name = paste0(prescription_name_famph_nl.amp,collapse="_"),
            mapped_to_n_atc = mapped_to_n_atc[1])  %>% 
  group_by(cnk) %>% 
  summarise(atc = paste0(atc,collapse="_"),
            name = paste0(name,collapse = "_"),
            mapped_to_n_atc = mapped_to_n_atc[1]) %>% 
  ungroup() %>% 
  arrange(cnk)
  

# Save the new cnk_atc_mapping 
write.csv(ampp_intego_alterations_collapsed, "cnk_to_atc_mapping.csv", fileEncoding = "UTF-8", row.names = FALSE)

library(arrow)  # or readr, depending on how you load
new <- read.csv("cnk_to_atc_mapping.csv")
old <- read.csv("/Users/u0121893/Library/CloudStorage/OneDrive-KULeuven/intego/projects/intego_ii_versioning/cleaning_info_prescriptions_and_vaccines/prescriptions/intego_prescription_mapping/cnk_to_atc_mapping.csv")

nrow(old); nrow(new)
length(setdiff(new$cnk, old$cnk))   # CNK codes new since 2024
length(setdiff(old$cnk, new$cnk))   # CNK codes that disappeared
