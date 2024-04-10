library(arrow)
library(dplyr)
library(tidyr)

# First data extraction ---------------------------------------------------

# Merge atc and cnk data frames, created by python script, into one final data frame,
# i.e., the resulting mapping table.

atc <- read_parquet("../intego_prescription_mapping/atc.parquet")
cnk <- read_parquet("../intego_prescription_mapping/cnk.parquet")

cnk_atc_mapping <- full_join(atc,cnk) %>% distinct()
# write.csv(cnk_atc_mapping, "atc_cnk_mapping.csv")

# # Filter diabetes medication 
# diabetes <- cnk_atc_mapping %>% 
#   filter(grepl("^A10",atc))
# write.csv(diabetes, "atc_cnk_mapping_diabetes.csv")
# 
# diabetes_na <- cnk_atc_mapping %>% 
#   filter(grepl("^A10",atc)) %>% 
#   drop_na()
# write.csv(diabetes, "atc_cnk_mapping_diabetes_no_na.csv")

# Checks 
n_distinct(cnk_atc_mapping$atc)
n_distinct(cnk_atc_mapping$cnk)

cnk_atc_mapping <- drop_na(cnk_atc_mapping) %>% 
  distinct(atc, cnk)
count_cnk_dup<- cnk_atc_mapping %>% 
  group_by(cnk) %>% 
  summarise(n = n())
cnk_dup <- count_cnk_dup %>% filter(n >1) %>% pull(cnk)
cnk_atc_mapping_dup <- cnk_atc_mapping %>% filter(cnk %in% cnk_dup) %>% 
  arrange(cnk) %>% 
  distinct()



# Second data extraction: more information --------------------------------

# amp:data
amp_data <- read_parquet("../intego_prescription_mapping/amp_data.parquet") %>% 
  distinct()

# amp:ampp:data
ampp_data <- read_parquet("../intego_prescription_mapping/ampp_data.parquet") %>% 
  distinct()

# amp:ampp:dmpp
ampp_dmpp <- read_parquet("../intego_prescription_mapping/ampp_dmpp.parquet") %>% 
  distinct()

# We can map CNK to ATC (and ATC to CNK) by doing a full join on ampp_data and ampp_dmpp
ampp <- full_join(ampp_data,ampp_dmpp, amp_data,by = c("amp_vmpCode","amp_code","cti_extended"))

# Do a simular check as before 
cnk_atc_mapping2 <- ampp %>% 
  distinct(atc,cnk) %>% 
  drop_na()
count_cnk_dup <- cnk_atc_mapping2 %>% 
  group_by(cnk) %>% 
  summarise(n = n())
cnk_dup2 <- count_cnk_dup %>% filter(n >1) %>% pull(cnk)

cnk_atc_mapping2_dup <- cnk_atc_mapping2 %>% filter(cnk %in% cnk_dup2) %>% 
  arrange(cnk) %>% 
  distinct()

test2 <- cnk_atc_mapping2 %>% 
  mutate(nchar = nchar(atc))

all(cnk_atc_mapping2$cnk %in% cnk_atc_mapping$cnk)

# Add names to mapping tbale 
cnk_atc_mapping2_name <- cnk_atc_mapping2 %>% 
  left_join(ampp_data %>% distinct(atc,prescription_name_famph_nl,atc_description))

# Save the new cnk_atc_mapping 
write.csv(cnk_atc_mapping2, "atc_cnk_mapping.csv")

