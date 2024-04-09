
# Merge atc and cnk data frames, created by python script, into one final data frame,
# i.e., the resulting mapping table.

library(arrow)
library(dplyr)
library(tidyr)
atc <- read_parquet("atc.parquet")
cnk <- read_parquet("cnk.parquet")

cnk_atc_mapping <- full_join(atc,cnk) %>% distinct()
write.csv(cnk_atc_mapping, "atc_cnk_mapping.csv")

# # Filter diabetes medication 
# diabetes <- cnk_atc_mapping %>% 
#   filter(grepl("^A10",atc))
# write.csv(diabetes, "atc_cnk_mapping_diabetes.csv")
# 
# diabetes_na <- cnk_atc_mapping %>% 
#   filter(grepl("^A10",atc)) %>% 
#   drop_na()
# write.csv(diabetes, "atc_cnk_mapping_diabetes_no_na.csv")
