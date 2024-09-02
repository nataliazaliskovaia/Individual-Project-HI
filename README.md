# Micro:bit project
Project description
The company "Strawbees" for which this project was carried out sells STEAM learning materials that are compatible with the micro:bit device.

The comlany collabs with Micro:bit Educational Foundation, so is able to get a data from them about their purchases as part of the exchange.
Strawbees uses a CRM system (Hubspot) to keep track of US school districts, schools, educational resellers, e.t.c. they maintain a relationship with.
A Micro:bit dataset contains a list of US schools districts that had spent part of their budget on acquiring micro:bit devices. These schools districts are of high interest for establishing communications for Strawbees. However, in order to identify these districts within the company's CRM, the records from the Microbit dataset need to be linked to the CRM records. 
The project involves matching these two datasets through exact and fuzzy linking techniques.

# Skills developed:
Importing data into a script for processing

Data cleaning

Data manipulation

Joining datasets

Record linkage (fuzzy matching)

In the interests of the company, data on the total amounts and number of purchases in the micro:bit dataset are hidden in accordance with the NDA.

# Micro:bit project outline:

CSV_files for the project:
1. Micro:bit school districts file from collaborator - number of district to match - 1897.
2. US school districts file from NCES - official source of National Center for Education Statistics 
https://nces.ed.gov/ccd/files.asp#Fiscal:2,LevelId:5,SchoolYearId:38,Page:1 - file location.
Total number of US school districts - 19627.

Part 1: Preparing micro:bit district file for the import.

Part 2: Preparing US school districts import file.

Part 3: Matching through exact name.

Part 4: Record linkage through similar names. 

# Imports required for the project
Importing Pandas library for data manipulation and Fuzz (a string matching library) for fuzzy string matching operations.
Fuzzy string matching refers to finding strings that are approximately rather than exactly equal, allowing for minor differences like typos or variations.

import pandas as pd
from thefuzz import fuzz, process

# Exploring and cleaning data

data_nces = pd.read_csv("school_districts_NCES.csv",dtype={'LEAID':str})

data_microbit = pd.read_csv("school_districts_micro_bit.csv")

data_nces.head(2)

data_microbit.head(2)

#Number of districts to match
data_microbit.shape[0]

Part 1. Preparing micro:bit district file for the import
Exploring Row partner's data requirued for the project, filter out unneeded, empty data. 
(According NDA data in columns 'Total spend (approx)' and 'Number of purchases' are  hidden)
Spliting 'Agency' column into 'District Name' and 'State'

data_microbit.drop(labels=["Unnamed: 1", "Number of purchases"], axis=1, inplace=True)
data_microbit.head(2)

data_microbit[['district','state']] = data_microbit["Agency"].str.split(pat=", ", n=1, expand=True)
data_microbit.head(2)

Part 2: Preparing US school districts import file.

Filtering out unneeded columns (keep at least STATENAME, LEAID, LEA_NAME)
Important: Importing LEAID as string rather than integer. Using dtype parameter from pandas.read_csv (using this source for the project and skills developing).
Adding prefix (NCES_District-) to LEAID in order to create National Data ID column (similar with the Property name in Hubspot SRM Strawbees DataBase).
Capitalising DISTRICTNAME, STATENAME columns (other if needed) to match the values in micro.bit file.
Capitalising all distcricts names in both files for more accurate matches.

data_nces.columns

labels_to_drop = ["SCHOOL_YEAR", "FIPST", "ST", "STATE_AGENCY_NO", "UNION", "ST_LEAID", "MSTREET2", "MSTREET3", "MSTATE", "MZIP", "MZIP4", "LSTREET1", "LSTREET2", "LSTREET3", 
"LCITY", "LSTATE", "LZIP", "LZIP4", "PHONE", "SY_STATUS", "SY_STATUS_TEXT", "UPDATED_STATUS", "UPDATED_STATUS_TEXT",
"EFFECTIVE_DATE", "LEA_TYPE", "LEA_TYPE_TEXT", "OUT_OF_STATE_FLAG", "CHARTER_LEA", "CHARTER_LEA_TEXT", "NOGRADES", "G_PK_OFFERED",
"G_KG_OFFERED", "G_1_OFFERED", "G_2_OFFERED", "G_3_OFFERED", "G_4_OFFERED", "G_5_OFFERED", "G_6_OFFERED", "G_7_OFFERED",
"G_8_OFFERED", "G_9_OFFERED", "G_10_OFFERED", "G_11_OFFERED", "G_12_OFFERED", "G_13_OFFERED", "G_UG_OFFERED", "G_AE_OFFERED", "GSLO",
"GSHI", "LEVEL", "IGOFFERED", "OPERATIONAL_SCHOOLS"]

data_nces.drop(labels=labels_to_drop, axis=1, inplace=True)
data_nces.head(2)

prefix = 'NCES_District-'
data_nces['LEAID'] = prefix + data_nces['LEAID']
data_nces.head(2)

mapping = {'LEAID': 'NATIONAL DATA ID', 'LEA_NAME': 'DISTRICT_NAME', 'MSTREET1': 'STREET_ADDRESS', 'MCITY': 'CITY', 
'WEBSITE': 'COMPANY_DOMAIN'}
data_nces = data_nces.rename(columns=mapping)
data_nces.head(2)

total_districts = len(data_nces)
print(f"Number of total districts in the dataframe: {total_districts}")

data_nces['DISTRICT_NAME'] = data_nces['DISTRICT_NAME'].str.upper()
data_nces.head(3)

# Joining data_microbit with data_nces dataframes.

Part 3: Matching through exact name.

Renaming columns in the right order.
Merging/joining micro:bit dataframe with NCES dataframe through exact District name and State name 
(Merge type (how) affects the results - I use 'Left' in order to keep all rows from the left dataframe (data_microbit), and only matching rows from the right (data_nces).)
Output: micro:bit dataframe has a column with National Data ID, which is empty for some districts (those without exact match) and filled for those with exact.
Spliting resulting dataframe into those rows with National data ID - to be imported; and the ones without National data ID - be able to work on Part 4 of the project.

data_microbit = data_microbit.rename(columns={'district': 'DISTRICT_NAME', 'state': 'STATENAME'})
data_microbit.head(3)

data_microbit['STATENAME'] = data_microbit['STATENAME'].str.upper()
data_microbit.head(3)

data_microbit['DISTRICT_NAME'] = data_microbit['DISTRICT_NAME'].str.upper()
data_microbit.head(3)

data_microbit.dtypes

#merging dataframes
merged_data_microbit_nces = pd.merge(data_microbit, data_nces, 
                     left_on=['DISTRICT_NAME', 'STATENAME'], 
                     right_on=['DISTRICT_NAME', 'STATENAME'],
                     how='left') 

merged_data_microbit_nces.head(10)

total_districts = len(merged_data_microbit_nces)
print(f"Number of total districts in the merged dataframe: {total_districts}")

#sampling dataframes with matched and non-matched school districts
nationaldataid_present = merged_data_microbit_nces[merged_data_microbit_nces['NATIONAL DATA ID'].notna()]
nationaldataid_missing = merged_data_microbit_nces[merged_data_microbit_nces['NATIONAL DATA ID'].isna()]

nationaldataid_present.shape[0]

nationaldataid_missing.shape[0]

# Record linkage / fuzzy matching

For fuzzy matching - Skills based on course "Cleaning data with pandas" in DataCamp.

nationaldataid_present.to_csv('nationaldataid_present.csv', index=False)
nationaldataid_missing.to_csv('nationaldataid_missing.csv', index=False)

# Fuzzy matching code

# Initialize needed lists to store results
match_count = 0
matched_ids = []
street = []
city = []
domain = []
names = []

# Iterate through each row in nationaldataid_missing
for index, row in nationaldataid_missing.iterrows():
    district_to_match = row['DISTRICT_NAME']
    state = row['STATENAME']
    
    # Convert values to strings 
    district_to_match = str(district_to_match) if not pd.isna(district_to_match) else 'Unknown'
    state = str(state) if not pd.isna(state) else 'Unknown'
    
    #print(f"Matching for: {district_to_match}: {state}")

    data_nces['Match_Score'] = data_nces['DISTRICT_NAME'].apply(lambda x: fuzz.WRatio(district_to_match, str(x)))
    
    # Filter rows by state and sort by match score
    best_match_row = data_nces[(data_nces['STATENAME'] == state)].sort_values('Match_Score', ascending=False).reset_index()
    
    # How we check if we have a matching row
    if not best_match_row.empty:
        best_match_name = best_match_row.loc[0, 'DISTRICT_NAME']
        national_id = best_match_row.loc[0, 'NATIONAL DATA ID']
        match_street = best_match_row.loc[0, 'STREET_ADDRESS']
        match_city = best_match_row.loc[0, 'CITY']
        matched_domain = best_match_row.loc[0, 'COMPANY_DOMAIN']
        #print(f"Best match: {best_match_name} with National ID: {national_id}")
        matched_ids.append(national_id)
        street.append(match_street)
        city.append(match_city)
        domain.append(matched_domain)
        names.append(best_match_name)
        match_count += 1  # Increment match count
    else:
        #print("No match found.")
        matched_ids.append(None)
        names.append(None)
        street.append(None)
        city.append(None)
        domain.append(None)

# Add the matched National IDs to nationaldataid_missing data_frame
nationaldataid_missing['NATIONAL DATA ID'] = matched_ids
nationaldataid_missing['matched_name'] = names
nationaldataid_missing['STREET_ADDRESS'] = street
nationaldataid_missing['CITY'] = city
nationaldataid_missing['COMPANY_DOMAIN'] = domain

print(f"Total number of matches: {match_count}")

# Filter rows by district name and sort by match score to get the best match
data_nces['Match_Score'] = data_nces['DISTRICT_NAME'].apply(lambda x: fuzz.WRatio('TRACY UNIFIED SCHOOL DISTRICT', str(x)))
best_match_row = data_nces[(data_nces['STATENAME'] == state)].sort_values('Match_Score', ascending=False).reset_index()

# Check how does the funcrion works
best_match_row.loc[0,'DISTRICT_NAME']

# There's 3 states that had commas in their statename and were incorrectly matched
# Keep them for manual correction in the final csv.file before doing import sorted and organised data into the Strawbees Hubspot CRM DataBase.

nationaldataid_missing[nationaldataid_missing['NATIONAL DATA ID'].isna()]

# Check the number of all metched districts
print(f"Total number of matches: {match_count}")

# Joining data frames nationaldataid_missing and nationaldataid_present

Part 5: Merging resulting dataframes for final version of school districts with all needed data ready for the import in Strawbees Hubspot CRM Database.

# Preparing final dataset for the import micro:bit data:
# 1. Combining both datasets
# 2. Adding "use_microbit" column with value "Yes"
# 3. Exporting to spreadsheet for manual correction

# Delete no needed columns
nationaldataid_missing.drop(labels=["matched_name"], axis=1, inplace=True)
nationaldataid_missing.head(2)

# Show total number of values after fuzzy matching
nationaldataid_missing.shape[0]

nationaldataid_present.head()

# Combining both datasets after data exploring, manipulation and merging
microbitdata_for_import = pd.concat([nationaldataid_missing, nationaldataid_present], ignore_index=True)
microbitdata_for_import.head()

total_districts = len(microbitdata_for_import)

Adding coulmn "use_microbit" (similar with the Property name in Hubspot SRM Strawbees DataBase).

# Adding "use_microbit" column with value "Yes" 
microbitdata_for_import["use_microbit"] = "Yes"
microbitdata_for_import.head()

# Exporting final resulting dataframe to spreadsheet for manual correction
microbitdata_for_import.to_csv('microbitdata_for_import.csv', index=False)


Part 5: Merging resulting dataframes for the final version.
