{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Micro:bit project\n",
    "Project description\n",
    "The company \"Strawbees\" for which this project was carried out sells STEAM learning materials that are compatible with the micro:bit device.\n",
    "\n",
    "The comlany collabs with Micro:bit Educational Foundation, so is able to get a data from them about their purchases as part of the exchange.\n",
    "Strawbees uses a CRM system (Hubspot) to keep track of US school districts, schools, educational resellers, e.t.c. they maintain a relationship with.\n",
    "A Micro:bit dataset contains a list of US schools districts that had spent part of their budget on acquiring micro:bit devices. These schools districts are of high interest for establishing communications for Strawbees. However, in order to identify these districts within the company's CRM, the records from the Microbit dataset need to be linked to the CRM records. \n",
    "The project involves matching these two datasets through exact and fuzzy linking techniques.\n",
    "\n",
    "# Skills developed:\n",
    "Importing data into a script for processing\n",
    "\n",
    "Data cleaning\n",
    "\n",
    "Data manipulation\n",
    "\n",
    "Joining datasets\n",
    "\n",
    "Record linkage (fuzzy matching)\n",
    "\n",
    "In the interests of the company, data on the total amounts and number of purchases in the micro:bit dataset are hidden in accordance with the NDA."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Micro:bit project outline:\n",
    "\n",
    "CSV_files for the project:\n",
    "1. Micro:bit school districts file from collaborator - number of district to match - 1897.\n",
    "2. US school districts file from NCES - official source of National Center for Education Statistics \n",
    "https://nces.ed.gov/ccd/files.asp#Fiscal:2,LevelId:5,SchoolYearId:38,Page:1 - file location.\n",
    "Total number of US school districts - 19627.\n",
    "\n",
    "Part 1: Preparing micro:bit district file for the import.\n",
    "\n",
    "Part 2: Preparing US school districts import file.\n",
    "\n",
    "Part 3: Matching through exact name.\n",
    "\n",
    "Part 4: Record linkage through similar names. \n",
    "\n",
    "Part 5: Merging resulting dataframes for the final version."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Imports required for the project\n",
    "Importing Pandas library for data manipulation and Fuzz (a string matching library) for fuzzy string matching operations.\n",
    "Fuzzy string matching refers to finding strings that are approximately rather than exactly equal, allowing for minor differences like typos or variations."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [],
   "source": [
    "import pandas as pd\n",
    "from thefuzz import fuzz, process"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Exploring and cleaning data"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "C:\\Users\\zalis\\AppData\\Local\\Temp\\ipykernel_15152\\3209038505.py:1: DtypeWarning: Columns (11) have mixed types. Specify dtype option on import or set low_memory=False.\n",
      "  data_nces = pd.read_csv(\"school_districts_NCES.csv\",dtype={'LEAID':str})\n"
     ]
    }
   ],
   "source": [
    "data_nces = pd.read_csv(\"school_districts_NCES.csv\",dtype={'LEAID':str})"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {},
   "outputs": [],
   "source": [
    "data_microbit = pd.read_csv(\"school_districts_micro_bit.csv\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>SCHOOL_YEAR</th>\n",
       "      <th>FIPST</th>\n",
       "      <th>STATENAME</th>\n",
       "      <th>ST</th>\n",
       "      <th>LEA_NAME</th>\n",
       "      <th>STATE_AGENCY_NO</th>\n",
       "      <th>UNION</th>\n",
       "      <th>ST_LEAID</th>\n",
       "      <th>LEAID</th>\n",
       "      <th>MSTREET1</th>\n",
       "      <th>...</th>\n",
       "      <th>G_11_OFFERED</th>\n",
       "      <th>G_12_OFFERED</th>\n",
       "      <th>G_13_OFFERED</th>\n",
       "      <th>G_UG_OFFERED</th>\n",
       "      <th>G_AE_OFFERED</th>\n",
       "      <th>GSLO</th>\n",
       "      <th>GSHI</th>\n",
       "      <th>LEVEL</th>\n",
       "      <th>IGOFFERED</th>\n",
       "      <th>OPERATIONAL_SCHOOLS</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>2023-2024</td>\n",
       "      <td>1</td>\n",
       "      <td>ALABAMA</td>\n",
       "      <td>AL</td>\n",
       "      <td>Alabama Youth Services</td>\n",
       "      <td>1</td>\n",
       "      <td>NaN</td>\n",
       "      <td>AL-210</td>\n",
       "      <td>0100002</td>\n",
       "      <td>1000 Industrial School Road</td>\n",
       "      <td>...</td>\n",
       "      <td>Yes</td>\n",
       "      <td>Yes</td>\n",
       "      <td>No</td>\n",
       "      <td>No</td>\n",
       "      <td>No</td>\n",
       "      <td>KG</td>\n",
       "      <td>12</td>\n",
       "      <td>Other</td>\n",
       "      <td>As reported</td>\n",
       "      <td>0</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>2023-2024</td>\n",
       "      <td>1</td>\n",
       "      <td>ALABAMA</td>\n",
       "      <td>AL</td>\n",
       "      <td>Albertville City</td>\n",
       "      <td>1</td>\n",
       "      <td>NaN</td>\n",
       "      <td>AL-101</td>\n",
       "      <td>0100005</td>\n",
       "      <td>8379 US Highway 431</td>\n",
       "      <td>...</td>\n",
       "      <td>Yes</td>\n",
       "      <td>Yes</td>\n",
       "      <td>No</td>\n",
       "      <td>No</td>\n",
       "      <td>No</td>\n",
       "      <td>PK</td>\n",
       "      <td>12</td>\n",
       "      <td>Other</td>\n",
       "      <td>As reported</td>\n",
       "      <td>6</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "<p>2 rows × 58 columns</p>\n",
       "</div>"
      ],
      "text/plain": [
       "  SCHOOL_YEAR  FIPST STATENAME  ST                LEA_NAME  STATE_AGENCY_NO  \\\n",
       "0   2023-2024      1   ALABAMA  AL  Alabama Youth Services                1   \n",
       "1   2023-2024      1   ALABAMA  AL        Albertville City                1   \n",
       "\n",
       "   UNION ST_LEAID    LEAID                     MSTREET1  ... G_11_OFFERED  \\\n",
       "0    NaN   AL-210  0100002  1000 Industrial School Road  ...          Yes   \n",
       "1    NaN   AL-101  0100005          8379 US Highway 431  ...          Yes   \n",
       "\n",
       "  G_12_OFFERED G_13_OFFERED G_UG_OFFERED  G_AE_OFFERED  GSLO GSHI  LEVEL  \\\n",
       "0          Yes           No           No            No    KG   12  Other   \n",
       "1          Yes           No           No            No    PK   12  Other   \n",
       "\n",
       "     IGOFFERED OPERATIONAL_SCHOOLS  \n",
       "0  As reported                   0  \n",
       "1  As reported                   6  \n",
       "\n",
       "[2 rows x 58 columns]"
      ]
     },
     "execution_count": 4,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "data_nces.head(2)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>Agency</th>\n",
       "      <th>Unnamed: 1</th>\n",
       "      <th>Total spend (approx)</th>\n",
       "      <th>Number of purchases</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>State College Area School District, Pennsylvania</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NDA</td>\n",
       "      <td>NDA</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>Carson City School District, Nevada</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NDA</td>\n",
       "      <td>NDA</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "                                             Agency Unnamed: 1  \\\n",
       "0  State College Area School District, Pennsylvania        NaN   \n",
       "1               Carson City School District, Nevada        NaN   \n",
       "\n",
       "  Total spend (approx) Number of purchases  \n",
       "0                  NDA                 NDA  \n",
       "1                  NDA                 NDA  "
      ]
     },
     "execution_count": 5,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "data_microbit.head(2)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 41,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "1896"
      ]
     },
     "execution_count": 41,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#Number of districts to match\n",
    "data_microbit.shape[0]"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Part 1. Preparing micro:bit district file for the import\n",
    "Exploring Row partner's data requirued for the project, filter out unneeded, empty data. \n",
    "(According NDA data in columns 'Total spend (approx)' and 'Number of purchases' are  hidden)\n",
    "Spliting 'Agency' column into 'District Name' and 'State'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>Agency</th>\n",
       "      <th>Total spend (approx)</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>State College Area School District, Pennsylvania</td>\n",
       "      <td>NDA</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>Carson City School District, Nevada</td>\n",
       "      <td>NDA</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "                                             Agency Total spend (approx)\n",
       "0  State College Area School District, Pennsylvania                  NDA\n",
       "1               Carson City School District, Nevada                  NDA"
      ]
     },
     "execution_count": 7,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "data_microbit.drop(labels=[\"Unnamed: 1\", \"Number of purchases\"], axis=1, inplace=True)\n",
    "data_microbit.head(2)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>Agency</th>\n",
       "      <th>Total spend (approx)</th>\n",
       "      <th>district</th>\n",
       "      <th>state</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>State College Area School District, Pennsylvania</td>\n",
       "      <td>NDA</td>\n",
       "      <td>State College Area School District</td>\n",
       "      <td>Pennsylvania</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>Carson City School District, Nevada</td>\n",
       "      <td>NDA</td>\n",
       "      <td>Carson City School District</td>\n",
       "      <td>Nevada</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "                                             Agency Total spend (approx)  \\\n",
       "0  State College Area School District, Pennsylvania                  NDA   \n",
       "1               Carson City School District, Nevada                  NDA   \n",
       "\n",
       "                             district         state  \n",
       "0  State College Area School District  Pennsylvania  \n",
       "1         Carson City School District        Nevada  "
      ]
     },
     "execution_count": 8,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "data_microbit[['district','state']] = data_microbit[\"Agency\"].str.split(pat=\", \", n=1, expand=True)\n",
    "data_microbit.head(2)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Part 2: Preparing US school districts import file.\n",
    "\n",
    "Filtering out unneeded columns (keep at least STATENAME, LEAID, LEA_NAME)\n",
    "Important: Importing LEAID as string rather than integer. Using dtype parameter from pandas.read_csv (using this source for the project and skills developing).\n",
    "Adding prefix (NCES_District-) to LEAID in order to create National Data ID column (similar with the Property name in Hubspot SRM Strawbees DataBase).\n",
    "Capitalising DISTRICTNAME, STATENAME columns (other if needed) to match the values in micro.bit file.\n",
    "Capitalising all distcricts names in both files for more accurate matches."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "Index(['SCHOOL_YEAR', 'FIPST', 'STATENAME', 'ST', 'LEA_NAME',\n",
       "       'STATE_AGENCY_NO', 'UNION', 'ST_LEAID', 'LEAID', 'MSTREET1', 'MSTREET2',\n",
       "       'MSTREET3', 'MCITY', 'MSTATE', 'MZIP', 'MZIP4', 'LSTREET1', 'LSTREET2',\n",
       "       'LSTREET3', 'LCITY', 'LSTATE', 'LZIP', 'LZIP4', 'PHONE', 'WEBSITE',\n",
       "       'SY_STATUS', 'SY_STATUS_TEXT', 'UPDATED_STATUS', 'UPDATED_STATUS_TEXT',\n",
       "       'EFFECTIVE_DATE', 'LEA_TYPE', 'LEA_TYPE_TEXT', 'OUT_OF_STATE_FLAG',\n",
       "       'CHARTER_LEA', 'CHARTER_LEA_TEXT', 'NOGRADES', 'G_PK_OFFERED',\n",
       "       'G_KG_OFFERED', 'G_1_OFFERED', 'G_2_OFFERED', 'G_3_OFFERED',\n",
       "       'G_4_OFFERED', 'G_5_OFFERED', 'G_6_OFFERED', 'G_7_OFFERED',\n",
       "       'G_8_OFFERED', 'G_9_OFFERED', 'G_10_OFFERED', 'G_11_OFFERED',\n",
       "       'G_12_OFFERED', 'G_13_OFFERED', 'G_UG_OFFERED', 'G_AE_OFFERED', 'GSLO',\n",
       "       'GSHI', 'LEVEL', 'IGOFFERED', 'OPERATIONAL_SCHOOLS'],\n",
       "      dtype='object')"
      ]
     },
     "execution_count": 9,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "data_nces.columns"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "metadata": {},
   "outputs": [],
   "source": [
    "labels_to_drop = [\"SCHOOL_YEAR\", \"FIPST\", \"ST\", \"STATE_AGENCY_NO\", \"UNION\", \"ST_LEAID\", \"MSTREET2\", \"MSTREET3\", \"MSTATE\", \"MZIP\", \"MZIP4\", \"LSTREET1\", \"LSTREET2\", \"LSTREET3\", \n",
    "\"LCITY\", \"LSTATE\", \"LZIP\", \"LZIP4\", \"PHONE\", \"SY_STATUS\", \"SY_STATUS_TEXT\", \"UPDATED_STATUS\", \"UPDATED_STATUS_TEXT\",\n",
    "\"EFFECTIVE_DATE\", \"LEA_TYPE\", \"LEA_TYPE_TEXT\", \"OUT_OF_STATE_FLAG\", \"CHARTER_LEA\", \"CHARTER_LEA_TEXT\", \"NOGRADES\", \"G_PK_OFFERED\",\n",
    "\"G_KG_OFFERED\", \"G_1_OFFERED\", \"G_2_OFFERED\", \"G_3_OFFERED\", \"G_4_OFFERED\", \"G_5_OFFERED\", \"G_6_OFFERED\", \"G_7_OFFERED\",\n",
    "\"G_8_OFFERED\", \"G_9_OFFERED\", \"G_10_OFFERED\", \"G_11_OFFERED\", \"G_12_OFFERED\", \"G_13_OFFERED\", \"G_UG_OFFERED\", \"G_AE_OFFERED\", \"GSLO\",\n",
    "\"GSHI\", \"LEVEL\", \"IGOFFERED\", \"OPERATIONAL_SCHOOLS\"]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>STATENAME</th>\n",
       "      <th>LEA_NAME</th>\n",
       "      <th>LEAID</th>\n",
       "      <th>MSTREET1</th>\n",
       "      <th>MCITY</th>\n",
       "      <th>WEBSITE</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>ALABAMA</td>\n",
       "      <td>Alabama Youth Services</td>\n",
       "      <td>0100002</td>\n",
       "      <td>1000 Industrial School Road</td>\n",
       "      <td>Mt Meigs</td>\n",
       "      <td>http://www.dys.alabama.gov/school-district.html</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>ALABAMA</td>\n",
       "      <td>Albertville City</td>\n",
       "      <td>0100005</td>\n",
       "      <td>8379 US Highway 431</td>\n",
       "      <td>Albertville</td>\n",
       "      <td>http://www.albertk12.org</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "  STATENAME                LEA_NAME    LEAID                     MSTREET1  \\\n",
       "0   ALABAMA  Alabama Youth Services  0100002  1000 Industrial School Road   \n",
       "1   ALABAMA        Albertville City  0100005          8379 US Highway 431   \n",
       "\n",
       "         MCITY                                          WEBSITE  \n",
       "0     Mt Meigs  http://www.dys.alabama.gov/school-district.html  \n",
       "1  Albertville                         http://www.albertk12.org  "
      ]
     },
     "execution_count": 11,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "data_nces.drop(labels=labels_to_drop, axis=1, inplace=True)\n",
    "data_nces.head(2)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>STATENAME</th>\n",
       "      <th>LEA_NAME</th>\n",
       "      <th>LEAID</th>\n",
       "      <th>MSTREET1</th>\n",
       "      <th>MCITY</th>\n",
       "      <th>WEBSITE</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>ALABAMA</td>\n",
       "      <td>Alabama Youth Services</td>\n",
       "      <td>NCES_District-0100002</td>\n",
       "      <td>1000 Industrial School Road</td>\n",
       "      <td>Mt Meigs</td>\n",
       "      <td>http://www.dys.alabama.gov/school-district.html</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>ALABAMA</td>\n",
       "      <td>Albertville City</td>\n",
       "      <td>NCES_District-0100005</td>\n",
       "      <td>8379 US Highway 431</td>\n",
       "      <td>Albertville</td>\n",
       "      <td>http://www.albertk12.org</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "  STATENAME                LEA_NAME                  LEAID  \\\n",
       "0   ALABAMA  Alabama Youth Services  NCES_District-0100002   \n",
       "1   ALABAMA        Albertville City  NCES_District-0100005   \n",
       "\n",
       "                      MSTREET1        MCITY  \\\n",
       "0  1000 Industrial School Road     Mt Meigs   \n",
       "1          8379 US Highway 431  Albertville   \n",
       "\n",
       "                                           WEBSITE  \n",
       "0  http://www.dys.alabama.gov/school-district.html  \n",
       "1                         http://www.albertk12.org  "
      ]
     },
     "execution_count": 12,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "prefix = 'NCES_District-'\n",
    "data_nces['LEAID'] = prefix + data_nces['LEAID']\n",
    "data_nces.head(2)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>STATENAME</th>\n",
       "      <th>DISTRICT_NAME</th>\n",
       "      <th>NATIONAL DATA ID</th>\n",
       "      <th>STREET_ADDRESS</th>\n",
       "      <th>CITY</th>\n",
       "      <th>COMPANY_DOMAIN</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>ALABAMA</td>\n",
       "      <td>Alabama Youth Services</td>\n",
       "      <td>NCES_District-0100002</td>\n",
       "      <td>1000 Industrial School Road</td>\n",
       "      <td>Mt Meigs</td>\n",
       "      <td>http://www.dys.alabama.gov/school-district.html</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>ALABAMA</td>\n",
       "      <td>Albertville City</td>\n",
       "      <td>NCES_District-0100005</td>\n",
       "      <td>8379 US Highway 431</td>\n",
       "      <td>Albertville</td>\n",
       "      <td>http://www.albertk12.org</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "  STATENAME           DISTRICT_NAME       NATIONAL DATA ID  \\\n",
       "0   ALABAMA  Alabama Youth Services  NCES_District-0100002   \n",
       "1   ALABAMA        Albertville City  NCES_District-0100005   \n",
       "\n",
       "                STREET_ADDRESS         CITY  \\\n",
       "0  1000 Industrial School Road     Mt Meigs   \n",
       "1          8379 US Highway 431  Albertville   \n",
       "\n",
       "                                    COMPANY_DOMAIN  \n",
       "0  http://www.dys.alabama.gov/school-district.html  \n",
       "1                         http://www.albertk12.org  "
      ]
     },
     "execution_count": 13,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "mapping = {'LEAID': 'NATIONAL DATA ID', 'LEA_NAME': 'DISTRICT_NAME', 'MSTREET1': 'STREET_ADDRESS', 'MCITY': 'CITY', \n",
    "'WEBSITE': 'COMPANY_DOMAIN'}\n",
    "data_nces = data_nces.rename(columns=mapping)\n",
    "data_nces.head(2)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Number of total districts in the dataframe: 19627\n"
     ]
    }
   ],
   "source": [
    "total_districts = len(data_nces)\n",
    "print(f\"Number of total districts in the dataframe: {total_districts}\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 15,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>STATENAME</th>\n",
       "      <th>DISTRICT_NAME</th>\n",
       "      <th>NATIONAL DATA ID</th>\n",
       "      <th>STREET_ADDRESS</th>\n",
       "      <th>CITY</th>\n",
       "      <th>COMPANY_DOMAIN</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>ALABAMA</td>\n",
       "      <td>ALABAMA YOUTH SERVICES</td>\n",
       "      <td>NCES_District-0100002</td>\n",
       "      <td>1000 Industrial School Road</td>\n",
       "      <td>Mt Meigs</td>\n",
       "      <td>http://www.dys.alabama.gov/school-district.html</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>ALABAMA</td>\n",
       "      <td>ALBERTVILLE CITY</td>\n",
       "      <td>NCES_District-0100005</td>\n",
       "      <td>8379 US Highway 431</td>\n",
       "      <td>Albertville</td>\n",
       "      <td>http://www.albertk12.org</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>ALABAMA</td>\n",
       "      <td>MARSHALL COUNTY</td>\n",
       "      <td>NCES_District-0100006</td>\n",
       "      <td>12380 US Highway 431 S</td>\n",
       "      <td>Guntersville</td>\n",
       "      <td>http://www.marshallk12.org</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "  STATENAME           DISTRICT_NAME       NATIONAL DATA ID  \\\n",
       "0   ALABAMA  ALABAMA YOUTH SERVICES  NCES_District-0100002   \n",
       "1   ALABAMA        ALBERTVILLE CITY  NCES_District-0100005   \n",
       "2   ALABAMA         MARSHALL COUNTY  NCES_District-0100006   \n",
       "\n",
       "                STREET_ADDRESS          CITY  \\\n",
       "0  1000 Industrial School Road      Mt Meigs   \n",
       "1          8379 US Highway 431   Albertville   \n",
       "2       12380 US Highway 431 S  Guntersville   \n",
       "\n",
       "                                    COMPANY_DOMAIN  \n",
       "0  http://www.dys.alabama.gov/school-district.html  \n",
       "1                         http://www.albertk12.org  \n",
       "2                       http://www.marshallk12.org  "
      ]
     },
     "execution_count": 15,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "data_nces['DISTRICT_NAME'] = data_nces['DISTRICT_NAME'].str.upper()\n",
    "data_nces.head(3)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Joining data_microbit with data_nces dataframes.\n",
    "\n",
    "Part 3: Matching through exact name.\n",
    "\n",
    "Renaming columns in the right order.\n",
    "Merging/joining micro:bit dataframe with NCES dataframe through exact District name and State name \n",
    "(Merge type (how) affects the results - I use 'Left' in order to keep all rows from the left dataframe (data_microbit), and only matching rows from the right (data_nces).)\n",
    "Output: micro:bit dataframe has a column with National Data ID, which is empty for some districts (those without exact match) and filled for those with exact.\n",
    "Spliting resulting dataframe into those rows with National data ID - to be imported; and the ones without National data ID - be able to work on Part 4 of the project."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>Agency</th>\n",
       "      <th>Total spend (approx)</th>\n",
       "      <th>DISTRICT_NAME</th>\n",
       "      <th>STATENAME</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>State College Area School District, Pennsylvania</td>\n",
       "      <td>NDA</td>\n",
       "      <td>State College Area School District</td>\n",
       "      <td>Pennsylvania</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>Carson City School District, Nevada</td>\n",
       "      <td>NDA</td>\n",
       "      <td>Carson City School District</td>\n",
       "      <td>Nevada</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>Gwinnett County Public Schools, Georgia</td>\n",
       "      <td>NDA</td>\n",
       "      <td>Gwinnett County Public Schools</td>\n",
       "      <td>Georgia</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "                                             Agency Total spend (approx)  \\\n",
       "0  State College Area School District, Pennsylvania                  NDA   \n",
       "1               Carson City School District, Nevada                  NDA   \n",
       "2           Gwinnett County Public Schools, Georgia                  NDA   \n",
       "\n",
       "                        DISTRICT_NAME     STATENAME  \n",
       "0  State College Area School District  Pennsylvania  \n",
       "1         Carson City School District        Nevada  \n",
       "2      Gwinnett County Public Schools       Georgia  "
      ]
     },
     "execution_count": 16,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "data_microbit = data_microbit.rename(columns={'district': 'DISTRICT_NAME', 'state': 'STATENAME'})\n",
    "data_microbit.head(3)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 17,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>Agency</th>\n",
       "      <th>Total spend (approx)</th>\n",
       "      <th>DISTRICT_NAME</th>\n",
       "      <th>STATENAME</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>State College Area School District, Pennsylvania</td>\n",
       "      <td>NDA</td>\n",
       "      <td>State College Area School District</td>\n",
       "      <td>PENNSYLVANIA</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>Carson City School District, Nevada</td>\n",
       "      <td>NDA</td>\n",
       "      <td>Carson City School District</td>\n",
       "      <td>NEVADA</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>Gwinnett County Public Schools, Georgia</td>\n",
       "      <td>NDA</td>\n",
       "      <td>Gwinnett County Public Schools</td>\n",
       "      <td>GEORGIA</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "                                             Agency Total spend (approx)  \\\n",
       "0  State College Area School District, Pennsylvania                  NDA   \n",
       "1               Carson City School District, Nevada                  NDA   \n",
       "2           Gwinnett County Public Schools, Georgia                  NDA   \n",
       "\n",
       "                        DISTRICT_NAME     STATENAME  \n",
       "0  State College Area School District  PENNSYLVANIA  \n",
       "1         Carson City School District        NEVADA  \n",
       "2      Gwinnett County Public Schools       GEORGIA  "
      ]
     },
     "execution_count": 17,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "data_microbit['STATENAME'] = data_microbit['STATENAME'].str.upper()\n",
    "data_microbit.head(3)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 18,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>Agency</th>\n",
       "      <th>Total spend (approx)</th>\n",
       "      <th>DISTRICT_NAME</th>\n",
       "      <th>STATENAME</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>State College Area School District, Pennsylvania</td>\n",
       "      <td>NDA</td>\n",
       "      <td>STATE COLLEGE AREA SCHOOL DISTRICT</td>\n",
       "      <td>PENNSYLVANIA</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>Carson City School District, Nevada</td>\n",
       "      <td>NDA</td>\n",
       "      <td>CARSON CITY SCHOOL DISTRICT</td>\n",
       "      <td>NEVADA</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>Gwinnett County Public Schools, Georgia</td>\n",
       "      <td>NDA</td>\n",
       "      <td>GWINNETT COUNTY PUBLIC SCHOOLS</td>\n",
       "      <td>GEORGIA</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "                                             Agency Total spend (approx)  \\\n",
       "0  State College Area School District, Pennsylvania                  NDA   \n",
       "1               Carson City School District, Nevada                  NDA   \n",
       "2           Gwinnett County Public Schools, Georgia                  NDA   \n",
       "\n",
       "                        DISTRICT_NAME     STATENAME  \n",
       "0  STATE COLLEGE AREA SCHOOL DISTRICT  PENNSYLVANIA  \n",
       "1         CARSON CITY SCHOOL DISTRICT        NEVADA  \n",
       "2      GWINNETT COUNTY PUBLIC SCHOOLS       GEORGIA  "
      ]
     },
     "execution_count": 18,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "data_microbit['DISTRICT_NAME'] = data_microbit['DISTRICT_NAME'].str.upper()\n",
    "data_microbit.head(3)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 19,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "Agency                  object\n",
       "Total spend (approx)    object\n",
       "DISTRICT_NAME           object\n",
       "STATENAME               object\n",
       "dtype: object"
      ]
     },
     "execution_count": 19,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "data_microbit.dtypes"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 20,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>Agency</th>\n",
       "      <th>Total spend (approx)</th>\n",
       "      <th>DISTRICT_NAME</th>\n",
       "      <th>STATENAME</th>\n",
       "      <th>NATIONAL DATA ID</th>\n",
       "      <th>STREET_ADDRESS</th>\n",
       "      <th>CITY</th>\n",
       "      <th>COMPANY_DOMAIN</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>State College Area School District, Pennsylvania</td>\n",
       "      <td>NDA</td>\n",
       "      <td>STATE COLLEGE AREA SCHOOL DISTRICT</td>\n",
       "      <td>PENNSYLVANIA</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>Carson City School District, Nevada</td>\n",
       "      <td>NDA</td>\n",
       "      <td>CARSON CITY SCHOOL DISTRICT</td>\n",
       "      <td>NEVADA</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>Gwinnett County Public Schools, Georgia</td>\n",
       "      <td>NDA</td>\n",
       "      <td>GWINNETT COUNTY PUBLIC SCHOOLS</td>\n",
       "      <td>GEORGIA</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>Tracy Unified School District, California</td>\n",
       "      <td>NDA</td>\n",
       "      <td>TRACY UNIFIED SCHOOL DISTRICT</td>\n",
       "      <td>CALIFORNIA</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>Pasco County Schools, Florida</td>\n",
       "      <td>NDA</td>\n",
       "      <td>PASCO COUNTY SCHOOLS</td>\n",
       "      <td>FLORIDA</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>5</th>\n",
       "      <td>Georgia Institute of Technology, Georgia</td>\n",
       "      <td>NDA</td>\n",
       "      <td>GEORGIA INSTITUTE OF TECHNOLOGY</td>\n",
       "      <td>GEORGIA</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>6</th>\n",
       "      <td>Harmony Public Schools, Texas</td>\n",
       "      <td>NDA</td>\n",
       "      <td>HARMONY PUBLIC SCHOOLS</td>\n",
       "      <td>TEXAS</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>7</th>\n",
       "      <td>Modesto City Schools, California</td>\n",
       "      <td>NDA</td>\n",
       "      <td>MODESTO CITY SCHOOLS</td>\n",
       "      <td>CALIFORNIA</td>\n",
       "      <td>NCES_District-0601330</td>\n",
       "      <td>426 Locust St.</td>\n",
       "      <td>Modesto</td>\n",
       "      <td>http://www.monet.k12.ca.us</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>8</th>\n",
       "      <td>Stanislaus County Office of Education, California</td>\n",
       "      <td>NDA</td>\n",
       "      <td>STANISLAUS COUNTY OFFICE OF EDUCATION</td>\n",
       "      <td>CALIFORNIA</td>\n",
       "      <td>NCES_District-0691041</td>\n",
       "      <td>1100 H St.</td>\n",
       "      <td>Modesto</td>\n",
       "      <td>http://www.stancoe.org</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>9</th>\n",
       "      <td>Hawaii State Department of Education, Hawaii</td>\n",
       "      <td>NDA</td>\n",
       "      <td>HAWAII STATE DEPARTMENT OF EDUCATION</td>\n",
       "      <td>HAWAII</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "                                              Agency Total spend (approx)  \\\n",
       "0   State College Area School District, Pennsylvania                  NDA   \n",
       "1                Carson City School District, Nevada                  NDA   \n",
       "2            Gwinnett County Public Schools, Georgia                  NDA   \n",
       "3          Tracy Unified School District, California                  NDA   \n",
       "4                      Pasco County Schools, Florida                  NDA   \n",
       "5           Georgia Institute of Technology, Georgia                  NDA   \n",
       "6                      Harmony Public Schools, Texas                  NDA   \n",
       "7                   Modesto City Schools, California                  NDA   \n",
       "8  Stanislaus County Office of Education, California                  NDA   \n",
       "9       Hawaii State Department of Education, Hawaii                  NDA   \n",
       "\n",
       "                           DISTRICT_NAME     STATENAME       NATIONAL DATA ID  \\\n",
       "0     STATE COLLEGE AREA SCHOOL DISTRICT  PENNSYLVANIA                    NaN   \n",
       "1            CARSON CITY SCHOOL DISTRICT        NEVADA                    NaN   \n",
       "2         GWINNETT COUNTY PUBLIC SCHOOLS       GEORGIA                    NaN   \n",
       "3          TRACY UNIFIED SCHOOL DISTRICT    CALIFORNIA                    NaN   \n",
       "4                   PASCO COUNTY SCHOOLS       FLORIDA                    NaN   \n",
       "5        GEORGIA INSTITUTE OF TECHNOLOGY       GEORGIA                    NaN   \n",
       "6                 HARMONY PUBLIC SCHOOLS         TEXAS                    NaN   \n",
       "7                   MODESTO CITY SCHOOLS    CALIFORNIA  NCES_District-0601330   \n",
       "8  STANISLAUS COUNTY OFFICE OF EDUCATION    CALIFORNIA  NCES_District-0691041   \n",
       "9   HAWAII STATE DEPARTMENT OF EDUCATION        HAWAII                    NaN   \n",
       "\n",
       "   STREET_ADDRESS     CITY              COMPANY_DOMAIN  \n",
       "0             NaN      NaN                         NaN  \n",
       "1             NaN      NaN                         NaN  \n",
       "2             NaN      NaN                         NaN  \n",
       "3             NaN      NaN                         NaN  \n",
       "4             NaN      NaN                         NaN  \n",
       "5             NaN      NaN                         NaN  \n",
       "6             NaN      NaN                         NaN  \n",
       "7  426 Locust St.  Modesto  http://www.monet.k12.ca.us  \n",
       "8      1100 H St.  Modesto      http://www.stancoe.org  \n",
       "9             NaN      NaN                         NaN  "
      ]
     },
     "execution_count": 20,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#merging dataframes\n",
    "merged_data_microbit_nces = pd.merge(data_microbit, data_nces, \n",
    "                     left_on=['DISTRICT_NAME', 'STATENAME'], \n",
    "                     right_on=['DISTRICT_NAME', 'STATENAME'],\n",
    "                     how='left') \n",
    "\n",
    "merged_data_microbit_nces.head(10)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 42,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Number of total districts in the merged dataframe: 1897\n"
     ]
    }
   ],
   "source": [
    "total_districts = len(merged_data_microbit_nces)\n",
    "print(f\"Number of total districts in the merged dataframe: {total_districts}\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 22,
   "metadata": {},
   "outputs": [],
   "source": [
    "#sampling dataframes with matched and non-matched school districts\n",
    "nationaldataid_present = merged_data_microbit_nces[merged_data_microbit_nces['NATIONAL DATA ID'].notna()]\n",
    "nationaldataid_missing = merged_data_microbit_nces[merged_data_microbit_nces['NATIONAL DATA ID'].isna()]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 23,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "418"
      ]
     },
     "execution_count": 23,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "nationaldataid_present.shape[0]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 24,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "1479"
      ]
     },
     "execution_count": 24,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "nationaldataid_missing.shape[0]"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Record linkage / fuzzy matching\n",
    "\n",
    "For fuzzy matching - Skills based on course \"Cleaning data with pandas\" in DataCamp."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 25,
   "metadata": {},
   "outputs": [],
   "source": [
    "nationaldataid_present.to_csv('nationaldataid_present.csv', index=False)\n",
    "nationaldataid_missing.to_csv('nationaldataid_missing.csv', index=False)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 26,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Total number of matches: 1476\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "C:\\Users\\zalis\\AppData\\Local\\Temp\\ipykernel_15152\\1396188897.py:46: SettingWithCopyWarning: \n",
      "A value is trying to be set on a copy of a slice from a DataFrame.\n",
      "Try using .loc[row_indexer,col_indexer] = value instead\n",
      "\n",
      "See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy\n",
      "  nationaldataid_missing['NATIONAL DATA ID'] = matched_ids\n",
      "C:\\Users\\zalis\\AppData\\Local\\Temp\\ipykernel_15152\\1396188897.py:47: SettingWithCopyWarning: \n",
      "A value is trying to be set on a copy of a slice from a DataFrame.\n",
      "Try using .loc[row_indexer,col_indexer] = value instead\n",
      "\n",
      "See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy\n",
      "  nationaldataid_missing['matched_name'] = names\n",
      "C:\\Users\\zalis\\AppData\\Local\\Temp\\ipykernel_15152\\1396188897.py:48: SettingWithCopyWarning: \n",
      "A value is trying to be set on a copy of a slice from a DataFrame.\n",
      "Try using .loc[row_indexer,col_indexer] = value instead\n",
      "\n",
      "See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy\n",
      "  nationaldataid_missing['STREET_ADDRESS'] = street\n",
      "C:\\Users\\zalis\\AppData\\Local\\Temp\\ipykernel_15152\\1396188897.py:49: SettingWithCopyWarning: \n",
      "A value is trying to be set on a copy of a slice from a DataFrame.\n",
      "Try using .loc[row_indexer,col_indexer] = value instead\n",
      "\n",
      "See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy\n",
      "  nationaldataid_missing['CITY'] = city\n",
      "C:\\Users\\zalis\\AppData\\Local\\Temp\\ipykernel_15152\\1396188897.py:50: SettingWithCopyWarning: \n",
      "A value is trying to be set on a copy of a slice from a DataFrame.\n",
      "Try using .loc[row_indexer,col_indexer] = value instead\n",
      "\n",
      "See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy\n",
      "  nationaldataid_missing['COMPANY_DOMAIN'] = domain\n"
     ]
    }
   ],
   "source": [
    "# Fuzzy matching code\n",
    "\n",
    "# Initialize needed lists to store results\n",
    "match_count = 0\n",
    "matched_ids = []\n",
    "street = []\n",
    "city = []\n",
    "domain = []\n",
    "names = []\n",
    "\n",
    "# Iterate through each row in nationaldataid_missing\n",
    "for index, row in nationaldataid_missing.iterrows():\n",
    "    district_to_match = row['DISTRICT_NAME']\n",
    "    state = row['STATENAME']\n",
    "    \n",
    "    # Convert values to strings \n",
    "    district_to_match = str(district_to_match) if not pd.isna(district_to_match) else 'Unknown'\n",
    "    state = str(state) if not pd.isna(state) else 'Unknown'\n",
    "    \n",
    "    #print(f\"Matching for: {district_to_match}: {state}\")\n",
    "\n",
    "    data_nces['Match_Score'] = data_nces['DISTRICT_NAME'].apply(lambda x: fuzz.WRatio(district_to_match, str(x)))\n",
    "    \n",
    "    # Filter rows by state and sort by match score\n",
    "    best_match_row = data_nces[(data_nces['STATENAME'] == state)].sort_values('Match_Score', ascending=False).reset_index()\n",
    "    \n",
    "    # How we check if we have a matching row\n",
    "    if not best_match_row.empty:\n",
    "        best_match_name = best_match_row.loc[0, 'DISTRICT_NAME']\n",
    "        national_id = best_match_row.loc[0, 'NATIONAL DATA ID']\n",
    "        match_street = best_match_row.loc[0, 'STREET_ADDRESS']\n",
    "        match_city = best_match_row.loc[0, 'CITY']\n",
    "        matched_domain = best_match_row.loc[0, 'COMPANY_DOMAIN']\n",
    "        #print(f\"Best match: {best_match_name} with National ID: {national_id}\")\n",
    "        matched_ids.append(national_id)\n",
    "        street.append(match_street)\n",
    "        city.append(match_city)\n",
    "        domain.append(matched_domain)\n",
    "        names.append(best_match_name)\n",
    "        match_count += 1  # Increment match count\n",
    "    else:\n",
    "        #print(\"No match found.\")\n",
    "        matched_ids.append(None)\n",
    "        names.append(None)\n",
    "        street.append(None)\n",
    "        city.append(None)\n",
    "        domain.append(None)\n",
    "\n",
    "# Add the matched National IDs to nationaldataid_missing data_frame\n",
    "nationaldataid_missing['NATIONAL DATA ID'] = matched_ids\n",
    "nationaldataid_missing['matched_name'] = names\n",
    "nationaldataid_missing['STREET_ADDRESS'] = street\n",
    "nationaldataid_missing['CITY'] = city\n",
    "nationaldataid_missing['COMPANY_DOMAIN'] = domain\n",
    "\n",
    "print(f\"Total number of matches: {match_count}\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 27,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Filter rows by district name and sort by match score to get the best match\n",
    "data_nces['Match_Score'] = data_nces['DISTRICT_NAME'].apply(lambda x: fuzz.WRatio('TRACY UNIFIED SCHOOL DISTRICT', str(x)))\n",
    "best_match_row = data_nces[(data_nces['STATENAME'] == state)].sort_values('Match_Score', ascending=False).reset_index()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 28,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "'STOCKTON COLLEGIATE INTERNATIONAL SECONDARY DISTRICT'"
      ]
     },
     "execution_count": 28,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Check how does the funcrion works\n",
    "best_match_row.loc[0,'DISTRICT_NAME']"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 30,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>Agency</th>\n",
       "      <th>Total spend (approx)</th>\n",
       "      <th>DISTRICT_NAME</th>\n",
       "      <th>STATENAME</th>\n",
       "      <th>NATIONAL DATA ID</th>\n",
       "      <th>STREET_ADDRESS</th>\n",
       "      <th>CITY</th>\n",
       "      <th>COMPANY_DOMAIN</th>\n",
       "      <th>matched_name</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>463</th>\n",
       "      <td>Central Union High School District, Imperial C...</td>\n",
       "      <td>NDA</td>\n",
       "      <td>CENTRAL UNION HIGH SCHOOL DISTRICT</td>\n",
       "      <td>IMPERIAL COUNTY, CALIFORNIA</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>502</th>\n",
       "      <td>Greenfield Union School District, Monterey Cou...</td>\n",
       "      <td>NDA</td>\n",
       "      <td>GREENFIELD UNION SCHOOL DISTRICT</td>\n",
       "      <td>MONTEREY COUNTY, CALIFORNIA</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>524</th>\n",
       "      <td>Mountain View School District, San Bernardino ...</td>\n",
       "      <td>NDA</td>\n",
       "      <td>MOUNTAIN VIEW SCHOOL DISTRICT</td>\n",
       "      <td>SAN BERNARDINO COUNTY, CALIFORNIA</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "      <td>None</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "                                                Agency Total spend (approx)  \\\n",
       "463  Central Union High School District, Imperial C...                  NDA   \n",
       "502  Greenfield Union School District, Monterey Cou...                  NDA   \n",
       "524  Mountain View School District, San Bernardino ...                  NDA   \n",
       "\n",
       "                          DISTRICT_NAME                          STATENAME  \\\n",
       "463  CENTRAL UNION HIGH SCHOOL DISTRICT        IMPERIAL COUNTY, CALIFORNIA   \n",
       "502    GREENFIELD UNION SCHOOL DISTRICT        MONTEREY COUNTY, CALIFORNIA   \n",
       "524       MOUNTAIN VIEW SCHOOL DISTRICT  SAN BERNARDINO COUNTY, CALIFORNIA   \n",
       "\n",
       "    NATIONAL DATA ID STREET_ADDRESS  CITY COMPANY_DOMAIN matched_name  \n",
       "463             None           None  None           None         None  \n",
       "502             None           None  None           None         None  \n",
       "524             None           None  None           None         None  "
      ]
     },
     "execution_count": 30,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# There's 3 states that had commas in their statename and were incorrectly matched\n",
    "# Keep them for manual correction in the final csv.file before doing import sorted and organised data into the Strawbees Hubspot CRM DataBase.\n",
    "\n",
    "nationaldataid_missing[nationaldataid_missing['NATIONAL DATA ID'].isna()]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 31,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Total number of matches: 1476\n"
     ]
    }
   ],
   "source": [
    "# Check the number of all metched districts\n",
    "print(f\"Total number of matches: {match_count}\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Joining data frames nationaldataid_missing and nationaldataid_present\n",
    "\n",
    "Part 5: Merging resulting dataframes for final version of school districts with all needed data ready for the import in Strawbees Hubspot CRM Database."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 32,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Preparing final dataset for the import micro:bit data:\n",
    "# 1. Combining both datasets\n",
    "# 2. Adding \"use_microbit\" column with value \"Yes\"\n",
    "# 3. Exporting to spreadsheet for manual correction"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 43,
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "C:\\Users\\zalis\\AppData\\Local\\Temp\\ipykernel_15152\\3650164116.py:1: SettingWithCopyWarning: \n",
      "A value is trying to be set on a copy of a slice from a DataFrame\n",
      "\n",
      "See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy\n",
      "  nationaldataid_missing.drop(labels=[\"matched_name\"], axis=1, inplace=True)\n"
     ]
    },
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>Agency</th>\n",
       "      <th>Total spend (approx)</th>\n",
       "      <th>DISTRICT_NAME</th>\n",
       "      <th>STATENAME</th>\n",
       "      <th>NATIONAL DATA ID</th>\n",
       "      <th>STREET_ADDRESS</th>\n",
       "      <th>CITY</th>\n",
       "      <th>COMPANY_DOMAIN</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>State College Area School District, Pennsylvania</td>\n",
       "      <td>NDA</td>\n",
       "      <td>STATE COLLEGE AREA SCHOOL DISTRICT</td>\n",
       "      <td>PENNSYLVANIA</td>\n",
       "      <td>NCES_District-4222770</td>\n",
       "      <td>240 Villa Crest Dr</td>\n",
       "      <td>State College</td>\n",
       "      <td>http://www.scasd.org</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>Carson City School District, Nevada</td>\n",
       "      <td>NDA</td>\n",
       "      <td>CARSON CITY SCHOOL DISTRICT</td>\n",
       "      <td>NEVADA</td>\n",
       "      <td>NCES_District-3200390</td>\n",
       "      <td>PO Box 603</td>\n",
       "      <td>Carson City</td>\n",
       "      <td>http://carsoncityschools.com</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "                                             Agency Total spend (approx)  \\\n",
       "0  State College Area School District, Pennsylvania                  NDA   \n",
       "1               Carson City School District, Nevada                  NDA   \n",
       "\n",
       "                        DISTRICT_NAME     STATENAME       NATIONAL DATA ID  \\\n",
       "0  STATE COLLEGE AREA SCHOOL DISTRICT  PENNSYLVANIA  NCES_District-4222770   \n",
       "1         CARSON CITY SCHOOL DISTRICT        NEVADA  NCES_District-3200390   \n",
       "\n",
       "       STREET_ADDRESS           CITY                COMPANY_DOMAIN  \n",
       "0  240 Villa Crest Dr  State College          http://www.scasd.org  \n",
       "1          PO Box 603    Carson City  http://carsoncityschools.com  "
      ]
     },
     "execution_count": 43,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Delete no needed columns\n",
    "nationaldataid_missing.drop(labels=[\"matched_name\"], axis=1, inplace=True)\n",
    "nationaldataid_missing.head(2)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 34,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "1479"
      ]
     },
     "execution_count": 34,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Show total number of values after fuzzy matching\n",
    "nationaldataid_missing.shape[0]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 35,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>Agency</th>\n",
       "      <th>Total spend (approx)</th>\n",
       "      <th>DISTRICT_NAME</th>\n",
       "      <th>STATENAME</th>\n",
       "      <th>NATIONAL DATA ID</th>\n",
       "      <th>STREET_ADDRESS</th>\n",
       "      <th>CITY</th>\n",
       "      <th>COMPANY_DOMAIN</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>7</th>\n",
       "      <td>Modesto City Schools, California</td>\n",
       "      <td>NDA</td>\n",
       "      <td>MODESTO CITY SCHOOLS</td>\n",
       "      <td>CALIFORNIA</td>\n",
       "      <td>NCES_District-0601330</td>\n",
       "      <td>426 Locust St.</td>\n",
       "      <td>Modesto</td>\n",
       "      <td>http://www.monet.k12.ca.us</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>8</th>\n",
       "      <td>Stanislaus County Office of Education, California</td>\n",
       "      <td>NDA</td>\n",
       "      <td>STANISLAUS COUNTY OFFICE OF EDUCATION</td>\n",
       "      <td>CALIFORNIA</td>\n",
       "      <td>NCES_District-0691041</td>\n",
       "      <td>1100 H St.</td>\n",
       "      <td>Modesto</td>\n",
       "      <td>http://www.stancoe.org</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>10</th>\n",
       "      <td>San Antonio ISD, Texas</td>\n",
       "      <td>NDA</td>\n",
       "      <td>SAN ANTONIO ISD</td>\n",
       "      <td>TEXAS</td>\n",
       "      <td>NCES_District-4838730</td>\n",
       "      <td>514 QUINCY ST</td>\n",
       "      <td>SAN ANTONIO</td>\n",
       "      <td>http://www.saisd.net/</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>11</th>\n",
       "      <td>Montgomery County Public Schools, Maryland</td>\n",
       "      <td>NDA</td>\n",
       "      <td>MONTGOMERY COUNTY PUBLIC SCHOOLS</td>\n",
       "      <td>MARYLAND</td>\n",
       "      <td>NCES_District-2400480</td>\n",
       "      <td>850 Hungerford Drive</td>\n",
       "      <td>Rockville</td>\n",
       "      <td>http://www.montgomeryschoolsmd.org</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>23</th>\n",
       "      <td>Cabarrus County Schools, North Carolina</td>\n",
       "      <td>NDA</td>\n",
       "      <td>CABARRUS COUNTY SCHOOLS</td>\n",
       "      <td>NORTH CAROLINA</td>\n",
       "      <td>NCES_District-3700530</td>\n",
       "      <td>PO Box 388</td>\n",
       "      <td>Concord</td>\n",
       "      <td>http://www.cabarrus.k12.nc.us/</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "                                               Agency Total spend (approx)  \\\n",
       "7                    Modesto City Schools, California                  NDA   \n",
       "8   Stanislaus County Office of Education, California                  NDA   \n",
       "10                             San Antonio ISD, Texas                  NDA   \n",
       "11         Montgomery County Public Schools, Maryland                  NDA   \n",
       "23            Cabarrus County Schools, North Carolina                  NDA   \n",
       "\n",
       "                            DISTRICT_NAME       STATENAME  \\\n",
       "7                    MODESTO CITY SCHOOLS      CALIFORNIA   \n",
       "8   STANISLAUS COUNTY OFFICE OF EDUCATION      CALIFORNIA   \n",
       "10                        SAN ANTONIO ISD           TEXAS   \n",
       "11       MONTGOMERY COUNTY PUBLIC SCHOOLS        MARYLAND   \n",
       "23                CABARRUS COUNTY SCHOOLS  NORTH CAROLINA   \n",
       "\n",
       "         NATIONAL DATA ID        STREET_ADDRESS         CITY  \\\n",
       "7   NCES_District-0601330        426 Locust St.      Modesto   \n",
       "8   NCES_District-0691041            1100 H St.      Modesto   \n",
       "10  NCES_District-4838730         514 QUINCY ST  SAN ANTONIO   \n",
       "11  NCES_District-2400480  850 Hungerford Drive    Rockville   \n",
       "23  NCES_District-3700530            PO Box 388      Concord   \n",
       "\n",
       "                        COMPANY_DOMAIN  \n",
       "7           http://www.monet.k12.ca.us  \n",
       "8               http://www.stancoe.org  \n",
       "10               http://www.saisd.net/  \n",
       "11  http://www.montgomeryschoolsmd.org  \n",
       "23      http://www.cabarrus.k12.nc.us/  "
      ]
     },
     "execution_count": 35,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "nationaldataid_present.head()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 36,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "418"
      ]
     },
     "execution_count": 36,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Show total number of values after merging original dataframes\n",
    "nationaldataid_present.shape[0]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 37,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>Agency</th>\n",
       "      <th>Total spend (approx)</th>\n",
       "      <th>DISTRICT_NAME</th>\n",
       "      <th>STATENAME</th>\n",
       "      <th>NATIONAL DATA ID</th>\n",
       "      <th>STREET_ADDRESS</th>\n",
       "      <th>CITY</th>\n",
       "      <th>COMPANY_DOMAIN</th>\n",
       "      <th>matched_name</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>State College Area School District, Pennsylvania</td>\n",
       "      <td>NDA</td>\n",
       "      <td>STATE COLLEGE AREA SCHOOL DISTRICT</td>\n",
       "      <td>PENNSYLVANIA</td>\n",
       "      <td>NCES_District-4222770</td>\n",
       "      <td>240 Villa Crest Dr</td>\n",
       "      <td>State College</td>\n",
       "      <td>http://www.scasd.org</td>\n",
       "      <td>STATE COLLEGE AREA SD</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>Carson City School District, Nevada</td>\n",
       "      <td>NDA</td>\n",
       "      <td>CARSON CITY SCHOOL DISTRICT</td>\n",
       "      <td>NEVADA</td>\n",
       "      <td>NCES_District-3200390</td>\n",
       "      <td>PO Box 603</td>\n",
       "      <td>Carson City</td>\n",
       "      <td>http://carsoncityschools.com</td>\n",
       "      <td>CARSON CITY</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>Gwinnett County Public Schools, Georgia</td>\n",
       "      <td>NDA</td>\n",
       "      <td>GWINNETT COUNTY PUBLIC SCHOOLS</td>\n",
       "      <td>GEORGIA</td>\n",
       "      <td>NCES_District-1302550</td>\n",
       "      <td>52 Gwinnett Dr</td>\n",
       "      <td>Lawrenceville</td>\n",
       "      <td>NaN</td>\n",
       "      <td>GWINNETT COUNTY</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>Tracy Unified School District, California</td>\n",
       "      <td>NDA</td>\n",
       "      <td>TRACY UNIFIED SCHOOL DISTRICT</td>\n",
       "      <td>CALIFORNIA</td>\n",
       "      <td>NCES_District-0601792</td>\n",
       "      <td>PO Box 2286</td>\n",
       "      <td>Stockton</td>\n",
       "      <td>http://www.stocktoncollegiate.org</td>\n",
       "      <td>STOCKTON COLLEGIATE INTERNATIONAL SECONDARY DI...</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>Pasco County Schools, Florida</td>\n",
       "      <td>NDA</td>\n",
       "      <td>PASCO COUNTY SCHOOLS</td>\n",
       "      <td>FLORIDA</td>\n",
       "      <td>NCES_District-1201530</td>\n",
       "      <td>7227 LAND O LAKES BLVD</td>\n",
       "      <td>LAND O LAKES</td>\n",
       "      <td>http://WWW.PASCO.K12.FL.US</td>\n",
       "      <td>PASCO</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "                                             Agency Total spend (approx)  \\\n",
       "0  State College Area School District, Pennsylvania                  NDA   \n",
       "1               Carson City School District, Nevada                  NDA   \n",
       "2           Gwinnett County Public Schools, Georgia                  NDA   \n",
       "3         Tracy Unified School District, California                  NDA   \n",
       "4                     Pasco County Schools, Florida                  NDA   \n",
       "\n",
       "                        DISTRICT_NAME     STATENAME       NATIONAL DATA ID  \\\n",
       "0  STATE COLLEGE AREA SCHOOL DISTRICT  PENNSYLVANIA  NCES_District-4222770   \n",
       "1         CARSON CITY SCHOOL DISTRICT        NEVADA  NCES_District-3200390   \n",
       "2      GWINNETT COUNTY PUBLIC SCHOOLS       GEORGIA  NCES_District-1302550   \n",
       "3       TRACY UNIFIED SCHOOL DISTRICT    CALIFORNIA  NCES_District-0601792   \n",
       "4                PASCO COUNTY SCHOOLS       FLORIDA  NCES_District-1201530   \n",
       "\n",
       "           STREET_ADDRESS           CITY                     COMPANY_DOMAIN  \\\n",
       "0      240 Villa Crest Dr  State College               http://www.scasd.org   \n",
       "1              PO Box 603    Carson City       http://carsoncityschools.com   \n",
       "2          52 Gwinnett Dr  Lawrenceville                                NaN   \n",
       "3             PO Box 2286       Stockton  http://www.stocktoncollegiate.org   \n",
       "4  7227 LAND O LAKES BLVD   LAND O LAKES         http://WWW.PASCO.K12.FL.US   \n",
       "\n",
       "                                        matched_name  \n",
       "0                              STATE COLLEGE AREA SD  \n",
       "1                                        CARSON CITY  \n",
       "2                                    GWINNETT COUNTY  \n",
       "3  STOCKTON COLLEGIATE INTERNATIONAL SECONDARY DI...  \n",
       "4                                              PASCO  "
      ]
     },
     "execution_count": 37,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Combining both datasets after data exploring, manipulation and merging\n",
    "microbitdata_for_import = pd.concat([nationaldataid_missing, nationaldataid_present], ignore_index=True)\n",
    "microbitdata_for_import.head()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 38,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Number of total districts in the merged dataframe: 1897\n"
     ]
    }
   ],
   "source": [
    "# Show total number of districts \n",
    "total_districts = len(microbitdata_for_import)\n",
    "print(f\"Number of total districts in the merged dataframe: {total_districts}\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Adding coulmn \"use_microbit\" (similar with the Property name in Hubspot SRM Strawbees DataBase)."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 39,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>Agency</th>\n",
       "      <th>Total spend (approx)</th>\n",
       "      <th>DISTRICT_NAME</th>\n",
       "      <th>STATENAME</th>\n",
       "      <th>NATIONAL DATA ID</th>\n",
       "      <th>STREET_ADDRESS</th>\n",
       "      <th>CITY</th>\n",
       "      <th>COMPANY_DOMAIN</th>\n",
       "      <th>matched_name</th>\n",
       "      <th>use_microbit</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>State College Area School District, Pennsylvania</td>\n",
       "      <td>NDA</td>\n",
       "      <td>STATE COLLEGE AREA SCHOOL DISTRICT</td>\n",
       "      <td>PENNSYLVANIA</td>\n",
       "      <td>NCES_District-4222770</td>\n",
       "      <td>240 Villa Crest Dr</td>\n",
       "      <td>State College</td>\n",
       "      <td>http://www.scasd.org</td>\n",
       "      <td>STATE COLLEGE AREA SD</td>\n",
       "      <td>Yes</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>Carson City School District, Nevada</td>\n",
       "      <td>NDA</td>\n",
       "      <td>CARSON CITY SCHOOL DISTRICT</td>\n",
       "      <td>NEVADA</td>\n",
       "      <td>NCES_District-3200390</td>\n",
       "      <td>PO Box 603</td>\n",
       "      <td>Carson City</td>\n",
       "      <td>http://carsoncityschools.com</td>\n",
       "      <td>CARSON CITY</td>\n",
       "      <td>Yes</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>Gwinnett County Public Schools, Georgia</td>\n",
       "      <td>NDA</td>\n",
       "      <td>GWINNETT COUNTY PUBLIC SCHOOLS</td>\n",
       "      <td>GEORGIA</td>\n",
       "      <td>NCES_District-1302550</td>\n",
       "      <td>52 Gwinnett Dr</td>\n",
       "      <td>Lawrenceville</td>\n",
       "      <td>NaN</td>\n",
       "      <td>GWINNETT COUNTY</td>\n",
       "      <td>Yes</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>Tracy Unified School District, California</td>\n",
       "      <td>NDA</td>\n",
       "      <td>TRACY UNIFIED SCHOOL DISTRICT</td>\n",
       "      <td>CALIFORNIA</td>\n",
       "      <td>NCES_District-0601792</td>\n",
       "      <td>PO Box 2286</td>\n",
       "      <td>Stockton</td>\n",
       "      <td>http://www.stocktoncollegiate.org</td>\n",
       "      <td>STOCKTON COLLEGIATE INTERNATIONAL SECONDARY DI...</td>\n",
       "      <td>Yes</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>Pasco County Schools, Florida</td>\n",
       "      <td>NDA</td>\n",
       "      <td>PASCO COUNTY SCHOOLS</td>\n",
       "      <td>FLORIDA</td>\n",
       "      <td>NCES_District-1201530</td>\n",
       "      <td>7227 LAND O LAKES BLVD</td>\n",
       "      <td>LAND O LAKES</td>\n",
       "      <td>http://WWW.PASCO.K12.FL.US</td>\n",
       "      <td>PASCO</td>\n",
       "      <td>Yes</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "                                             Agency Total spend (approx)  \\\n",
       "0  State College Area School District, Pennsylvania                  NDA   \n",
       "1               Carson City School District, Nevada                  NDA   \n",
       "2           Gwinnett County Public Schools, Georgia                  NDA   \n",
       "3         Tracy Unified School District, California                  NDA   \n",
       "4                     Pasco County Schools, Florida                  NDA   \n",
       "\n",
       "                        DISTRICT_NAME     STATENAME       NATIONAL DATA ID  \\\n",
       "0  STATE COLLEGE AREA SCHOOL DISTRICT  PENNSYLVANIA  NCES_District-4222770   \n",
       "1         CARSON CITY SCHOOL DISTRICT        NEVADA  NCES_District-3200390   \n",
       "2      GWINNETT COUNTY PUBLIC SCHOOLS       GEORGIA  NCES_District-1302550   \n",
       "3       TRACY UNIFIED SCHOOL DISTRICT    CALIFORNIA  NCES_District-0601792   \n",
       "4                PASCO COUNTY SCHOOLS       FLORIDA  NCES_District-1201530   \n",
       "\n",
       "           STREET_ADDRESS           CITY                     COMPANY_DOMAIN  \\\n",
       "0      240 Villa Crest Dr  State College               http://www.scasd.org   \n",
       "1              PO Box 603    Carson City       http://carsoncityschools.com   \n",
       "2          52 Gwinnett Dr  Lawrenceville                                NaN   \n",
       "3             PO Box 2286       Stockton  http://www.stocktoncollegiate.org   \n",
       "4  7227 LAND O LAKES BLVD   LAND O LAKES         http://WWW.PASCO.K12.FL.US   \n",
       "\n",
       "                                        matched_name use_microbit  \n",
       "0                              STATE COLLEGE AREA SD          Yes  \n",
       "1                                        CARSON CITY          Yes  \n",
       "2                                    GWINNETT COUNTY          Yes  \n",
       "3  STOCKTON COLLEGIATE INTERNATIONAL SECONDARY DI...          Yes  \n",
       "4                                              PASCO          Yes  "
      ]
     },
     "execution_count": 39,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Adding \"use_microbit\" column with value \"Yes\" \n",
    "microbitdata_for_import[\"use_microbit\"] = \"Yes\"\n",
    "microbitdata_for_import.head()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 40,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Exporting final resulting dataframe to spreadsheet for manual correction\n",
    "microbitdata_for_import.to_csv('microbitdata_for_import.csv', index=False)\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.10.6"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
