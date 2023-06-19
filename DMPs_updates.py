# This script is intended to find out how often DMPs in DMP Online are updated
# It is based on downloaded json files
# Mark Bruyneel
# 2023-05-08
# Script version 1.0

import os
import re
import pandas as pd
import numpy as np
import json
import api_v0_new as api
import api_v1_new as api2
from csv import reader
from loguru import logger
from datetime import datetime, timedelta
from pathlib import Path

# Show all data in screen
pd.set_option("display.max.columns", None)
# Create year variable for filenames to get only files for the current year
# This for when the script is periodically run during the yeartoday = datetime.now()
today = datetime.now()
year = today.strftime("%Y")
runday = str(datetime.today().date())
runyear = str(datetime.today().year)

# Step 1 Read Json files
# Establish location and files with data. Put the filenames in a table
# and add the date in the file name as data for a column

path = 'U:\Werk\Data Management\Python\Files\DMP_Online\dmps'

# Get list of all files only in the given directory
dmplist = lambda x: os.path.isfile(os.path.join(path, x))
files_list = filter(dmplist, os.listdir(path))

# Create a list of files in directory along with the size
size_of_file = [
    (f, os.stat(os.path.join(path, f)).st_size)
    for f in files_list
]

# Create a table with the list as input
dmp_list = pd.DataFrame(size_of_file, columns=['File_name', 'File_size'])
# Add some extra fields
dmp_list['File_Date'] = dmp_list.File_name.str[0:10]
dmp_list['Year'] = dmp_list.File_name.str[0:4]

# Remove files from the list that are empty or close to it
dmp_list = dmp_list[dmp_list.File_size > 1025] # 10 = bytes
# Remove files that are not Json files but csv files
dmp_list = dmp_list[ dmp_list['File_name'].str.contains('.csv')==False ]

# Reset index of the table
dmp_list.reset_index(drop=True, inplace=True)
filenr = dmp_list.shape[0]

# Step 2 Use list to get data from Json files
# Generate folders for the data
Path('DMP_updates').mkdir(parents=True, exist_ok=True)

DMP_data = pd.DataFrame()
i = 0
while i != filenr:
    logger.debug(f'Copying and adding data from ' + dmp_list.File_name[i])
    f = open(f'U:\Werk\Data Management\Python\\Files\DMP_Online\dmps\\{dmp_list.File_name[i]}', 'r')
    # returns JSON object as a dicionary
    data = json.load(f)
    # Iterating through the json list to get specific items
    # First put them in item lists and then create a table by combining them
    id_list = []
    LastUpdate = []
    CreationDate = []
    for l1 in data:
        for l2 in l1:
            id_list.append(str(l2['id']))
            LastUpdate.append(l2['last_updated'])
            CreationDate.append(l2['creation_date'])
    # Closing file
    f.close()
    id_data = {'id': id_list}
    test_list = pd.DataFrame(id_data)
    test_list['last_updated'] = LastUpdate
    test_list['creation_date'] = CreationDate
    DMP_data = pd.concat([DMP_data, test_list], ignore_index=True)
    i = i + 1

# Remove any duplicate data rows. It replaces the original Dataframe because inplace = True
DMP_data.drop_duplicates(keep="first", inplace=True)

# Sorting data based on three fields
DMP_data.sort_values(by=['id', 'creation_date', 'last_updated'])
DMP_data.loc[:, ['id', 'creation_date', 'last_updated']]
print(DMP_data.head())

# Export result as a CSV file
DMP_data.to_csv(f'U:\Werk\Data Management\Python\\Files\DMP_Online\DMP_updates\\fulldmplist_updates_v1.csv', encoding='utf-8', index=False)

# Step 3 Get a list of all DMPS to process and get the metadata through api2
Path('DMP_updates\meta').mkdir(parents=True, exist_ok=True)
filenrfm = DMP_data.shape[0]
filenrf = str(filenrfm)
metalist = DMP_data['id'].values.tolist()
dmps = metalist

# api2 currently has a limit somewhere beyond 150 items
# Step 8 Use metalist to download the meta for DMPS as separate file
if len(dmps) < 151:
    filenrf = str(len(dmps))
    print('Gettings data for ' + filenrf + ' DMPs')
    plans = api2.retrieve_plans(dmps)
    pages = list(plans)
    # save json file of the DMP
    with open(f'DMP_updates\meta\DMPS_metadata0.json', 'w') as f:
        f.write(json.dumps(pages))
else:
    # divide up the full list in brackets of 150
    fmllen = len(dmps)
    nrofml = round(len(dmps) / 150) + 1
    print("Nr. of sublists to generate: ", nrofml)

    def getnums(s, e, i):
        return list(range(s, e, i))

    nroflists = (getnums(0, int(nrofml), 1))

    snrofml = 0
    startmlnr = 0
    while snrofml < fmllen:
        startl = snrofml
        endl = snrofml + 150
        mlistnew = dmps[startl: endl]
        print("Nr. of DMPS in the list it is fetching data for: ", len(mlistnew))
        plans = api2.retrieve_plans(mlistnew)
        pages = list(plans)
        # save json file of the DMP
        with open(f'DMP_updates\meta/DMPS_metadata'+str(startmlnr)+'.json', 'w') as f:
            f.write(json.dumps(pages))
        snrofml = snrofml + 150
        startmlnr = startmlnr + 1

# Step 4: Use Metadata json file to get data for each dmp
# Get list of all metadata files only in the given directory
metapath = 'U:\Werk\Data Management\Python\Files\DMP_Online\DMP_updates\meta'

metadmplist = lambda x: os.path.isfile(os.path.join(metapath, x))
metafiles_list = filter(metadmplist, os.listdir(metapath))

# Create a table with the list as input
metadmp_list = pd.DataFrame(metafiles_list, columns=['File_name'])

metafilenr = metadmp_list.shape[0]

print('Number of Metadata Json files to process: ', metafilenr)

DMP_meta_data = pd.DataFrame()
m = 0
while m != metafilenr:
    logger.debug(f'Copying and adding meta data from ' + metadmp_list.File_name[m])
    nf = open(f'U:\Werk\Data Management\Python\\Files\DMP_Online\DMP_updates\meta/{metadmp_list.File_name[m]}', 'r')
    # returns JSON object as a dictionary
    data_meta = json.load(nf)
    # Iterating through the metadata json file to get specific items for all DMPs
    id_list_md = []
    project_start = []
    project_end = []
    for l1 in data_meta:
        if l1 is not None:
            for l2 in l1:
                id_list_md.append(str(l2['dmp']['dmp_id']['identifier'][40:]))
                project_start.append(l2['dmp']['project'][0]['start'][0:10])
                project_end.append(l2['dmp']['project'][0]['end'][0:10])
    # Closing file
    nf.close()
    id_data_md = {'id': id_list_md}
    test_listmd = pd.DataFrame(id_data_md)
    test_listmd['project_start'] = project_start
    test_listmd['project_end'] = project_end
    DMP_meta_data = pd.concat([DMP_meta_data, test_listmd], ignore_index=True)
    m = m + 1

DMP_meta_data.to_csv(f'U:\Werk\Data Management\Python\\Files\DMP_Online\DMP_updates\dmps_metadata_list_v1.csv', encoding='utf-8')

# Step 5: Combine data and metadata for all DMPs
DMP_changes_list = pd.DataFrame()
DMP_changes_list = pd.merge(DMP_data, DMP_meta_data, on='id')
DMP_changes_list.drop_duplicates(keep="first", inplace=True)

# Step 6: Getting the latest Register list to compare
path = 'U:\Werk\Data Management\Python\Files\DMP_Online\DMP_stats/'+runyear+'\Final'

# Get list of all files only in the given directory
# This needs to be changed later if you want a multi-year register list
registerlist = lambda x: os.path.isfile(os.path.join(path, x))
regfiles_list = filter(registerlist, os.listdir(path))

# Create a list of files in directory along with the size
reg_size_of_file = [
    (f, os.stat(os.path.join(path, f)).st_size)
    for f in regfiles_list
]

# Create a table with the list as input
register_list = pd.DataFrame(reg_size_of_file, columns=['File_name', 'File_size'])
# Add some extra fields in case I ever want to use the list
register_list['Date'] = register_list.File_name.str[13:23]
register_list["Date"] =  pd.to_datetime(register_list["Date"], format="%Y-%m-%d")
# Get the last file/row from the list
register_list_last = register_list.loc[register_list.index[-1]]
# Use this to load the last list for the year
rf = pd.read_csv(f'U:\Werk\Data Management\Python\Files\DMP_Online\DMP_stats\\{runyear}\\Final\\{register_list_last[0]}')

# Add a dummy variable if there is a match with the register list
# First make sure the code that needs to be matched on is a string
DMP_changes_list['id'] = DMP_changes_list['id'].apply(str)
rf['id'] = rf['id'].apply(str)
DMP_changes_list_final = pd.merge(DMP_changes_list, rf, on="id", how='outer')
# Remove unneeded fields and rename column for match Register
DMP_changes_list_final.drop(DMP_changes_list_final.iloc[:, 6:53], axis=1, inplace=True)
DMP_changes_list_final.rename(columns={'Unnamed: 0': 'Register'}, inplace=True)
# Generate a dummy value in the column register for in register or not
DMP_changes_list_final.loc[DMP_changes_list_final['Register'] >= 0, 'Register'] = 1
DMP_changes_list_final.loc[DMP_changes_list_final['Register'] != 1, 'Register'] = 0
# Save end result
DMP_changes_list_final.to_csv(f'U:\Werk\Data Management\Python\\Files\DMP_Online\DMP_updates\DMP_changes_list_v1.csv', encoding='utf-8')
