# This script is intended to create statistics from DMPs in DMP Online
# It is based on downloaded json files
# Mark Bruyneel
# 2024-01-01
# Script version 1.2
#
# The API scripts were created by Max Paulus. https://github.com/paulmaxus
#

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
# This for when the script is periodically run during the year: on the first of the month
# The rundate if scheduled on the first of January is adjusted to account for availability
# of downloaded data.
today = datetime.now()
month = today.month
day = today.day
if month==1 and day==1:
    yearnew = today.year
    runyear = str(yearnew - 1)
    year = runyear
    runday = runyear + '-12-31'
else:
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

# Remove files from the list that are not for the current year
dmp_list = dmp_list[dmp_list.Year == year]
# Remove files from the list that are empty or close to it
dmp_list = dmp_list[dmp_list.File_size > 1025] # 10 = bytes
# Remove files that are not Json files but csv files
dmp_list = dmp_list[ dmp_list['File_name'].str.contains('.csv')==False ]

# Reset index of the table
dmp_list.reset_index(drop=True, inplace=True)

filenr = dmp_list.shape[0]

print('Number of Json files to process: ', filenr)

# Step 2 Use list to get data from Json files
# Generate folders for the data
Path('DMP_stats\\'+runyear).mkdir(parents=True, exist_ok=True)
Path('DMP_stats\\'+runyear+'\cert2').mkdir(parents=True, exist_ok=True)
Path('DMP_stats\\'+runyear+'\gdpr2').mkdir(parents=True, exist_ok=True)

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
    title_list = []
    file_name = []
    templates = []
    funders = []
    LastUpdate = []
    for l1 in data:
        for l2 in l1:
            id_list.append(str(l2['id']))
            title_list.append(l2['title'])
            file_name.append(dmp_list.File_name[i])
            templates.append(l2['template']['title'])
            funders.append(l2['funder']['name'])
            LastUpdate.append(l2['last_updated'])
    # Closing file
    f.close()
    id_data = {'id': id_list}
    test_list = pd.DataFrame(id_data)
    test_list['title'] = title_list
    test_list['template'] = templates
    test_list['funder'] = funders
    test_list['last_updated'] = LastUpdate
    test_list['file_name'] = file_name
    DMP_data = pd.concat([DMP_data, test_list], ignore_index=True)
    i = i + 1
# Export result as a CSV file with the date of the Python run
DMP_data.to_csv(f'U:\Werk\Data Management\Python\\Files\DMP_Online\DMP_stats\\'+runyear+'\\fulldmplist2.csv', encoding='utf-8')

# Step 3. Create seperate lists for the VU templates to download specific dmps
DMP_data_certified = DMP_data[DMP_data['template'] == '1 - VU DMP template 2021 (NWO & ZonMW certified) v1.4'].copy()
DMP_data_certified.to_csv(f'U:\Werk\Data Management\Python\\Files\DMP_Online\DMP_stats\\'+runyear+'\VU_DMP_Cert2_list.csv', encoding='utf-8')
DMP_data_certified = DMP_data_certified.drop(DMP_data_certified.columns[[1, 2, 3, 4, 5]], axis=1)
DMP_data_certified.drop_duplicates()
Cert_dmps_l = DMP_data_certified.values.tolist()
cert_dmps = [item for sublist in Cert_dmps_l for item in sublist]

DMP_data_gdpr = DMP_data[DMP_data['template'] == '2 - VU GDPR registration form for research 2021 v1.1'].copy()
DMP_data_gdpr.to_csv(f'U:\Werk\Data Management\Python\\Files\DMP_Online\DMP_stats\\'+runyear+'\VU_GDPR_forms2_list.csv', encoding='utf-8')
DMP_data_gdpr = DMP_data_gdpr.drop(DMP_data_gdpr.columns[[1, 2, 3, 4, 5]], axis=1)
DMP_data_gdpr = DMP_data_gdpr.drop_duplicates()
gdpr_dmps_l = DMP_data_gdpr.values.tolist()
gdpr_dmps = [item for sublist in gdpr_dmps_l for item in sublist]

# Step 4 Use each list to download the DMPS as separate files in separate folders for the record
print('Number of GDPR forms: ', len(gdpr_dmps))
for item in gdpr_dmps:
    print(item)
    identifier = item
    plans = api.retrieve_plans(identifier)
    pages = list(plans)
    # save json file of the DMP
    with open(f'DMP_stats\{runyear}\gdpr2\{identifier}.json', 'w') as f:
        f.write(json.dumps(pages))

print('Number of VU DMP forms: ', len(cert_dmps))
for item2 in cert_dmps:
    print(item2)
    identifier = item2
    plans = api.retrieve_plans(identifier)
    pages = list(plans)
    # save json file of the DMP
    with open(f'DMP_stats\{runyear}\cert2\{identifier}.json', 'w') as f:
        f.write(json.dumps(pages))

# I have created code to process specific fields in different ways for both forms.
# This applies to the code for both the GDPR form as well as the VU main template
# - Regular text answer fields are cleaned of html code as well as odd characters
# - Questions with multi-options that can be marked are aggregated into a single
#   variable and afterwards it is possible to create variables to separate them again.

# Step 5 Get the data from the forms for the overview
# Generate path for the AVG Register overview
Path('DMP_stats\\'+runyear+'\overview2').mkdir(parents=True, exist_ok=True)

# Function to strip html code from text answer fields
def remove_html(text):
    tags = False
    quote = False
    output = ""

    for ch in text:
        if ch == '<' and not quote:
            tag = True
        elif ch == '>' and not quote:
            tag = False
        elif (ch == '"' or ch == "'") and tag:
            quote = not quote
        elif not tag:
            output = output + ch
    return output

# List of characters combinations to clean or replace in text fields
tags_sp = ['&ndash', '&nbsp', '&amp']
tags_nv = [';']

# Step 5a Start getting the data from the GDPR form json files
pathg = 'U:\Werk\Data Management\Python\Files\DMP_Online\DMP_stats\\'+runyear+'\gdpr2'

# Get list of all gdpr form files only in the given directory
dmplistgdpr = lambda x: os.path.isfile(os.path.join(pathg, x))
files_list_gdpr = filter(dmplistgdpr, os.listdir(pathg))

# Create a list of files in directory along with the size
size_of_file_g = [
    (f, os.stat(os.path.join(pathg, f)).st_size)
    for f in files_list_gdpr
]

# Create a table with the list as input
dmp_list_gdpr = pd.DataFrame(size_of_file_g, columns=['File_name', 'File_size'])
filenrg = dmp_list_gdpr.shape[0]

DMP_gdpr_data = pd.DataFrame()
i = 0
while i != filenrg:
    logger.debug(f'Collecting data from GDPR file: ' + dmp_list_gdpr.File_name[i])
    g = open(f'U:\Werk\Data Management\Python\Files\DMP_Online\DMP_stats\\{runyear}\gdpr2\\{dmp_list_gdpr.File_name[i]}', 'r')
    # returns JSON object as a dictionary
    datag = json.load(g)
    # Iterating through the json list to get specific items
    # First put them in item lists and then create a table by combining them
    id_list_g = []
    title_list_g = []
    file_name_g = []
    templates_g = []
    funders_g = []
    LastUpdate_g = []
    data_contact_ng = []
    data_contact_eg = []

    for l1 in datag:
        for l2 in l1:
            id_list_g.append(str(l2['id']))
            title_list_g.append(l2['title'])
            file_name_g.append(dmp_list_gdpr.File_name[i])
            templates_g.append(l2['template']['title'])
            funders_g.append(l2['funder']['name'])
            LastUpdate_g.append(l2['last_updated'])
            data_contact_ng.append(l2['data_contact']['name'])
            data_contact_eg.append(l2['data_contact']['email'])

            # Add exception to take into account that the answer has not been given by the author
            # which means that the answer variable to a question is possibly not in the dictionary
            Dictionaryq = l2['plan_content'][0]['sections'][0]['questions'][0]
            dictlen = len(Dictionaryq)
            Plan_version_g = []
            a = 7
            try:
                if dictlen < a:
                    answerv = 'Unknown'
                else:
                    text = Dictionaryq['answer']['text']
                    # Strip html code from answer
                    answervt = remove_html(text)
                    # Combine multiline input into a single line
                    answerv = "_-_".join(line.strip() for line in answervt.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerv:
                            answerv = answerv.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerv:
                            answerv = answerv.replace(tag, '.')
            except:
                answerv = 'Unknown'
                pass
            Plan_version_g.append(answerv)

            Dictionarypt = l2['plan_content'][0]['sections'][0]['questions'][1]
            dictlenpt = len(Dictionarypt)
            Project_title = []
            a = 7
            try:
                if dictlenpt < a:
                    answerpt = 'Unknown'
                else:
                    newtext = Dictionarypt['answer']['text']
                    # Strip html code from answer
                    answerptt = remove_html(newtext)
                    # Combine multiline input into a single line
                    answerpt = "_-_".join(line.strip() for line in answerptt.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerpt:
                            answerpt = answerpt.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerpt:
                            answerpt = answerpt.replace(tag, '.')
            except:
                answerpt = 'Unknown'
                pass
            Project_title.append(answerpt)

            # New question introduced in the new GDPR template in April 2023 for faculty
            DictionaryFAN = l2['plan_content'][0]['sections'][0]['questions'][3]
            dictlenFAN = len(DictionaryFAN)
            Fac_situated = []
            a = 7
            try:
                if dictlenFAN < a:
                    answerFAN = 'Unknown'
                else:
                    textFAN = len(DictionaryFAN['answer']['options'])
                    FANnr = 0
                    FANlistdur = ''
                    while FANnr < textFAN:
                        FANlistdur = FANlistdur + " - " + (DictionaryFAN['answer']['options'][FANnr]['text'])
                        FANnr = FANnr + 1
                    answerFAN = FANlistdur
            except:
                answerFAN = 'Unknown'
                pass
            Fac_situated.append(answerFAN)

            Dictionaryon = l2['plan_content'][0]['sections'][0]['questions'][4]
            dictlenon = len(Dictionaryon)
            Org_name = []
            a = 7
            try:
                if dictlenon < a:
                    answeron = 'Unknown'
                else:
                    text = Dictionaryon['answer']['text']
                    # Strip html code from answer
                    answeront = remove_html(text)
                    # Combine multiline input into a single line
                    answeron = "_-_".join(line.strip() for line in answeront.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answeron:
                            answeron = answeron.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answeront:
                            answeron = answeron.replace(tag, '')
            except:
                answeron = 'Unknown'
                pass
            Org_name.append(answeron)

            Dictionaryoo = l2['plan_content'][0]['sections'][0]['questions'][5]
            dictlenoo = len(Dictionaryoo)
            Oth_Org = []
            a = 7
            try:
                if dictlenoo < a:
                    answeroo = 'Unknown'
                else:
                    textyoo = Dictionaryoo['answer']['text']
                    # Strip html code from answer
                    answeroot = remove_html(textyoo)
                    # Combine multiline input into a single line
                    answeroo = "_-_".join(line.strip() for line in answeroot.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answeroo:
                            answeroo = answeroo.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answeroo:
                            answeroo = answeroo.replace(tag, '')
            except:
                answeroo = 'Unknown'
                pass
            Oth_Org.append(answeroo)

            Dictionarypc = l2['plan_content'][0]['sections'][0]['questions'][6]
            dictlenpc = len(Dictionarypc)
            Project_code = []
            a = 7
            try:
                if dictlenpc < a:
                    answerpc = 'Unknown'
                else:
                    textpc = Dictionarypc['answer']['text']
                    # Strip html code from answer
                    answerpct = remove_html(textpc)
                    # Combine multiline input into a single line
                    answerpc = "_-_".join(line.strip() for line in answerpct.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerpc:
                            answerpc = answerpc.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerpc:
                            answerpc = answerpc.replace(tag, '')
            except:
                answerpc = 'Unknown'
                pass
            Project_code.append(answerpc)

            # Exception is necessary here as the DMP url question was introduced in 2023 Jan only
            Other_DMP_url = []
            if len(l2['plan_content'][0]['sections'][0]['questions']) == 8:
                answerodmp = 'Unknown'
            else:
                Dictionarydmp = l2['plan_content'][0]['sections'][0]['questions'][8]
                dictlendmp = len(Dictionarydmp)
                a = 7
                try:
                    if dictlendmp < a:
                        answerodmp = 'Unknown'
                    else:
                        textdmp = Dictionarydmp['answer']['text']
                        if textdmp == '':
                            answerodmp = 'Unknown'
                        else:
                            # Strip html code from answer
                            answerdmpt = remove_html(textdmp)
                            # Combine multiline input into a single line
                            answerodmp = "_-_".join(line.strip() for line in answerdmpt.splitlines())
                            # Remove the special characters
                            for tag in tags_sp:
                                if tag in answerodmp:
                                    answerodmp = answerodmp.replace(tag, ' ')
                            for tag in tags_nv:
                                if tag in answerodmp:
                                    answerodmp = answerodmp.replace(tag, '')
                except:
                    answerodmp = 'Unknown'
                    pass
            Other_DMP_url.append(answerodmp)

            Dictionaryddr = l2['plan_content'][0]['sections'][1]['questions'][0]
            dictlenddr = len(Dictionaryddr)
            data_reuse = []
            a = 7
            try:
                if dictlenddr < a:
                    answerddr = 'Unknown'
                else:
                    textddr = Dictionaryddr['answer']['text']
                    # Strip html code from answer
                    answerddrt = remove_html(textddr)
                    # Combine multiline input into a single line
                    answerddr = "_-_".join(line.strip() for line in answerddrt.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerddr:
                            answerddr = answerddr.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerddr:
                            answerddr = answerddr.replace(tag, '')
            except:
                answerddr = 'Unknown'
                pass
            data_reuse.append(answerddr)

            Dictionaryndd = l2['plan_content'][0]['sections'][1]['questions'][1]
            dictlenndd = len(Dictionaryndd)
            data_new = []
            a = 7
            try:
                if dictlenndd < a:
                    answerndd = 'Unknown'
                else:
                    textndd = Dictionaryndd['answer']['text']
                    # Strip html code from answer
                    answernddt = remove_html(textndd)
                    # Combine multiline input into a single line
                    answerndd = "_-_".join(line.strip() for line in answernddt.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerndd:
                            answerndd = answerndd.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerndd:
                            answerndd = answerndd.replace(tag, '')
            except:
                answerndd = 'Unknown'
                pass
            data_new.append(answerndd)

            Dictionarypop = l2['plan_content'][0]['sections'][1]['questions'][2]
            dictlenpop = len(Dictionarypop)
            popul = []
            a = 7
            try:
                if dictlenpop < a:
                    answerpop = 'Unknown'
                else:
                    textpop = Dictionarypop['answer']['text']
                    # Strip html code from answer
                    answerpopt = remove_html(textpop)
                    # Combine multiline input into a single line
                    answerpop = "_-_".join(line.strip() for line in answerpopt.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerpop:
                            answerpop = answerpop.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerpop:
                            answerpop = answerpop.replace(tag, '')
            except:
                answerpop = 'Unknown'
                pass
            popul.append(answerpop)

            Dictionarydtype = l2['plan_content'][0]['sections'][1]['questions'][3]
            dictlendtype = len(Dictionarydtype)
            pers_data_type = []
            a = 7
            try:
                if dictlendtype < a:
                    answerdtype = 'Unknown'
                else:
                    textcon = len(Dictionarydtype['answer']['options'])
                    connr = 0
                    conslistdur = ''
                    while connr < textcon:
                        conslistdur = conslistdur +" - "+ (Dictionarydtype['answer']['options'][connr]['text'])
                        connr = connr + 1
                    answerdtype = conslistdur
            except:
                answerdtype = 'Unknown'
                pass
            pers_data_type.append(answerdtype)

            Dictionaryic = l2['plan_content'][0]['sections'][1]['questions'][4]
            dictlenic = len(Dictionaryic)
            Inf_consent = []
            a = 7
            try:
                if dictlenic < a:
                    answeric = 'Unknown'
                else:
                    texticn = len(Dictionaryic['answer']['options'])
                    icnnr = 0
                    icnlistdur = ''
                    while icnnr < texticn:
                        icnlistdur = icnlistdur + " - " + (Dictionaryic['answer']['options'][icnnr]['text'])
                        icnnr = icnnr + 1
                    answeric = icnlistdur
            except:
                answeric = 'Unknown'
                pass
            Inf_consent.append(answeric)

            Dictionarylg = l2['plan_content'][0]['sections'][1]['questions'][5]
            dictlenlg = len(Dictionarylg)
            legal_ground = []
            legal_ground_desc = []
            a = 7
            try:
                if dictlenlg < a:
                    answerlg = 'Unknown'
                    answerlgd = 'Unknown'
                else:
                    textlg = Dictionarylg['answer']['text']
                    answerlgdt = remove_html(textlg)
                    # Combine multiline input into a single line
                    answerlgd = "_-_".join(line.strip() for line in answerlgdt.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerlgd:
                            answerlgd = answerlgd.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerlgd:
                            answerlgd = answerlgd.replace(tag, '')
                    if answerlgd == '':
                        answerlgd = 'Unknown'
                    else:
                        answerlgd = answerlgd

                    textlg2 = len(Dictionarylg['answer']['options'])
                    lgnr = 0
                    lglist = ''
                    while lgnr < textlg2:
                        lglist = lglist + " - " + (Dictionarylg['answer']['options'][lgnr]['text'])
                        lgnr = lgnr + 1
                    if len(lglist) == 0:
                        answerlg = 'Unknown'
                    else:
                        answerlg = lglist
            except:
                answerlg = 'Unknown'
                answerlgd = 'Unknown'
                pass
            legal_ground_desc.append(answerlgd)
            legal_ground.append(answerlg)

            Dictionarypdc = l2['plan_content'][0]['sections'][1]['questions'][6]
            dictlenpdc = len(Dictionarypdc)
            Pers_data_cat = []
            Pers_data_cat_desc = []
            a = 7
            try:
                if dictlenpdc < a:
                    answerpdc = 'Unknown'
                    answerpdcd = 'Unknown'
                else:
                    textpdc = Dictionarypdc['answer']['text']
                    if textpdc == "":
                        answerpdcd = 'Unknown'
                    else:
                        answerpdcdt = remove_html(textpdc)
                        # Combine multiline input into a single line
                        answerpdcd = "_-_".join(line.strip() for line in answerpdcdt.splitlines())
                        # Remove the special characters
                        for tag in tags_sp:
                            if tag in answerpdcd:
                                answerpdcd = answerpdcd.replace(tag, ' ')
                        for tag in tags_nv:
                            if tag in answerpdcd:
                                answerpdcd = answerpdcd.replace(tag, '')

                    textdc = len(Dictionarypdc['answer']['options'])
                    dcnr = 0
                    dclist = ''
                    while dcnr < textdc:
                        dclist = dclist + " - " + (Dictionarypdc['answer']['options'][dcnr]['text'])
                        dcnr = dcnr + 1
                    if len(dclist) == 0:
                        answerpdc = 'Unknown'
                    else:
                        answerpdc = dclist
            except:
                answerpdc = 'Unknown'
                answerpdcd = 'Unknown'
                pass
            Pers_data_cat.append(answerpdc)
            Pers_data_cat_desc.append(answerpdcd)

            Dictionaryce = l2['plan_content'][0]['sections'][1]['questions'][7]
            dictlence = len(Dictionaryce)
            Consent_exemption = []
            a = 7
            try:
                if dictlence < a:
                    answerce = 'Unknown'
                else:
                    textce = Dictionaryce['answer']['text']
                    if textce == '':
                        answerce = 'Unknown'
                    else:
                        # Strip html code from answer
                        answercet = remove_html(textce)
                        # Combine multiline input into a single line
                        answerce = "_-_".join(line.strip() for line in answercet.splitlines())
                        # Remove the special characters
                        for tag in tags_sp:
                            if tag in answerce:
                                answerce = answerce.replace(tag, ' ')
                        for tag in tags_nv:
                            if tag in answerce:
                                answerce = answerce.replace(tag, '')
            except:
                answerce = 'Unknown'
                pass
            Consent_exemption.append(answerce)

            Dictionarysecm = l2['plan_content'][0]['sections'][2]['questions'][0]
            dictlensecm = len(Dictionarysecm)
            sec_measures = []
            a = 7
            try:
                if dictlensecm < a:
                    answersecm = 'Unknown'
                else:
                    textsecm = Dictionarysecm['answer']['text']
                    # Strip html code from answer
                    answersecmt = remove_html(textsecm)
                    # Combine multiline input into a single line
                    answersecm = "_-_".join(line.strip() for line in answersecmt.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answersecm:
                            answersecm = answersecm.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answersecm:
                            answersecm = answersecm.replace(tag, '')
            except:
                answersecm = 'Unknown'
                pass
            sec_measures.append(answersecm)

            Dictionarytud = l2['plan_content'][0]['sections'][2]['questions'][1]
            dictlentud = len(Dictionarytud)
            tools_used_during = []
            tools_used_during_desc = []
            a = 7
            try:
                if dictlentud < a:
                    answertud = 'Unknown'
                    answertudd = 'Unknown'
                else:
                    texttud = Dictionarytud['answer']['text']
                    answertuddt = remove_html(texttud)
                    # Combine multiline input into a single line
                    answertudd = "_-_".join(line.strip() for line in answertuddt.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answertudd:
                            answertudd = answertudd.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answertudd:
                            answertudd = answertudd.replace(tag, '')
                    if answertudd == '':
                        answertudd = 'Unknown'
                    else:
                        answertudd = answertudd

                    texttud2 = len(Dictionarytud['answer']['options'])
                    tudnr = 0
                    toolslistdur = ''
                    while tudnr < texttud2:
                        toolslistdur = toolslistdur +" - "+ (Dictionarytud['answer']['options'][tudnr]['text'])
                        tudnr = tudnr + 1
                    answertud = toolslistdur
            except:
                answertud = 'Unknown'
                answertudd = 'Unknown'
                pass
            tools_used_during.append(answertud)
            tools_used_during_desc.append(answertudd)

            Dictionaryotud = l2['plan_content'][0]['sections'][2]['questions'][2]
            dictlenotud = len(Dictionaryotud)
            tools_other_during = []
            a = 7
            try:
                if dictlenotud < a:
                    answerotud = 'Unknown'
                else:
                    textotud = Dictionaryotud['answer']['text']
                    answerotudt = remove_html(textotud)
                    # Combine multiline input into a single line
                    answerotud = "_-_".join(line.strip() for line in answerotudt.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerotud:
                            answerotud = answerotud.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerotud:
                            answerotud = answerotud.replace(tag, '')
                    if answerotud == '':
                        answerotud = 'Unknown'
            except:
                answerotud = 'Unknown'
                pass
            tools_other_during.append(answerotud)

            Dictionarytr = l2['plan_content'][0]['sections'][2]['questions'][3]
            dictlentr = len(Dictionarytr)
            Data_transfer = []
            Data_transfer_a = []
            a = 7
            try:
                if dictlentr < a:
                    answertr = 'Unknown'
                    answertra = 'Unknown'
                else:
                    texttr = Dictionarytr['answer']['text']
                    answertrat = remove_html(texttr)
                    # Combine multiline input into a single line
                    answertra = "_-_".join(line.strip() for line in answertrat.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answertra:
                            answertra = answertra.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answertra:
                            answertra = answertra.replace(tag, '')
                    if answertra == '':
                        answertra = 'Unknown'
                    else:
                        answertra = answertra

                    textt = len(Dictionarytr['answer']['options'])
                    trnr = 0
                    trlist = ''
                    while trnr < textt:
                        trlist = trlist + (Dictionarytr['answer']['options'][trnr]['text'])
                        trnr = trnr + 1
                    if len(trlist) == 0:
                        answertr = 'Unknown'
                    else:
                        answertr = trlist
            except:
                answertr = 'Unknown'
                answertra = 'Unknown'
                pass
            Data_transfer.append(answertr)
            Data_transfer_a.append(answertra)

            Dictionaryeea = l2['plan_content'][0]['sections'][2]['questions'][4]
            dictleneea = len(Dictionaryeea)
            Data_transfer_eea = []
            Data_transfer_eea_d = []
            a = 7
            try:
                if dictleneea < a:
                    answereea = 'Unknown'
                    answereead = 'Unknown'
                else:
                    texteead = Dictionaryeea['answer']['text']
                    answereeadt = remove_html(texteead)
                    # Combine multiline input into a single line
                    answereead = "_-_".join(line.strip() for line in answereeadt.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answereead:
                            answereead = answereead.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answereead:
                            answereead = answereead.replace(tag, '')
                    if answereead == '':
                        answereead = 'Unknown'
                    else:
                        answereead = answereead

                    texteea = len(Dictionaryeea['answer']['options'])
                    eeanr = 0
                    transfereea = ''
                    while eeanr < texteea:
                        transfereea = transfereea + (Dictionaryeea['answer']['options'][eeanr]['text'])
                        eeanr = eeanr + 1
                    answereea = transfereea
            except:
                answereea = 'Unknown'
                answereead = 'Unknown'
                pass
            Data_transfer_eea.append(answereea)
            Data_transfer_eea_d.append(answereead)

            Dictionaryeco = l2['plan_content'][0]['sections'][3]['questions'][0]
            dictleneco = len(Dictionaryeco)
            Ethics_com_approval = []
            Ethics_com_data = []
            a = 7
            try:
                if dictleneco < a:
                    answereco = 'Unknown'
                    answerecod = 'Unknown'
                else:
                    texteco = Dictionaryeco['answer']['text']
                    answerecot = remove_html(texteco)
                    # Combine multiline input into a single line
                    answereco = "_-_".join(line.strip() for line in answerecot.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answereco:
                            answereco = answereco.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answereco:
                            answereco = answereco.replace(tag, '')
                    if answereco == '':
                        answereco = 'Unknown'
                    else:
                        answereco = answereco

                    textecd = len(Dictionaryeco['answer']['options'])
                    econr = 0
                    ecodlist = ''
                    while econr < textecd:
                        ecodlist = ecodlist + " - " + (Dictionaryeco['answer']['options'][econr]['text'])
                        econr = econr + 1
                    if len(ecodlist) == 0:
                        answerecod = 'Unknown'
                    else:
                        answerecod = ecodlist
            except:
                answereco = 'Unknown'
                answerecod = 'Unknown'
                pass
            Ethics_com_approval.append(answereco)
            Ethics_com_data.append(answerecod)

            Dictionaryaop = l2['plan_content'][0]['sections'][2]['questions'][0]
            dictlenaop = len(Dictionaryaop)
            Arch_and_Publ = []
            a = 7
            try:
                if dictlenaop < a:
                    answeraop = 'Unknown'
                else:
                    textaop = Dictionaryaop['answer']['text']
                    # Strip html code from answer
                    answeraopt = remove_html(textaop)
                    # Combine multiline input into a single line
                    answeraop = "_-_".join(line.strip() for line in answeraopt.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answeraop:
                            answeraop = answeraop.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answeraop:
                            answeraop = answeraop.replace(tag, '')
            except:
                answeraop = 'Unknown'
                pass
            Arch_and_Publ.append(answeraop)

            Dictionaryarch = l2['plan_content'][0]['sections'][4]['questions'][1]
            dictlenarch = len(Dictionaryarch)
            Archives = []
            Archives_options = []
            a = 7
            try:
                if dictlenarch < a:
                    answerarch = 'Unknown'
                    answerarcho = 'Unknown'
                else:
                    textarch = Dictionaryarch['answer']['text']
                    answerarcht = remove_html(textarch)
                    # Combine multiline input into a single line
                    answerarch = "_-_".join(line.strip() for line in answerarcht.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerarch:
                            answerarch = answerarch.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerarch:
                            answerarch = answerarch.replace(tag, '')
                    if answerarch == '':
                        answerarch = 'Unknown'
                    else:
                        answerarch = answerarch

                    textarchd2 = len(Dictionaryarch['answer']['options'])
                    archnr = 0
                    archlist = ''
                    while archnr < textarchd2:
                        archlist = archlist + " - " + (Dictionaryarch['answer']['options'][archnr]['text'])
                        archnr = archnr + 1
                    if len(archlist) == 0:
                        answerarcho = 'Unknown'
                    else:
                        answerarcho = archlist
            except:
                answerarch = 'Unknown'
                answerarcho = 'Unknown'
                pass
            Archives.append(answerarch)
            Archives_options.append(answerarcho)

            Dictionarywoa = l2['plan_content'][0]['sections'][4]['questions'][2]
            dictlenwoa = len(Dictionarywoa)
            Other_Archives = []
            a = 7
            try:
                if dictlenwoa < a:
                    answerwoa = 'Unknown'
                else:
                    textwoa = Dictionarywoa['answer']['text']
                    # Strip html code from answer
                    answerwoat = remove_html(textwoa)
                    # Combine multiline input into a single line
                    answerwoa = "_-_".join(line.strip() for line in answerwoat.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerwoa:
                            answerwoa = answerwoa.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerwoa:
                            answerwoa = answerwoa.replace(tag, '')
            except:
                answerwoa = 'Unknown'
                pass
            Other_Archives.append(answerwoa)

            Dictionaryterm = l2['plan_content'][0]['sections'][4]['questions'][3]
            dictlenterm = len(Dictionaryterm)
            Archive_period = []
            a = 7
            try:
                if dictlenterm < a:
                    answerterm = 'Unknown'
                else:
                    textterm = Dictionaryterm['answer']['text']
                    # Strip html code from answer
                    answertermt = remove_html(textterm)
                    # Combine multiline input into a single line
                    answerterm = "_-_".join(line.strip() for line in answertermt.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerterm:
                            answerterm = answerterm.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerterm:
                            answerterm = answerterm.replace(tag, '')
            except:
                answerterm = 'Unknown'
                pass
            Archive_period.append(answerterm)

            Dictionarypubl = l2['plan_content'][0]['sections'][4]['questions'][4]
            dictlenpubl = len(Dictionarypubl)
            Dataset_Publ = []
            a = 7
            try:
                if dictlenpubl < a:
                    answerpubl = 'Unknown'
                else:
                    textpubl = Dictionarypubl['answer']['text']
                    # Strip html code from answer
                    answerpublt = remove_html(textpubl)
                    # Combine multiline input into a single line
                    answerpubl = "_-_".join(line.strip() for line in answerpublt.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerpubl:
                            answerpubl = answerpubl.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerpubl:
                            answerpubl = answerpubl.replace(tag, '')
            except:
                answerpubl = 'Unknown'
                pass
            Dataset_Publ.append(answerpubl)

            Dictionaryrrd = l2['plan_content'][0]['sections'][5]['questions'][0]
            dictlenrrd = len(Dictionaryrrd)
            Res_Resp_during = []
            a = 7
            try:
                if dictlenrrd < a:
                    answerrrd = 'Unknown'
                else:
                    textrrd = Dictionaryrrd['answer']['text']
                    # Strip html code from answer
                    answerrrdt = remove_html(textrrd)
                    # Combine multiline input into a single line
                    answerrrd = "_-_".join(line.strip() for line in answerrrdt.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerrrd:
                            answerrrd = answerrrd.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerrrd:
                            answerrrd = answerrrd.replace(tag, '')
            except:
                answerrrd = 'Unknown'
                pass
            Res_Resp_during.append(answerrrd)

            Dictionaryrar = l2['plan_content'][0]['sections'][5]['questions'][1]
            dictlenrar = len(Dictionaryrar)
            Person_resp_after = []
            a = 7
            try:
                if dictlenrar < a:
                    answerar = 'Unknown'
                else:
                    textrar = Dictionaryrar['answer']['text']
                    # Strip html code from answer
                    answerart = remove_html(textrar)
                    # Combine multiline input into a single line
                    answerar = "_-_".join(line.strip() for line in answerart.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerar:
                            answerar = answerar.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerar:
                            answerar = answerar.replace(tag, '')
            except:
                answerar = 'Unknown'
                pass
            Person_resp_after.append(answerar)

            Dictionaryram = l2['plan_content'][0]['sections'][5]['questions'][1]
            dictlenram = len(Dictionaryram)
            Req_dataset_proc = []
            a = 7
            try:
                if dictlenram < a:
                    answeram = 'Unknown'
                else:
                    textram = Dictionaryram['answer']['text']
                    # Strip html code from answer
                    answeramt = remove_html(textram)
                    # Combine multiline input into a single line
                    answeram = "_-_".join(line.strip() for line in answeramt.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answeram:
                            answeram = answeram.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answeram:
                            answeram = answeram.replace(tag, '')
            except:
                answeram = 'Unknown'
                pass
            Req_dataset_proc.append(answeram)

    # Closing file
    g.close()
    id_data_g = {'id': id_list_g}
    test_listg = pd.DataFrame(id_data_g)
    test_listg['title'] = title_list_g
    test_listg['template'] = templates_g
    test_listg['funder'] = funders_g
    test_listg['last_updated'] = LastUpdate_g
    test_listg['file_name'] = file_name_g
    test_listg['contact_name'] = data_contact_ng
    test_listg['contact_email'] = data_contact_eg
    test_listg['plan_version'] = Plan_version_g
    test_listg['project_title'] = Project_title
    test_listg['Organisation'] = Org_name
    test_listg['Other_org'] = Oth_Org
    test_listg['Project_code'] = Project_code
    test_listg['Other_DMP_url'] = Other_DMP_url
    test_listg['Existing_data'] = data_reuse
    test_listg['New_data'] = data_new
    test_listg['Population_descr'] = popul
    test_listg['Personal_data_type'] = pers_data_type
    test_listg['Informed_consent_type'] = Inf_consent
    test_listg['Other_legal_ground_desc'] = legal_ground_desc
    test_listg['Other_legal_ground'] = legal_ground
    test_listg['Personal_data_cat_desc'] = Pers_data_cat_desc
    test_listg['Personal_data_spec_cat'] = Pers_data_cat
    test_listg['Informed_Consent_Exemption'] = Consent_exemption
    test_listg['Data_security_measures'] = sec_measures
    test_listg['Tools_during_res_descr'] = tools_used_during_desc
    test_listg['Tools_during_research'] = tools_used_during
    test_listg['Other_tools_during'] = tools_other_during
    test_listg['Data_transfer'] = Data_transfer
    test_listg['Data_transfer_desc'] = Data_transfer_a
    test_listg['Data_transfer_EEA'] = Data_transfer_eea
    test_listg['Transfer_EEA_desc'] = Data_transfer_eea_d
    test_listg['Eth_Com_app_needed'] = Ethics_com_approval
    test_listg['Eth_Com_Approved'] = Ethics_com_data
    test_listg['Arch_and_or_Publ'] = Arch_and_Publ
    test_listg['Which_archives'] = Archives
    test_listg['Archive_options'] = Archives_options
    test_listg['Other_archives'] = Other_Archives
    test_listg['Archive_period'] = Archive_period
    test_listg['Dataset_Publish'] = Dataset_Publ
    test_listg['Researcher_Resp_during'] = Res_Resp_during
    test_listg['Person_resp_after'] = Person_resp_after
    test_listg['Proced_to_req_dataset'] = Req_dataset_proc
    test_listg['Faculty_situated'] = Fac_situated
    DMP_gdpr_data = pd.concat([DMP_gdpr_data, test_listg], ignore_index=True)
    i = i + 1

# Drop DMPs that no longer exist and have null data in the overview
DMP_gdpr_data = DMP_gdpr_data.dropna(subset=['id'])
# Export result as a CSV file with the date of the Python run
DMP_gdpr_data.to_csv(f'U:\Werk\Data Management\Python\\Files\DMP_Online\DMP_stats\\'+runyear+'\\overview2\\gdprfulldmplist_'+runday+'.csv', encoding='utf-8')

# Step 5b Start getting the data from the VU main DMP form from json files
# The DMPs for the VU template are in this folder
pathm = 'U:\Werk\Data Management\Python\Files\DMP_Online\DMP_stats\\'+runyear+'\cert2'

# Get list of all VU Cert form files only in the given directory
dmplistcert = lambda x: os.path.isfile(os.path.join(pathm, x))
files_list_cert = filter(dmplistcert, os.listdir(pathm))

# Create a list of files in directory along with the size
size_of_file_c = [
    (f, os.stat(os.path.join(pathm, f)).st_size)
    for f in files_list_cert
]

# Create a table with the list as input
dmp_list_cert = pd.DataFrame(size_of_file_c, columns=['File_name', 'File_size'])
filenrc = dmp_list_cert.shape[0]

# Go through the files to find out which DMPs concern personal data
DMP_cert_data_temp = pd.DataFrame()
i = 0
while i != filenrc:
    c = open(f'U:\Werk\Data Management\Python\Files\DMP_Online\DMP_stats\\{runyear}\cert2\\{dmp_list_cert.File_name[i]}', 'r')
    # returns JSON object as a dictionary
    datac = json.load(c)
    # Iterating through the json list to get the item on sensitive research: yes/no
    id_list_c = []
    file_name = []

    for l1 in datac:
        for l2 in l1:
            id_list_c.append(str(l2['id']))
            file_name.append(dmp_list_cert.File_name[i])

            CDictionaryq = l2['plan_content'][0]['sections'][1]['questions'][0]
            dictlenpdch = len(CDictionaryq)
            Personal_data_check = []
            a = 7
            try:
                if dictlenpdch < a:
                    answerpdch = 'Unknown'
                else:
                    answerpdch = CDictionaryq['answer']['options'][0]['text']
            except:
                answerpdch = 'Unknown'
                pass
            Personal_data_check.append(answerpdch)

    # Close file
    c.close()
    id_data_c = {'id': id_list_c}
    test_listc = pd.DataFrame(id_data_c)
    test_listc['File_name'] = file_name
    test_listc['PersonalDataCheck'] = Personal_data_check
    DMP_cert_data_temp = pd.concat([DMP_cert_data_temp, test_listc], ignore_index=True)
    # Drop DMPs that do not involve personal data
    DMP_cert_data_temp = DMP_cert_data_temp[DMP_cert_data_temp['PersonalDataCheck'].str.contains('Yes') == True]
    # Drop Dmps files that are empty
    DMP_cert_data_temp = DMP_cert_data_temp[DMP_cert_data_temp['id'].le('id') > 0]
    i = i + 1

# DMP_cert_data_temp.to_csv(f'U:\Werk\Data Management\Python\\Files\DMP_Online\DMP_stats\\'+runyear+'\cert\\fullcertlist_sens.csv', encoding='utf-8')

# The table DMP_cert_data_temp now lists all VU cert DMPs involving personal data
filenrcert = DMP_cert_data_temp.shape[0]

DMP_cert_data = pd.DataFrame()

v = 0
while v != filenrcert:
    logger.debug(f'Collecting data from Cert file: ' + DMP_cert_data_temp.File_name[v])
    h = open(f'U:\Werk\Data Management\Python\Files\DMP_Online\DMP_stats\\{runyear}\cert2\\{DMP_cert_data_temp.File_name[v]}', 'r')
    # returns JSON object as a dictionary
    datah = json.load(h)
    # Iterating through the json list to get specific items
    # First put them in item lists and then create a table by combining them
    id_list_h = []
    title_list_h = []
    file_name_h = []
    templates_h = []
    funders_h = []
    LastUpdate_h = []
    data_contact_nh = []
    data_contact_eh = []

    for l1 in datah:
        for l2 in l1:
            id_list_h.append(str(l2['id']))
            # Remove the special characters from the title
            title = l2['title']
            for tag in tags_sp:
                if tag in title:
                    title = title.replace(tag, ' ')
            for tag in tags_nv:
                if tag in title:
                    title = title.replace(tag, '.')
            title_list_h.append(title)
            file_name_h.append(DMP_cert_data_temp.File_name[v])
            templates_h.append(l2['template']['title'])
            # How to deal with null in json dictionary ?
            funder = l2['funder']['name']
            funders_h.append(funder)
            LastUpdate_h.append(l2['last_updated'])
            data_contact_nh.append(l2['data_contact']['name'])
            data_contact_eh.append(l2['data_contact']['email'])

            Dictionarypv = l2['plan_content'][0]['sections'][0]['questions'][0]
            dictlenpv = len(Dictionarypv)
            Plan_version_h = []
            a = 7
            try:
                if dictlenpv < a:
                    answerpv = 'Unknown'
                else:
                    textpv = Dictionarypv['answer']['text']
                    # Strip html code from answer
                    answerpvt = remove_html(textpv)
                    # Combine multiline input into a single line
                    answerpv = "_-_".join(line.strip() for line in answerpvt.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerpv:
                            answerpv = answerpv.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerpv:
                            answerpv = answerpv.replace(tag, '.')
            except:
                answerpv = 'Unknown'
                pass
            Plan_version_h.append(answerpv)

            Dictionaryptc = l2['plan_content'][0]['sections'][0]['questions'][1]
            dictlenptc = len(Dictionaryptc)
            Project_title_h = []
            a = 7
            try:
                if dictlenptc < a:
                    answerptc = 'Unknown'
                else:
                    newtext = Dictionaryptc['answer']['text']
                    # Strip html code from answer
                    answerptct = remove_html(newtext)
                    # Combine multiline input into a single line
                    answerptc = "_-_".join(line.strip() for line in answerptct.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerptc:
                            answerptc = answerptc.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerptc:
                            answerptc = answerptc.replace(tag, '.')
            except:
                answerptc = 'Unknown'
                pass
            Project_title_h.append(answerptc)

            # New question introduced in the new main certified template in April 2023 for faculty
            DictionaryFAN2 = l2['plan_content'][0]['sections'][0]['questions'][3]
            dictlenFAN2 = len(DictionaryFAN2)
            Fac_situated2 = []
            a = 7
            try:
                if dictlenFAN2 < a:
                    answerFAN2 = 'Unknown'
                else:
                    textFAN2 = len(DictionaryFAN2['answer']['options'])
                    FANnr2 = 0
                    FANlistdur2 = ''
                    while FANnr2 < textFAN2:
                        FANlistdur2 = FANlistdur2 + " - " + (DictionaryFAN2['answer']['options'][FANnr2]['text'])
                        FANnr2 = FANnr2 + 1
                    answerFAN2 = FANlistdur2
            except:
                answerFAN2 = 'Unknown'
                pass
            Fac_situated2.append(answerFAN2)

            Dictionaryonc = l2['plan_content'][0]['sections'][0]['questions'][4]
            dictlenonc = len(Dictionaryonc)
            Org_name_h = []
            a = 7
            try:
                if dictlenonc < a:
                    answeronc = 'Unknown'
                else:
                    textonc = Dictionaryonc['answer']['text']
                    # Strip html code from answer
                    answeronct = remove_html(textonc)
                    # Combine multiline input into a single line
                    answeronc = "_-_".join(line.strip() for line in answeronct.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answeronc:
                            answeronc = answeronc.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answeronc:
                            answeronc = answeronc.replace(tag, '')
            except:
                answeronc = 'Unknown'
                pass
            Org_name_h.append(answeronc)

            Dictionaryooc = l2['plan_content'][0]['sections'][0]['questions'][5]
            dictlenooc = len(Dictionaryooc)
            Oth_Org_h = []
            a = 7
            try:
                if dictlenooc < a:
                    answerooc = 'Unknown'
                else:
                    textooc = Dictionaryooc['answer']['text']
                    # Strip html code from answer
                    answerooct = remove_html(textooc)
                    # Combine multiline input into a single line
                    answerooc = "_-_".join(line.strip() for line in answerooct.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerooc:
                            answerooc = answerooc.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerooc:
                            answerooc = answerooc.replace(tag, '')
            except:
                answerooc = 'Unknown'
                pass
            Oth_Org_h.append(answerooc)

            Dictionarypcc = l2['plan_content'][0]['sections'][0]['questions'][6]
            dictlenpcc = len(Dictionarypcc)
            Project_code_h = []
            a = 7
            try:
                if dictlenpcc < a:
                    answerpcc = 'Unknown'
                else:
                    textpcc = Dictionarypcc['answer']['text']
                    # Strip html code from answer
                    answerpcct = remove_html(textpcc)
                    # Combine multiline input into a single line
                    answerpcc = "_-_".join(line.strip() for line in answerpcct.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerpcc:
                            answerpcc = answerpcc.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerpcc:
                            answerpcc = answerpcc.replace(tag, '')
                    if answerpcc == '':
                        answerpcc = 'Unknown'
                    else:
                        pass
            except:
                answerpcc = 'Unknown'
                pass
            Project_code_h.append(answerpcc)

            Dictionaryddrc = l2['plan_content'][0]['sections'][1]['questions'][1]
            dictlenddrc = len(Dictionaryddrc)
            data_reuse_h = []
            a = 7
            try:
                if dictlenddrc < a:
                    answerddrc = 'Unknown'
                else:
                    textddrc = Dictionaryddrc['answer']['text']
                    # Strip html code from answer
                    answerddrct = remove_html(textddrc)
                    # Combine multiline input into a single line
                    answerddrc = "_-_".join(line.strip() for line in answerddrct.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerddrc:
                            answerddrc = answerddrc.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerddrc:
                            answerddrc = answerddrc.replace(tag, '')
                    if answerddrc == '':
                        answerddrc = 'Unknown'
                    else:
                        pass
            except:
                answerddrc = 'Unknown'
                pass
            data_reuse_h.append(answerddrc)

            Dictionarynddc = l2['plan_content'][0]['sections'][1]['questions'][2]
            dictlennddc = len(Dictionarynddc)
            data_new_h = []
            a = 7
            try:
                if dictlennddc < a:
                    answernddc = 'Unknown'
                else:
                    textddc = Dictionarynddc['answer']['text']
                    # Strip html code from answer
                    answernddct = remove_html(textddc)
                    # Combine multiline input into a single line
                    answernddc = "_-_".join(line.strip() for line in answernddct.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answernddc:
                            answernddc = answernddc.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answernddc:
                            answernddc = answernddc.replace(tag, '')
            except:
                answernddc = 'Unknown'
                pass
            data_new_h.append(answernddc)

            Dictionarypopc = l2['plan_content'][0]['sections'][1]['questions'][3]
            dictlenpopc = len(Dictionarypopc)
            popul_h = []
            a = 7
            try:
                if dictlenpopc < a:
                    answerpopc = 'Unknown'
                else:
                    textpop = Dictionarypopc['answer']['text']
                    # Strip html code from answer
                    answerpopct = remove_html(textpop)
                    # Combine multiline input into a single line
                    answerpopc = "_-_".join(line.strip() for line in answerpopct.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerpopc:
                            answerpopc = answerpopc.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerpopc:
                            answerpopc = answerpopc.replace(tag, '')
            except:
                answerpopc = 'Unknown'
                pass
            popul_h.append(answerpopc)

            Dictionarydtypeh = l2['plan_content'][0]['sections'][1]['questions'][4]
            dictlendtypeh = len(Dictionarydtypeh)
            pers_data_type_h = []
            a = 7
            try:
                if dictlendtypeh < a:
                    answerdtypeh = 'Unknown'
                else:
                    textconh = len(Dictionarydtypeh['answer']['options'])
                    connrh = 0
                    conslistdurh = ''
                    while connrh < textconh:
                        conslistdurh = conslistdurh +" - "+ (Dictionarydtypeh['answer']['options'][connrh]['text'])
                        connrh = connrh + 1
                    answerdtypeh = conslistdurh
            except:
                answerdtypeh = 'Unknown'
                pass
            pers_data_type_h.append(answerdtypeh)

            Dictionaryich = l2['plan_content'][0]['sections'][1]['questions'][5]
            dictlenich = len(Dictionaryich)
            Inf_consent_h = []
            a = 7
            try:
                if dictlenich < a:
                    answerich = 'Unknown'
                else:
                    texticnh = len(Dictionaryich['answer']['options'])
                    icnnrh = 0
                    icnlistdurh = ''
                    while icnnrh < texticnh:
                        icnlistdurh = icnlistdurh + " - " + (Dictionaryich['answer']['options'][icnnrh]['text'])
                        icnnrh = icnnrh + 1
                    answerich = icnlistdurh
            except:
                answerich = 'Unknown'
                pass
            Inf_consent_h.append(answerich)

            Dictionarylgh = l2['plan_content'][0]['sections'][1]['questions'][6]
            dictlenlgh = len(Dictionarylgh)
            legal_ground_h = []
            legal_ground_hd = []
            a = 7
            try:
                if dictlenlgh < a:
                    answerlgh = 'Unknown'
                    answerlghd = 'Unknown'
                else:
                    textlgh = Dictionarylgh['answer']['text']
                    answerlghdt = remove_html(textlgh)
                    # Combine multiline input into a single line
                    answerlghd = "_-_".join(line.strip() for line in answerlghdt.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerlghd:
                            answerlghd = answerlghd.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerlghd:
                            answerlghd = answerlghd.replace(tag, '')
                    if answerlghd == '':
                        answerlghd = 'Unknown'
                    else:
                        answerlghd = answerlghd

                    textlghn = len(Dictionarylgh['answer']['options'])
                    lghnr = 0
                    lghlist = ''
                    while lghnr < textlghn:
                        lghlist = lghlist + " - " + (Dictionarylgh['answer']['options'][lghnr]['text'])
                        lghnr = lghnr + 1
                    if len(lghlist) == 0:
                        answerlgh = 'Unknown'
                    else:
                        answerlgh = lghlist
            except:
                answerlgh = 'Unknown'
                answerlghd = 'Unknown'
                pass
            legal_ground_h.append(answerlgh)
            legal_ground_hd.append(answerlghd)

            Dictionarypdch = l2['plan_content'][0]['sections'][1]['questions'][7]
            dictlenpdch = len(Dictionarypdch)
            Pers_data_cat_h = []
            Pers_cat_desc_h = []
            a = 7
            try:
                if dictlenpdch < a:
                    answerpdch = 'Unknown'
                    answerpdcdh = 'Unknown'
                else:
                    textpdch = Dictionarypdch['answer']['text']
                    if textpdch == "":
                        answerpdcdh = 'Unknown'
                    else:
                        answerpdcdht = remove_html(textpdch)
                        # Combine multiline input into a single line
                        answerpdcdh = "_-_".join(line.strip() for line in answerpdcdht.splitlines())
                        # Remove the special characters
                        for tag in tags_sp:
                            if tag in answerpdcdh:
                                answerpdcdh = answerpdcdh.replace(tag, ' ')
                        for tag in tags_nv:
                            if tag in answerpdcdh:
                                answerpdcdh = answerpdcdh.replace(tag, '')

                    textdch = len(Dictionarypdch['answer']['options'])
                    dchnr = 0
                    dchlist = ''
                    while dchnr < textdch:
                        dchlist = dchlist + " - " + (Dictionarypdch['answer']['options'][dchnr]['text'])
                        dchnr = dchnr + 1
                    if len(dchlist) == 0:
                        answerpdch = 'Unknown'
                    else:
                        answerpdch = dclist
            except:
                answerpdch = 'Unknown'
                answerpdcdh = 'Unknown'
                pass
            Pers_data_cat_h.append(answerpdch)
            Pers_cat_desc_h.append(answerpdcdh)

            Dictionaryceh = l2['plan_content'][0]['sections'][1]['questions'][8]
            dictlenceh = len(Dictionaryceh)
            Consent_exemption_h = []
            a = 7
            try:
                if dictlenceh < a:
                    answerceh = 'Unknown'
                else:
                    textceh = Dictionaryceh['answer']['text']
                    if textceh == '':
                        answerceh = 'Unknown'
                    else:
                        # Strip html code from answer
                        answerceht = remove_html(textceh)
                        # Combine multiline input into a single line
                        answerceh = "_-_".join(line.strip() for line in answerceht.splitlines())
                        # Remove the special characters
                        for tag in tags_sp:
                            if tag in answerceh:
                                answerceh = answerceh.replace(tag, ' ')
                        for tag in tags_nv:
                            if tag in answerceh:
                                answerceh = answerceh.replace(tag, '')
            except:
                answerceh = 'Unknown'
                pass
            Consent_exemption_h.append(answerceh)

            Dictionarysecmh = l2['plan_content'][0]['sections'][3]['questions'][0]
            dictlensecmh = len(Dictionarysecmh)
            sec_measures_h = []
            a = 7
            try:
                if dictlensecmh < a:
                    answersecmh = 'Unknown'
                else:
                    textsecm = Dictionarysecmh['answer']['text']
                    # Strip html code from answer
                    answersecmht = remove_html(textsecm)
                    # Combine multiline input into a single line
                    answersecmh = "_-_".join(line.strip() for line in answersecmht.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answersecmh:
                            answersecmh = answersecmh.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answersecmh:
                            answersecmh = answersecmh.replace(tag, '')
            except:
                answersecmh = 'Unknown'
                pass
            sec_measures_h.append(answersecmh)

            Dictionarytudh = l2['plan_content'][0]['sections'][3]['questions'][2]
            dictlentudh = len(Dictionarytudh)
            tools_used_during_h = []
            tools_used_during_desch = []
            a = 7
            try:
                if dictlentudh < a:
                    answertudh = 'Unknown'
                    answertuddh = 'Unknown'
                else:
                    texttudh = Dictionarytudh['answer']['text']
                    answertuddth = remove_html(texttudh)
                    # Combine multiline input into a single line
                    answertuddh = "_-_".join(line.strip() for line in answertuddth.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answertuddh:
                            answertuddh = answertuddh.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answertuddh:
                            answertuddh = answertuddh.replace(tag, '')
                    if answertuddh == '':
                        answertuddh = 'Unknown'
                    else:
                        answertuddh = answertuddh

                    texttudh = len(Dictionarytudh['answer']['options'])
                    tudhnr = 0
                    toolslistdurh = ''
                    while tudhnr < texttudh:
                        toolslistdurh = toolslistdurh + " - " + (Dictionarytudh['answer']['options'][tudhnr]['text'])
                        tudhnr = tudhnr + 1
                    answertudh = toolslistdurh
            except:
                answertudh = 'Unknown'
                answertuddh = 'Unknown'
                pass
            tools_used_during_h.append(answertudh)
            tools_used_during_desch.append(answertuddh)

            Dictionaryotudh = l2['plan_content'][0]['sections'][3]['questions'][3]
            dictlenotudh = len(Dictionaryotudh)
            tools_other_during_h = []
            a = 7
            try:
                if dictlenotudh < a:
                    answerotudh = 'Unknown'
                else:
                    textotudh = Dictionaryotudh['answer']['text']
                    answerotudht = remove_html(textotudh)
                    # Combine multiline input into a single line
                    answerotudh = "_-_".join(line.strip() for line in answerotudht.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerotudh:
                            answerotudh = answerotudh.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerotudh:
                            answerotudh = answerotudh.replace(tag, '')
                    if answerotudh == '':
                        answerotudh = 'Unknown'
            except:
                answerotudh = 'Unknown'
                pass
            tools_other_during_h.append(answerotudh)

            Dictionarytrh = l2['plan_content'][0]['sections'][3]['questions'][4]
            dictlentrh = len(Dictionarytrh)
            Data_transfer_h = []
            Data_transfer_ah = []
            a = 7
            try:
                if dictlentrh < a:
                    answertrh = 'Unknown'
                    answertrah = 'Unknown'
                else:
                    texth = Dictionarytrh['answer']['text']
                    answertraht = remove_html(texth)
                    # Combine multiline input into a single line
                    answertrah = "_-_".join(line.strip() for line in answertraht.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answertrah:
                            answertrah = answertrah.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answertrah:
                            answertrah = answertrah.replace(tag, '')
                    if answertrah == '':
                        answertrah = 'Unknown'
                    else:
                        answertrah = answertrah

                    textth = len(Dictionarytrh['answer']['options'])
                    trnrh = 0
                    trhlist = ''
                    while trnrh < textth:
                        trhlist = trhlist + (Dictionarytrh['answer']['options'][trnrh]['text'])
                        trnrh = trnrh + 1
                    if len(trhlist) == 0:
                        answertrh = 'Unknown'
                    else:
                        answertrh = trhlist
            except:
                answertrh = 'Unknown'
                answertrah = 'Unknown'
                pass
            Data_transfer_h.append(answertrh)
            Data_transfer_ah.append(answertrah)

            Dictionaryeeah = l2['plan_content'][0]['sections'][3]['questions'][6]
            dictleneeah = len(Dictionaryeeah)
            Data_transfer_eeah = []
            Data_transfer_eea_dh = []
            a = 7
            try:
                if dictleneeah < a:
                    answereeah = 'Unknown'
                    answereeadh = 'Unknown'
                else:
                    texteeadh = Dictionaryeeah['answer']['text']
                    answereeadht = remove_html(texteeadh)
                    # Combine multiline input into a single line
                    answereeadh = "_-_".join(line.strip() for line in answereeadht.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answereeadh:
                            answereeadh = answereeadh.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answereeadh:
                            answereeadh = answereeadh.replace(tag, '')
                    if answereeadh == '':
                        answereeadh = 'Unknown'
                    else:
                        answereeadh = answereeadh

                    texteeah = len(Dictionaryeeah['answer']['options'])
                    eeanrh = 0
                    transfereeah = ''
                    while eeanrh < texteeah:
                        transfereeah = transfereeah + (Dictionaryeeah['answer']['options'][eeanrh]['text'])
                        eeanrh = eeanrh + 1
                    answereeah = transfereeah
            except:
                answereeah = 'Unknown'
                answereeadh = 'Unknown'
                pass
            Data_transfer_eeah.append(answereeah)
            Data_transfer_eea_dh.append(answereeadh)

            Dictionaryecoh = l2['plan_content'][0]['sections'][2]['questions'][2]
            dictlenecoh = len(Dictionaryecoh)
            Ethics_com_approvalh = []
            Ethics_com_datah = []
            a = 7
            try:
                if dictlenecoh < a:
                    answerecoh = 'Unknown'
                    answerecodh = 'Unknown'
                else:
                    textecoh = Dictionaryecoh['answer']['text']
                    answerecoht = remove_html(textecoh)
                    # Combine multiline input into a single line
                    answerecoh = "_-_".join(line.strip() for line in answerecoht.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerecoh:
                            answerecoh = answerecoh.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerecoh:
                            answerecoh = answerecoh.replace(tag, '')
                    if answerecoh == '':
                        answerecoh = 'Unknown'
                    else:
                        answerecoh = answerecoh

                    textecdh = len(Dictionaryecoh['answer']['options'])
                    ecohnr = 0
                    ecodhlist = ''
                    while ecohnr < textecdh:
                        ecodhlist = ecodhlist + (Dictionaryecoh['answer']['options'][ecohnr]['text'])
                        ecohnr = ecohnr + 1
                    if len(ecodhlist) == 0:
                        answerecodh = 'Unknown'
                    else:
                        answerecodh = ecodhlist
            except:
                answerecoh = 'Unknown'
                answerecodh = 'Unknown'
                pass
            Ethics_com_approvalh.append(answerecoh)
            Ethics_com_datah.append(answerecodh)

            Dictionaryaoph = l2['plan_content'][0]['sections'][4]['questions'][0]
            dictlenaoph = len(Dictionaryaoph)
            Arch_and_Publh = []
            a = 7
            try:
                if dictlenaoph < a:
                    answeraoph = 'Unknown'
                else:
                    textaoph = Dictionaryaoph['answer']['text']
                    # Strip html code from answer
                    answeraopht = remove_html(textaoph)
                    # Combine multiline input into a single line
                    answeraoph = "_-_".join(line.strip() for line in answeraopht.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answeraoph:
                            answeraoph = answeraoph.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answeraoph:
                            answeraoph = answeraoph.replace(tag, '')
                    if answeraoph == '':
                        answeraoph = 'Unknown'
                    else:
                        answeraoph = answeraoph
            except:
                answeraoph = 'Unknown'
                pass
            Arch_and_Publh.append(answeraoph)

            Dictionaryarchh = l2['plan_content'][0]['sections'][4]['questions'][1]
            dictlenarchh = len(Dictionaryarchh)
            Archivesh = []
            Archives_optionsh = []
            a = 7
            try:
                if dictlenarchh < a:
                    answerarchh = 'Unknown'
                    answerarchoh = 'Unknown'
                else:
                    textarchh = Dictionaryarchh['answer']['text']
                    answerarchht = remove_html(textarchh)
                    # Combine multiline input into a single line
                    answerarchh = "_-_".join(line.strip() for line in answerarchht.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerarchh:
                            answerarchh = answerarchh.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerarchh:
                            answerarchh = answerarchh.replace(tag, '')
                    if answerarchh == '':
                        answerarchh = 'Unknown'
                    else:
                        answerarchh = answerarchh

                    textarchd = len(Dictionaryarchh['answer']['options'])
                    archhnr = 0
                    archhlist = ''
                    while archhnr < textarchd:
                        archhlist = archhlist + " - " + (Dictionaryarchh['answer']['options'][archhnr]['text'])
                        archhnr = archhnr + 1
                    if len(archhlist) == 0:
                        answerarchoh = 'Unknown'
                    else:
                        answerarchoh = archhlist
            except:
                answerarchh = 'Unknown'
                answerarchoh = 'Unknown'
                pass
            Archivesh.append(answerarchh)
            Archives_optionsh.append(answerarchoh)

            Dictionarywoah = l2['plan_content'][0]['sections'][4]['questions'][2]
            dictlenwoah = len(Dictionarywoah)
            Other_Archivesh = []
            a = 7
            try:
                if dictlenwoah < a:
                    answerwoah = 'Unknown'
                else:
                    textwoah = Dictionarywoah['answer']['text']
                    # Strip html code from answer
                    answerwoaht = remove_html(textwoah)
                    # Combine multiline input into a single line
                    answerwoah = "_-_".join(line.strip() for line in answerwoaht.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerwoah:
                            answerwoah = answerwoah.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerwoah:
                            answerwoah = answerwoah.replace(tag, '')
                    if answerwoah == '':
                        answerwoah = 'Unknown'
                    else:
                        answerwoah = answerwoah
            except:
                answerwoah = 'Unknown'
                pass
            Other_Archivesh.append(answerwoah)

            Dictionarytermh = l2['plan_content'][0]['sections'][4]['questions'][3]
            dictlentermh = len(Dictionarytermh)
            Archive_periodh = []
            a = 7
            try:
                if dictlentermh < a:
                    answertermh = 'Unknown'
                else:
                    texttermh = Dictionarytermh['answer']['text']
                    # Strip html code from answer
                    answertermht = remove_html(texttermh)
                    # Combine multiline input into a single line
                    answertermh = "_-_".join(line.strip() for line in answertermht.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answertermh:
                            answertermh = answertermh.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answertermh:
                            answertermh = answertermh.replace(tag, '')
                    if answertermh == '':
                        answertermh = 'Unknown'
                    else:
                        answertermh = answertermh
            except:
                answerterm = 'Unknown'
                pass
            Archive_periodh.append(answertermh)

            Dictionarypublh = l2['plan_content'][0]['sections'][4]['questions'][5]
            dictlenpublh = len(Dictionarypublh)
            Dataset_Publh = []
            a = 7
            try:
                if dictlenpublh < a:
                    answerpublh = 'Unknown'
                else:
                    textpublh = Dictionarypublh['answer']['text']
                    # Strip html code from answer
                    answerpublht = remove_html(textpublh)
                    # Combine multiline input into a single line
                    answerpublh = "_-_".join(line.strip() for line in answerpublht.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerpublh:
                            answerpublh = answerpubl.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerpublh:
                            answerpublh = answerpublh.replace(tag, '')
                    if answerpublh == '':
                        answerpublh = 'Unknown'
                    else:
                        answerpublh = answerpublh
            except:
                answerpublh = 'Unknown'
                pass
            Dataset_Publh.append(answerpublh)

            Dictionaryrrdh = l2['plan_content'][0]['sections'][6]['questions'][0]
            dictlenrrdh = len(Dictionaryrrdh)
            Res_Resp_duringh = []
            a = 7
            try:
                if dictlenrrdh < a:
                    answerrrdh = 'Unknown'
                else:
                    textrrdh = Dictionaryrrdh['answer']['text']
                    # Strip html code from answer
                    answerrrdht = remove_html(textrrdh)
                    # Combine multiline input into a single line
                    answerrrdh = "_-_".join(line.strip() for line in answerrrdht.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerrrdh:
                            answerrrdh = answerrrdh.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerrrdh:
                            answerrrdh = answerrrdh.replace(tag, '')
            except:
                answerrrdh = 'Unknown'
                pass
            Res_Resp_duringh.append(answerrrdh)

            Dictionaryrarh = l2['plan_content'][0]['sections'][6]['questions'][1]
            dictlenrarh = len(Dictionaryrarh)
            Person_resp_afterh = []
            a = 7
            try:
                if dictlenrarh < a:
                    answerarh = 'Unknown'
                else:
                    textrarh = Dictionaryrarh['answer']['text']
                    # Strip html code from answer
                    answerarht = remove_html(textrarh)
                    # Combine multiline input into a single line
                    answerarh = "_-_".join(line.strip() for line in answerarht.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answerarh:
                            answerarh = answerarh.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answerarh:
                            answerarh = answerarh.replace(tag, '')
            except:
                answerarh = 'Unknown'
                pass
            Person_resp_afterh.append(answerarh)

            Dictionaryramh = l2['plan_content'][0]['sections'][6]['questions'][2]
            dictlenramh = len(Dictionaryramh)
            Req_dataset_proch = []
            a = 7
            try:
                if dictlenramh < a:
                    answeramh = 'Unknown'
                else:
                    textramh = Dictionaryramh['answer']['text']
                    # Strip html code from answer
                    answeramht = remove_html(textramh)
                    # Combine multiline input into a single line
                    answeramh = "_-_".join(line.strip() for line in answeramht.splitlines())
                    # Remove the special characters
                    for tag in tags_sp:
                        if tag in answeramh:
                            answeramh = answeramh.replace(tag, ' ')
                    for tag in tags_nv:
                        if tag in answeramh:
                            answeramh = answeramh.replace(tag, '')
            except:
                answeramh = 'Unknown'
                pass
            Req_dataset_proch.append(answeramh)

    # Closing file
    h.close()
    id_data_h = {'id': id_list_h}
    test_listh = pd.DataFrame(id_data_h)
    test_listh['title'] = title_list_h
    test_listh['template'] = templates_h
    test_listh['funder'] = funders_h
    test_listh['last_updated'] = LastUpdate_h
    test_listh['file_name'] = file_name_h
    test_listh['contact_name'] = data_contact_nh
    test_listh['contact_email'] = data_contact_eh
    test_listh['plan_version'] = Plan_version_h
    test_listh['project_title'] = Project_title_h
    test_listh['Organisation'] = Org_name_h
    test_listh['Other_org'] = Oth_Org_h
    test_listh['Project_code'] = Project_code_h
    test_listh['Other_DMP_url'] = 'Unknown'
    test_listh['Existing_data'] = data_reuse_h
    test_listh['New_data'] = data_new_h
    test_listh['Population_descr'] = popul_h
    test_listh['Personal_data_type'] = pers_data_type_h
    test_listh['Informed_consent_type'] = Inf_consent_h
    test_listh['Other_legal_ground_desc'] = legal_ground_hd
    test_listh['Other_legal_ground'] = legal_ground_h
    test_listh['Personal_data_cat_desc'] = Pers_cat_desc_h
    test_listh['Personal_data_spec_cat'] = Pers_data_cat_h
    test_listh['Informed_Consent_Exemption'] = Consent_exemption_h
    test_listh['Data_security_measures'] = sec_measures_h
    test_listh['Tools_during_res_descr'] = tools_used_during_desch
    test_listh['Tools_during_research'] = tools_used_during_h
    test_listh['Other_tools_during'] = tools_other_during_h
    test_listh['Data_transfer'] = Data_transfer_h
    test_listh['Data_transfer_desc'] = Data_transfer_ah
    test_listh['Data_transfer_EEA'] = Data_transfer_eeah
    test_listh['Transfer_EEA_desc'] = Data_transfer_eea_dh
    test_listh['Eth_Com_app_needed'] = Ethics_com_approvalh
    test_listh['Eth_Com_Approved'] = Ethics_com_datah
    test_listh['Arch_and_or_Publ'] = Arch_and_Publh
    test_listh['Which_archives'] = Archivesh
    test_listh['Archive_options'] = Archives_optionsh
    test_listh['Other_archives'] = Other_Archivesh
    test_listh['Archive_period'] = Archive_periodh
    test_listh['Dataset_Publish'] = Dataset_Publh
    test_listh['Researcher_Resp_during'] = Res_Resp_duringh
    test_listh['Person_resp_after'] = Person_resp_afterh
    test_listh['Proced_to_req_dataset'] = Req_dataset_proch
    test_listh['Faculty_situated'] = Fac_situated2
    DMP_cert_data = pd.concat([DMP_cert_data, test_listh], ignore_index=True)
    v = v + 1

# Drop DMPs that no longer exist and have null data in the overview
DMP_cert_data = DMP_cert_data.dropna(subset=['id'])
# Export full file with cert VU DMP data
DMP_cert_data.to_csv(f'U:\Werk\Data Management\Python\\Files\DMP_Online\DMP_stats\\'+runyear+'\\overview2\\certfulldmplist_'+runday+'.csv', encoding='utf-8')

# Step 6: Combine data from both certified VU template dmps and GDPR forms into an overview
# Create an empty dataframe
Register_sensitive_research_temp = pd.DataFrame()
# Make a list of Tables / DataFrames to combine
overview = [DMP_cert_data, DMP_gdpr_data]
# Combine both into a single table
Register_sensitive_research_temp = pd.concat(overview, ignore_index=True)

# Step 7 Get a list of all DMPS to process and get the metadata through api2
Path('DMP_stats\\'+runyear+'\meta_all2').mkdir(parents=True, exist_ok=True)
filenrfm = Register_sensitive_research_temp.shape[0]
filenrf = str(filenrfm)
metalist = Register_sensitive_research_temp['id'].values.tolist()
dmps = metalist

# api2 currently has a limit somewhere beyond 150 items
# Step 8 Use metalist to download the meta for DMPS as separate file
if len(dmps) < 151:
    filenrf = str(len(dmps))
    print('Gettings data for ' + filenrf + ' DMPs')
    plans = api2.retrieve_plans(dmps)
    pages = list(plans)
    # save json file of the DMP
    with open(f'DMP_stats\\'+runyear+'\meta_all2\DMPS_metadata0.json', 'w') as f:
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
        with open(f'DMP_stats\\'+runyear+'\meta_all2/DMPS_metadata'+str(startmlnr)+'.json', 'w') as f:
            f.write(json.dumps(pages))
        snrofml = snrofml + 150
        startmlnr = startmlnr + 1

# Step 9 Use Metadata json file to get data for each dmp
# Get list of all metadata files only in the given directory
metapath = 'U:\Werk\Data Management\Python\Files\DMP_Online\DMP_stats\\'+runyear+'\meta_all2'

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
    nf = open(f'U:\Werk\Data Management\Python\\Files\DMP_Online\DMP_stats\\{runyear}\meta_all2/{metadmp_list.File_name[m]}', 'r')
    # returns JSON object as a dictionary
    data_meta = json.load(nf)
    # Iterating through the metadata json file to get specific items for all DMPs
    id_list_md = []
    project_start = []
    project_end = []
    for l1 in data_meta:
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

DMP_meta_data.to_csv(f'U:\Werk\Data Management\Python\\Files\DMP_Online\DMP_stats\\'+runyear+'\\overview2\\register_metadata_list_'+runday+'.csv', encoding='utf-8')

# Step 10: Combine data and metadata for all DMPs
Register_sensitive_research = pd.DataFrame()
Register_sensitive_research = pd.merge(Register_sensitive_research_temp, DMP_meta_data, on='id')

# Step 11: Use a list to create Faculty names (abbreviations) based on variations in the name
# provided in the text field Organisation. The list can be expanded if needed.
filename = "faculty_abb.csv"
pathfac = os.path.join(r'U:\Werk\Data Management\Python\Files\DMP_Online\DMP_stats', filename)
var_fac = pd.read_csv(pathfac)
# If needed a dictionary for the values can be made
dict_lookup = dict(zip(var_fac['Faculty_name'], var_fac['Abbrev']))

# Create a new variable for the abbreviated Faculty Name
Register_sensitive_research['Faculty_name'] = np.nan
# Create a variable that indicates how many faculties were selected for a DMP
Register_sensitive_research['Faculty_strcount'] = Register_sensitive_research['Faculty_situated'].str.count(' - ')

# Loop through the table and assign the Faculty Name (or Unknown) from the dictionary
# unless someone has marked mutiple faculties. In that case assign MultiFaculty
aux = Register_sensitive_research.copy()
for i, row in aux.iterrows():
    if row['Faculty_strcount'] > 1:
        Register_sensitive_research.loc[i, 'Faculty_name'] = 'MultiFaculty'
    elif row['Faculty_strcount'] < 1:
        Register_sensitive_research.loc[i, 'Faculty_name'] = 'Unknown'
    else:
        for Faculty_name, Abbrev in dict_lookup.items():
            if Faculty_name in row['Faculty_situated']:
                Register_sensitive_research.loc[i, 'Faculty_name'] = Abbrev
                break

# Drop variable Faculty_strcount
# Register_sensitive_research.drop(columns=['Faculty_strcount'])

# Step 12: Export the second overview
Register_sensitive_research.to_csv(f'U:\Werk\Data Management\Python\\Files\DMP_Online\DMP_stats\\'+runyear+'\\overview2\\registerlist_'+runday+'.csv', encoding='utf-8')

# Step 13: Combine the second overview with the first overview based on the older templates
# to create a single overview of all the dmps regardless of the templates

# Create a new folder for the combined list of all dmps for the register
Path('DMP_stats\\'+runyear+'\Final').mkdir(parents=True, exist_ok=True)

# Get the first list based on the older DMP templates
firstlist = pd.read_csv('U:\Werk\Data Management\Python\\Files\DMP_Online\DMP_stats\\'+runyear+'\\overview\\registerlist_'+runday+'.csv', index_col=0)

# Combine both lists
Register_sensitive_research_final = pd.concat([firstlist, Register_sensitive_research], ignore_index=True)
# Save the combined list in the specific "Final" folder
Register_sensitive_research_final.to_csv(f'U:\Werk\Data Management\Python\\Files\DMP_Online\DMP_stats\\'+runyear+'\\Final\\registerlist_'+runday+'.csv', encoding='utf-8')
