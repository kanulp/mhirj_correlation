import pandas as pd
import RAKE
from fuzzywuzzy import fuzz 
from fuzzywuzzy import process 
import re

df = pd.read_csv("corr_data_test.csv", usecols=['mdc_ID', 'aircraftno','ATA_Description','LRU','CAS','ATA_Main','ATA_Sub','Discrepancy','Corrective Action', 'p_id', 'Value','EQ_DESCRIPTION','MDC_MESSAGE'],
                 skip_blank_lines=True, na_filter=True, encoding= 'unicode_escape').dropna(how = 'all')
pd.notnull(df["Value"])  
df["CAS"].fillna("None", inplace = True)  
df["Value"].fillna(0, inplace = True)  
df.sort_values(['aircraftno', 'p_id', 'mdc_ID'], ascending=[True, True, True])
df.loc[(df['ATA_Main'] != '33') & df['Value'] != 0]
result = df[df['mdc_ID'].isin([28919])]

myfile = "words.txt"
rake_object = RAKE.Rake(myfile)

def extract_keyword():
    df_keyword = pd.read_csv("keyword.csv")
    return  df_keyword

keyword_list = extract_keyword()

def keyword_match(mdc_keyword,keyword_list):
    for key in mdc_keyword:
        list_key = list(key) 
        perc_match = 0
        ret_keyword =''
        if not bool(re.match('[^\w]', list_key[0])):
            for k in keyword_list:
                if "###" in k :
                    split_array = k.split("###")
                perc_match = fuzz.partial_ratio(k.lower(), list_key[0])
                if perc_match > 60:
                    ret_keyword =  k
                    break
                else:
                    ret_keyword = ""
            return ret_keyword
           

def keyword_match_descrepancy(pm_message,my_keyword):
    for key in pm_message:
        list_key = list(key) 
        perc_match = 0
        status=0
        if not bool(re.match('[^\w]', list_key[0])):
            perc_match = fuzz.partial_ratio(my_keyword.lower(), list_key[0])
            if perc_match > 60 : 
                status = 3
                break
            else : 
                status = 1
    return status
            
for item in result.itertuples():
    cas = rake_object.run(item[5])
    descrepancy = rake_object.run(item[10])
   
    my_keyword = keyword_match(cas,keyword_list)
    if my_keyword !=  "" :
        print('found')
    else:
        lru = rake_object.run(item[4])
        my_keyword =  keyword_match(lru,keyword_list)
    print('my keyword ',my_keyword)

    status =  keyword_match_descrepancy(descrepancy,my_keyword)
    print('status is ',status)
    if status ==  1 :
        corrective_action = rake_object.run(item[11])
        status =  keyword_match_descrepancy(corrective_action.my_keyword)
    else : 
        print('match with eq_id')
    
    result.insert(loc=8, column='Status', value=status)
    
    #df["CAS"].fillna("None", inplace = True)  

    
    
