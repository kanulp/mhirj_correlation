import pandas as pd
import RAKE
from fuzzywuzzy import fuzz 
from fuzzywuzzy import process 
import re

df = pd.read_csv("corr_data_test.csv", usecols=['mdc_ID', 'aircraftno','ATA_Description','LRU','CAS','MDC_MESSAGE','EQ_DESCRIPTION','ATA_Main','ATA_Sub','Discrepancy','Corrective Action', 'p_id', 'Value'],
                 skip_blank_lines=True, na_filter=True, encoding= 'unicode_escape').dropna(how = 'all')
pd.notnull(df["Value"])  
df["CAS"].fillna("None", inplace = True)  
df["Value"].fillna(0, inplace = True)  
df.sort_values(['aircraftno', 'p_id', 'mdc_ID'], ascending=[True, True, True])
df.loc[(df['ATA_Main'] != '33') & df['Value'] != 0]
result = df[df['mdc_ID'].isin([28919])]
result

myfile = "words.txt"
rake_object = RAKE.Rake(myfile)

def extract_keyword():
    df_keyword = pd.read_csv("keyword.csv")
    return  df_keyword

keyword_list = extract_keyword()

def keyword_match(mdc_message,keyword_list):
    ret_keyword = []
    for key in mdc_message:
        list_key = list(key) 
        perc_match = 0
        if not bool(re.match('[^\w]', list_key[0])):
            for k in keyword_list:
                if "###" in k :
                    split_array = k.split("###")
                    print(split_array)
                    for spr in split_array :
                        perc_match = fuzz.partial_ratio(spr.lower(), list_key[0])
                        if perc_match > 70:
                            ret_keyword.append(spr)
                    break
                else : 
                    perc_match = fuzz.partial_ratio(k.lower(), list_key[0])
                    print(perc_match, ' ', k.lower(), ' ', list_key[0])
                    if perc_match > 70:
                        ret_keyword.append(k)
                        break
        if ret_keyword : 
            break
   
    return ret_keyword
           

def keyword_match_descrepancy(pm_message,my_keyword):
    for key in pm_message:
        list_key = list(key) 
        perc_match = 0
        status=0
        if not bool(re.match('[^\w]', list_key[0])):
            for k in my_keyword :
                perc_match = fuzz.partial_ratio(k.lower(), list_key[0])
            if perc_match > 70 : 
                status = 3
                break
            else : 
                status = 1
    return status
            
for item in result.itertuples():
    cas = rake_object.run(item[7])
    lru = rake_object.run(item[4])
    eq_desc = rake_object.run(item[6])
    mdc_message = rake_object.run(item[5])
    ## pm
    descrepancy = rake_object.run(item[12])
    corrective_action = rake_object.run(item[13])

    mdc_keyword_list = []
    mdc_keyword_list = keyword_match(cas,keyword_list)
    mdc_keyword_list.extend(keyword_match(lru,keyword_list))
    mdc_keyword_list.extend(keyword_match(eq_desc,keyword_list))
    mdc_keyword_list.extend(keyword_match(mdc_message,keyword_list))

    if mdc_keyword_list:
        status =  keyword_match_descrepancy(descrepancy,mdc_keyword_list)
        print('status is ',status)
        if status ==  1 :
            status =  keyword_match_descrepancy(corrective_action,mdc_keyword_list)
        else : 
            print('match with eq_id')
    else:
        print("not found")

    # status =  keyword_match_descrepancy(descrepancy,my_keyword)
    print('status is ',status)
    # if status ==  1 :
    #     corrective_action = rake_object.run(item[11])
    #     status =  keyword_match_descrepancy(corrective_action.my_keyword)
    # else : 
    #     print('match with eq_id')
    
    # result.insert(loc=8, column='Status', value=status)
    
    #df["CAS"].fillna("None", inplace = True)  

    
    
