import pandas as pd
import RAKE
from fuzzywuzzy import fuzz 
from fuzzywuzzy import process 
import re
import datetime

print(datetime.datetime.now())
df = pd.read_csv("corr_test_2.csv", usecols=['mdc_ID', 'aircraftno','ATA_Description','LRU','CAS','MDC_MESSAGE','EQ_DESCRIPTION','ATA_Main','ATA_Sub','Discrepancy','Corrective Action', 'p_id', 'Value'],
                 skip_blank_lines=True, na_filter=True, encoding= 'unicode_escape').dropna(how = 'all')
pd.notnull(df["Value"])  
df["CAS"].fillna("None", inplace = True)  
df["Value"].fillna(0, inplace = True)  
df.sort_values(['aircraftno', 'p_id', 'mdc_ID'], ascending=[True, True, True])
result = df.loc[(df['ATA_Main'] != '33') & df['Value'] != 0]
result = df[df['mdc_ID'].isin([66860])]


myfile = "words.txt"
rake_object = RAKE.Rake(myfile)


def extract_keyword():
    df_keyword = pd.read_csv("keyword.csv")
    return  df_keyword

keyword_list = extract_keyword()

def get_keyword_without_rake(str):
    ret_keyword = []
    str = re.findall(r"[\w']+", str)
    for key in str:
        list_key = list(key) 
        perc_match = 0
        if not bool(re.match('[^\w]', list_key[0])):
            ret_keyword.append(key)
    return ret_keyword

def keyword_match(mdc_message,keyword_list):
    ret_keyword = []
    for key in mdc_message:
        list_key = list(key) 
        perc_match = 0
        if not bool(re.match('[^\w]', list_key[0])):
            for k in keyword_list:
                if not bool(re.match('[^\w]', k)):
                    if "###" in k :
                        split_array = k.split("###")
                        for spr in split_array :
                            perc_match = fuzz.partial_ratio(spr.lower(), list_key[0])
                            if perc_match > 75:
                                ret_keyword.extend(split_array)
                        break
                    else : 
                        perc_match = fuzz.partial_ratio(k.lower(), list_key[0])
                        if perc_match > 75:
                            ret_keyword.append(k)
                            break
        if ret_keyword : 
            break
   
    return ret_keyword
          

def keyword_match_pm_messages(pm_message,my_keyword):
    for key in pm_message:
        list_key = list(key) 
        perc_match = 0
        count=0
        avg_perc_match = 0
        if not bool(re.match('[^\w]', list_key[0])):
            for k in my_keyword :
                print(k, ' ', list_key[0])
                perc_match = fuzz.partial_ratio(k.lower(), list_key[0])
                if perc_match > 90 :
                    return perc_match
                elif perc_match >= 70 : 
                    count = count + 1
                    avg_perc_match += perc_match
                else:
                    return 0     

    avg_perc_match =  (avg_perc_match/count)         
    return avg_perc_match   

def add_corr_status(discp, corr_ac):      
    status = 0
    if ((' ' + 'fom' + ' ') in (' ' + discp.lower() + ' ') or 
            (' ' + 'further other maintainence' + ' ') in (' ' + discp.lower() + ' ') or  
                (' ' + 'to get access' + ' ') in (' ' + discp.lower() + ' ') or
                     (' ' + 'troubleshooting required' + ' ') in (' ' + discp.lower() + ' ') or 
                          (' ' + 'mci' + ' ') in (' ' + discp.lower() + ' ')):
       status = 2
    elif  ((' ' + 'fom' + ' ') in (' ' + corr_ac.lower() + ' ') or 
            (' ' + 'further other maintainence' + ' ') in (' ' + corr_ac.lower() + ' ') or  
                (' ' + 'to get access' + ' ') in (' ' + corr_ac.lower() + ' ') or
                     (' ' + 'troubleshooting required' + ' ') in (' ' + corr_ac.lower() + ' ') or 
                          (' ' + 'mci' + ' ') in (' ' + corr_ac.lower() + ' ')):
        status = 2
    else:
        status = 1   
    return status     
   
            
for item in result.itertuples():
    status = 0
    cas = ''
    if not item[7] == 'None':
        cas = rake_object.run(item[7])
        if not cas:
            cas = get_keyword_without_rake(item[7])

    lru = rake_object.run(item[4])
    if not lru:
        lru = get_keyword_without_rake(item[4])
    
    eq_desc = rake_object.run(item[6])
    if not eq_desc:
        eq_desc = get_keyword_without_rake(item[6])

    mdc_message = rake_object.run(item[5])
    list_mdc_message = []
    if not mdc_message:
        mdc_message = get_keyword_without_rake(item[5])
    for mdc in mdc_message:    
        list_mdc_message.append(mdc[0])
    ## pm
    pm_full_desc = item[12].replace('"','')
    pm_full_corr_acc = item[13].replace('"','')

    descrepancy = rake_object.run(pm_full_desc)
    if not descrepancy:
        descrepancy = get_keyword_without_rake(pm_full_desc)
    
    corrective_action = rake_object.run(pm_full_corr_acc)
    if not corrective_action:
        corrective_action = get_keyword_without_rake(corrective_action)

    mdc_keyword_list = []
    if not item[7] == 'None':
        mdc_keyword_list = keyword_match(cas,keyword_list)
    mdc_keyword_list.extend(keyword_match(lru,keyword_list))
    mdc_keyword_list.extend(keyword_match(eq_desc,keyword_list))
    mdc_keyword_list = list(dict.fromkeys(mdc_keyword_list))
    
    status_list =[]
    perc_match_list = []
    if mdc_keyword_list:
        perc_match =  keyword_match_pm_messages(descrepancy,mdc_keyword_list)
        if perc_match >  90:
            status = add_corr_status(pm_full_desc, pm_full_corr_acc)
            status_list.append(status)
            perc_match_list.append(perc_match)
            # result.insert(loc=item[0], column='Status', value=status)
            # result.insert(loc=item[0], column='perc match', value=perc_match)
        else: 
            perc_match =  keyword_match_pm_messages(descrepancy,list_mdc_message)
            if perc_match != 0 :
                status = add_corr_status(pm_full_desc, pm_full_corr_acc)
                status_list.append(status)
                perc_match_list.append(perc_match)
                # result.insert(loc=item[0], column='Status', value=status)
                # result.insert(loc=item[0], column='perc match', value=perc_match)
            else: 
                perc_match =  keyword_match_pm_messages(corrective_action,mdc_keyword_list)
                if perc_match >  90 :
                    status = add_corr_status(pm_full_desc, pm_full_corr_acc)
                    status_list.append(status)
                    perc_match_list.append(perc_match)
                    # result.insert(loc=item[0], column='Status', value=status)
                    # result.insert(loc=item[0], column='perc match', value=perc_match)
                else:
                    perc_match =  keyword_match_pm_messages(corrective_action,list_mdc_message)
                    if perc_match != 0 :
                        status = add_corr_status(pm_full_desc, pm_full_corr_acc)
                        status_list.append(status)
                        perc_match_list.append(perc_match)
                        # result.insert(loc=item[0], column='Status', value=status)
                        # result.insert(loc=item[0], column='perc match', value=perc_match)
                    else:
                        status_list.append(status)
                        perc_match_list.append(0)
                        # result.insert(loc=item[0], column='Status', value=1)
                        # result.insert(loc=item[0], column='perc match', value=perc_match)
    else:
        mdc_msg_keyword_list = []
        if not item[7] == 'None':
            mdc_msg_keyword_list.extend(cas)
        mdc_msg_keyword_list.extend(lru)
        mdc_msg_keyword_list.extend(eq_desc)

        perc_match =  keyword_match_pm_messages(descrepancy,mdc_msg_keyword_list)

        if perc_match >  90 :
            status = add_corr_status(pm_full_desc, pm_full_corr_acc)
            status_list.append(status)
            perc_match_list.append(perc_match)
            # result.insert(loc=item[0], column='Status', value=status)
            # result.insert(loc=item[0], column='perc match', value=perc_match)
        else: 
            perc_match =  keyword_match_pm_messages(descrepancy,list_mdc_message)
            if perc_match != 0 :
                status = add_corr_status(pm_full_desc, pm_full_corr_acc)
                status_list.append(status)
                perc_match_list.append(perc_match)
                # result.insert(loc=item[0], column='Status', value=status)
                # result.insert(loc=item[0], column='perc match', value=perc_match)
            else: 
                perc_match =  keyword_match_pm_messages(corrective_action,mdc_keyword_list)
                if perc_match >  90 :
                    status = add_corr_status(pm_full_desc, pm_full_corr_acc)
                    status_list.append(status)
                    perc_match_list.append(perc_match)
                    # result.insert(loc=item[0], column='Status', value=status)
                    # result.insert(loc=item[0], column='perc match', value=perc_match)
                else:
                    perc_match =  keyword_match_pm_messages(corrective_action,list_mdc_message)
                    if perc_match != 0 :
                        status = add_corr_status(pm_full_desc, pm_full_corr_acc)
                        status_list.append(status)
                        perc_match_list.append(perc_match)
                        # result.insert(loc=item[0], column='Status', value=status)
                        # result.insert(loc=item[0], column='perc match', value=perc_match)
                    else:
                        status_list.append(status)
                        perc_match_list.append(0)
                        # result.insert(loc=item[0], column='Status', value=1)
                        # result.insert(loc=item[0], column='perc match', value=perc_match)

result['Status'] = status_list
result['Percentage Match'] = perc_match_list
result.to_csv(r'G:\Capstone\mhirj_correlation\test.csv', index = False)
print(datetime.datetime.now())
