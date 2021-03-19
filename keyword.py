import pandas as pd
import json


# get only the columns you want from the csv file
df = pd.read_csv('data.csv', usecols=['mdc_ID', 'aircraftno','ATA_Description','LRU','CAS','ATA_Main','ATA_Sub','Discrepancy','Corrective Action'])
result = df.to_dict(orient='records')
print(json.dumps(result, indent = 4))

#print(df.head(5))
