# ---
# jupyter:
#   jupytext:
#     formats: py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
import pandas as pd
import numpy as np
# get the 2011-2012data
file = pd.read_sas('https://wwwn.cdc.gov/Nchs/Nhanes/2011-2012/DEMO_G.XPT')
file_11_12=pd.DataFrame(file)
file_11_12=file_11_12[["SEQN", "RIAGENDR", "RIDAGEYR", "RIDRETH3", 
                       "DMDEDUC2","DMDMARTL","RIDSTATR", "SDMVPSU", 
                       "SDMVSTRA", "WTMEC2YR","WTINT2YR"]]
year=[]
total=len(file_11_12.index)
for i in range(total):
    year.append("2011-2012")
file_11_12['year']= year

# get the 2013-2014data
file=pd.read_sas('https://wwwn.cdc.gov/Nchs/Nhanes/2013-2014/DEMO_H.XPT')
file_13_14=pd.DataFrame(file)
file_13_14=file_13_14[["SEQN", "RIAGENDR", "RIDAGEYR", "RIDRETH3", 
                       "DMDEDUC2","DMDMARTL","RIDSTATR", "SDMVPSU", 
                       "SDMVSTRA", "WTMEC2YR","WTINT2YR"]]
year=[]
total=len(file_13_14.index)
for i in range(total):
    year.append("2013-2014")
file_13_14['year']= year

# get the 2015-2016data
file=pd.read_sas('https://wwwn.cdc.gov/Nchs/Nhanes/2015-2016/DEMO_I.XPT')
file_15_16=pd.DataFrame(file)
file_15_16=file_15_16[["SEQN", "RIAGENDR", "RIDAGEYR", "RIDRETH3", 
                       "DMDEDUC2", "DMDMARTL","RIDSTATR", "SDMVPSU",
                       "SDMVSTRA", "WTMEC2YR","WTINT2YR"]]
year=[]
total=len(file_15_16.index)
for i in range(total):
    year.append("2015-2016")
file_15_16['year']= year

# get the 2017-2018data
file=pd.read_sas('https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/DEMO_J.XPT')
file_17_18=pd.DataFrame(file)
file_17_18=file_17_18[["SEQN", "RIAGENDR", "RIDAGEYR", "RIDRETH3", 
                       "DMDEDUC2","DMDMARTL","RIDSTATR", "SDMVPSU", 
                       "SDMVSTRA", "WTMEC2YR","WTINT2YR"]]
year=[]
total=len(file_17_18.index)
for i in range(total):
    year.append("2017-2018")
file_17_18['year']= year

# combine the file get change the vairbales name 
data_DEMO=pd.concat([file_11_12, file_13_14, file_15_16,file_17_18])
data_DEMO=data_DEMO.rename(columns={"SEQN": "ids", "RIAGENDR":"gender", 
                                    "RIDAGEYR": "age", "RIDRETH3":"race",
                            "DMDEDUC2":"education", "DMDMARTL":"marital"})
data_DEMO=data_DEMO.rename(columns=str.lower)

# change the data type
data_DEMO['ids'] = data_DEMO['ids'].astype('int64')
data_DEMO['age'] = data_DEMO['age'].astype('int64')
data_DEMO['gender'] = data_DEMO['gender'].astype('category')
data_DEMO['race'] = data_DEMO['race'].astype('category')
data_DEMO['education'] = data_DEMO['education'].astype('category')
data_DEMO['marital'] = data_DEMO['marital'].astype('category')
data_DEMO[['ridstatr','sdmvpsu','sdmvstra','wtmec2yr',
      'wtint2yr']]=data_DEMO[['ridstatr','sdmvpsu','sdmvstra',
                         'wtmec2yr','wtint2yr']].astype('int64')
data_DEMO['year'] = data_DEMO['year'].astype('category')

# save the dataframe as pickle
data_DEMO.to_pickle('DEMO.pkl')

# +
data_OHXDEN = pd.read_pickle('OHXDEN.pkl')
revised_DEMO = data_DEMO[['ids', 'gender', 'age', 'ridstatr']]

# add the under_20 if age < 20
check_age = []
for i in range(len(revised_DEMO)):
    age = revised_DEMO['age'].iloc[i]
    if age < 20:
        check_age.append("True")
    else:
        check_age.append("False")

# add the college - with two levels
college = []
for i in range(len(revised_DEMO)):
    age = data_DEMO['age'].iloc[i]
    college_score = data_DEMO['education'].iloc[i]
    if age >20 and ((college_score == 4) or (college_score == 5)):
        college.append("some college/college graduate")
    else:
        college.append('No college/<20')      
# put the check varaibles into the dataframe
ids = revised_DEMO['ids'].tolist()
add = {'ids': ids, 
        'under_20': check_age, 
        'college': college}  
df_add = pd.DataFrame(add)
revised_DEMO = pd.merge(revised_DEMO, df_add, on=['ids'], how='left')

# add the variable of ohx_status - (OHDDESTS)
OHDDESTS = data_OHXDEN[['ids', 'ohddests']]
revised_DEMO = pd.merge(revised_DEMO, OHDDESTS, on=['ids'], how='left')
revised_DEMO = revised_DEMO.rename(columns={'ridstatr': 'exam_status',
                                            'ohddests': 'ohx_status'})
ohx = []
# add the ohx
for i in range(len(revised_DEMO)):
    exam_status = revised_DEMO['exam_status'].iloc[i]
    ohx_status = revised_DEMO['ohx_status'].iloc[i]
    if exam_status == 2 and ohx_status == 1:
        ohx.append('complete')
    else:
        ohx.append('missing')
ids = revised_DEMO['ids'].tolist()
add = {'ids': ids, 
       'ohx': ohx}     
df_add = pd.DataFrame(add)
revised_DEMO = pd.merge(revised_DEMO, df_add, on=['ids'], how='left')

revised_DEMO['exam_status'] = revised_DEMO['exam_status'].astype('category')
revised_DEMO['college'] = revised_DEMO['college'].astype('category')
revised_DEMO['under_20'] = revised_DEMO['under_20'].astype('category')
revised_DEMO['ohx_status'] = revised_DEMO['ohx_status'].astype('category')
revised_DEMO['ohx'] = revised_DEMO['ohx'].astype('category')
# -

# calculate the number of the remain data
data_remain = revised_DEMO[revised_DEMO['exam_status']==2]
print(len(data_remain))
# calculate the number of the remove data
data_remove = revised_DEMO[revised_DEMO['exam_status']!=2]
print(len(data_remove))
