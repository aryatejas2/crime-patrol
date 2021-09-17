#!/usr/bin/env python
# coding: utf-8

# <b>Loading separate data</b>

# In[1]:


import pandas as pd
import re
import warnings
warnings.filterwarnings('ignore')
data_la=pd.read_csv("Crime_Data_from_2010_to_2019.csv")
data_nyc=pd.read_csv("NYPD_Complaint_Data_Historic.csv")

data_la.head()
data_nyc.head()


# <b> Selecting columns to drop</b>

# In[2]:


def find_percentage_of_missing_values(df):
    print(df.isnull().sum()/len(df)*100)


# In[3]:


find_percentage_of_missing_values(data_la)


# In[4]:


find_percentage_of_missing_values(data_nyc)


# Thus we neglect the columns with high missing values and also the columns that are not common in either of the dataset and rename columns that are same

# In[5]:


list(data_nyc)


# In[6]:


data_la.rename(columns = {'Date Rptd':'DATE_REPORTED','DATE OCC':'DATE_OCCURRED','TIME OCC':'TIME_OCCURRED','AREA ':'PATROL_DIVISION','AREA NAME':'AREA_NAME','Crm Cd':'CRIME_CODE','Crm Cd Desc':'CRIME_DESCRIPTION','Vict Age':'VICTIM_AGE','Vict Sex':'VICTIM_SEX','Vict Descent':'VICTIM_RACE','Premis Desc':'PREMISE'},inplace = True)
data_nyc.rename(columns = {'RPT_DT':'DATE_REPORTED','CMPLNT_FR_DT':'DATE_OCCURRED','CMPLNT_FR_TM':'TIME_OCCURRED','ADDR_PCT_CD':'PATROL_DIVISION','BORO_NM':'AREA_NAME','KY_CD':'CRIME_CODE','OFNS_DESC':'CRIME_DESCRIPTION','VIC_AGE_GROUP':'VICTIM_AGE','VIC_SEX':'VICTIM_SEX','VIC_RACE':'VICTIM_RACE','PREM_TYP_DESC':'PREMISE','Latitude':'LAT','Longitude':'LON'},inplace = True)


# In[7]:


data_nyc.head()


# In[8]:


data_la.head()


# Changing date format in both datasets for columns "DATE_REPORTED" and "DATE_OCCURRED" to match a standard time format

# In[9]:


def change_date_format(val):
    if len(val)>10:
        split_val=val[:10].split('/')
    else:
        split_val=val.split('/')
    return split_val[2]+"-"+split_val[0]+"-"+split_val[1]


# <b>Appending target column to each dataframe which determines whether it is la or nyc & Selecting common attributes and merging them</b>

# In[10]:


data_la["TARGET"]="LA"
data_nyc["TARGET"]="NYC"
attributes_to_select=["DATE_REPORTED","DATE_OCCURRED","TIME_OCCURRED","PATROL_DIVISION","AREA_NAME","CRIME_CODE","CRIME_DESCRIPTION","VICTIM_AGE","VICTIM_SEX","VICTIM_RACE","PREMISE","LAT","LON","TARGET"]
data_la=data_la[attributes_to_select]
data_nyc=data_nyc[attributes_to_select]


data_nyc = data_nyc.dropna(how='any',axis=0) 
data_la = data_la.dropna(how='any',axis=0) 
data_nyc=data_nyc.reset_index()
data_la=data_la.reset_index()

f = lambda x: change_date_format(x)
data_nyc["DATE_REPORTED"] = data_nyc["DATE_REPORTED"].apply(f)
data_nyc["DATE_OCCURRED"] = data_nyc["DATE_OCCURRED"].apply(f)

data_la["DATE_REPORTED"] = data_la["DATE_REPORTED"].apply(f)
data_la["DATE_OCCURRED"] = data_la["DATE_OCCURRED"].apply(f)


data_nyc.head()



# In[11]:


data_la


# <b>Loading data in csv</b>

# In[12]:


data=pd.concat([data_la, data_nyc], ignore_index=True)
data.head()


# In[28]:


data = pd.read_csv("raw_data.csv")
data = data.dropna(how='any',axis=0) 


# <b> Drop column </b>

# In[29]:


from sklearn import preprocessing
le = preprocessing.LabelEncoder()
col="VICTIM_RACE"
print(data["VICTIM_RACE"].value_counts())
le.fit(data[col])
data[col]=le.transform(data[col])
classes=le.classes_
transformed_class=le.transform(le.classes_)
sns.boxplot(x=data[col])


# In[13]:


data.drop(data.columns[0], axis=1, inplace=True)


# In[14]:


data


# <b>Cleaning attributes</b>
# 

# <b>TIME_OCCURRED</b>: is in a "hh:mm:ss" format for the new york dataset whereas military time format for LA dataset.
# Converting the time to hh:mm:yy format all rows

# In[15]:


def split_time(time):
    if type(time)==type(""):
        return "".join(time.split(":")[:-1])
    else:
        return time
f = lambda x: split_time(x)
data.loc[data["TARGET"] == "NYC", 'TIME_OCCURRED'] = data["TIME_OCCURRED"].apply(f)
data
# for index,val in enumerate(data["TIME_OCCURRED"]):
#     if data["TARGET"]=="NYC":
#         data["TIME_OCCURRED"][index]=re.sub('^(?:(?:([01]?\d|2[0-3]):)?([0-5]?\d):)?([0-5]?\d)$', "".join(val.split(":")[:-1]), val)


# Removing rows where value is blank

# In[16]:


data=data[data["TIME_OCCURRED"]!='']
data=data.reset_index()


# In[17]:


def convert_to_hh_mm_ss(val):
    if len(val)==1:
        return "00:0"+val+":00"
    elif len(val)==2:
        return "00:"+val+":00"
    elif len(val)==3:
        return "0"+val[0]+":"+val[1:]+":00"
    else:
        return val[0:2]+":"+val[2:]+":00"

f = lambda x: convert_to_hh_mm_ss(str(x))

data["TIME_OCCURRED"] = data["TIME_OCCURRED"].apply(f)


# In[18]:


data["TIME_OCCURRED"]


# <b>PATROL DIVISION</b>: These numbers belong to the respective precincts, they are float values, would be converted to integers
# Same is the case for <b>CRIME_CODE</b>
# 

# In[19]:


data['PATROL_DIVISION'] = data['PATROL_DIVISION'].astype('Int64')
data['CRIME_CODE'] = data['CRIME_CODE'].astype('Int64')


# In[35]:


data.head()


# <b>VICTIM_AGE</b> was converted to range values, as NY database consisted of range and LA database consisted to absolute values , replacing the unknown values of this column with the mode

# In[21]:


from collections import defaultdict
import operator

def range_from_age(age):
    if age==0 or age is None:
        return "UNKNOWN"
    if age<18:
        return "<18"
    if age > 18 and age <=24:
        return "18-24"
    if age >24 and age <=44:
        return "25-44"
    if age >44 and age <=65:
        return "45-65"
    else:
        return "65+"


def get_age(val):
    if type(val)==str:
        if str.isdigit(val):
            return range_from_age(int(val))
        else:
            return "UNKNOWN"
    else:
        return "UNKNOWN"
    
f = lambda x: get_age(str(x))

data["VICTIM_AGE"] = data["VICTIM_AGE"].apply(f)
    
data    


# In[45]:


data


# In[22]:


def increment_dict(x,dictionary):
    dictionary[x]+=1
    return x

def replace_unknown_with_mode(column_name,unknown_value_name,replace_by_str=None):
    d=defaultdict(int)
    f = lambda x: increment_dict(x,d)
    
    data[column_name]=data[column_name].apply(f)
    
    d[unknown_value_name]=0
    if replace_by_str:
        mode_val=replace_by_str
    else:
        mode_val= max(d.items(), key=operator.itemgetter(1))[0]
    data[column_name]=data[column_name].replace([unknown_value_name],mode_val)

replace_unknown_with_mode("VICTIM_AGE","UNKNOWN")


# <b>VICTIM_SEX</b> has a defaut value for unknown which is "D" , the current default value is "E" for unknown

# In[23]:


def check_for_unknown(val):
    if val =="D":
        return "E"
    else:
        return val
    
f = lambda x: check_for_unknown(x)
data["VICTIM_SEX"]=data["VICTIM_SEX"].apply(f)

    
data["VICTIM_SEX"]


# In[24]:


replace_unknown_with_mode("VICTIM_SEX","E")
replace_unknown_with_mode("VICTIM_SEX","-")
replace_unknown_with_mode("VICTIM_SEX","X")
replace_unknown_with_mode("VICTIM_SEX","H")
replace_unknown_with_mode("VICTIM_SEX","N")


# In[25]:


data["VICTIM_SEX"].value_counts()


# <b>VICTIM_RACE</b> is menioned in words for NY dataset whereas LA dataset provides a character to word mapping, hence exapnding the map to get appropriate race

# In[26]:


def map_to_race(string,char_to_descent_map):
    if len(string.lstrip().rstrip())==1:
        if string in char_to_descent_map.keys():
            return char_to_descent_map[string]
        else:
            return char_to_descent_map["X"]
    else:
        if string=="American indian/alaskan native":
            return "American Indian/Alaskan Native"
        return string.capitalize()
    
char_to_descent_map={"A":"Other Asian", "B": "Black", "C" : "Chinese", "D" : "Cambodian", "F": "Filipino","G" :"Guamanian" ,"H":"Hispanic/Latin/Mexican" ,"I": "American Indian/Alaskan Native" ,"J": "Japanese", "K":"Korean", "L" :"Laotian" ,"O" : "Other", "P": "Pacific Islander", "S": "Samoan", "U": "Hawaiian","V": "Vietnamese", "W" : "White" ,"X": "Unknown" ,"Z" :"Asian Indian"}

f = lambda x: map_to_race(x,char_to_descent_map)

data["VICTIM_RACE"]=data["VICTIM_RACE"].apply(f)
    


# In[27]:


replace_unknown_with_mode("VICTIM_RACE","Unknown","Other")


# In[28]:


data["VICTIM_RACE"].value_counts()


# <b> Drop missing values </b>

# In[29]:



data = data.dropna(how='any',axis=0) 
data["TARGET"].value_counts()


# In[30]:


[n_max,n_min]=data["TARGET"].value_counts()

# Sample the data with n value and shuffle all rows and return
def sample_data(n):
    df1 = data[data['TARGET']=="NYC"].sample(n=n)
    df0 = data[data['TARGET']=="LA"].sample(n=n,replace=True)
    return pd.concat([df1,df0]).sample(frac=1)


# <b> Oversampling the data <b>

# In[31]:


data_oversampled=sample_data(n_max)
data_oversampled=data_oversampled.reset_index()
del data_oversampled["index"]
del data_oversampled["level_0"]
data_oversampled["TARGET"].value_counts()


# In[32]:


data_oversampled.columns


# <b> Undersampling the data <b>

# In[33]:


data_undersampled=sample_data(n_min)
data_undersampled=data_undersampled.reset_index()
del data_undersampled["index"]
data_undersampled["TARGET"].value_counts()
del data_undersampled["level_0"]
data_undersampled


# <b>Data with 100k rows<b>

# <b>Saving the data to a file</b>

# In[268]:


data_oversampled.to_csv("cleaned_data_oversampled.csv")
data_undersampled.to_csv("cleaned_data_undersampled.csv")
data_100k.to_csv("cleaned_data_100k.csv")


# <b>Boxplot of categorical and numerical values<b>

# In[38]:


df_to_work=data.drop(['index','TARGET',"DATE_REPORTED",'DATE_OCCURRED','TIME_OCCURRED','CRIME_DESCRIPTION','PREMISE'],axis=1)


# In[39]:


from sklearn import preprocessing
le = preprocessing.LabelEncoder()


# In[40]:


df_to_work


# In[41]:


df_to_work["VICTIM_AGE"].value_counts()


# In[44]:


import seaborn as sns
sns.set(style="whitegrid")
col="PATROL_DIVISION"
sns.boxplot(x=df_to_work[df_to_work['PATROL_DIVISION'])


# In[273]:


# sns.boxplot(x=df_to_work['CRIME_CODE'])
col="CRIME_CODE"
sns.boxplot(x=df_to_work[col])


# In[274]:


col="AREA_NAME"
le.fit(df_to_work[col])
df_to_work[col]=le.transform(df_to_work[col])
classes=le.classes_
transformed_class=le.transform(le.classes_)
for i in range(len(classes)):
    print(transformed_class[i]," -> represents -> ",classes[i])
sns.boxplot(x=df_to_work[col])


# In[275]:


col="VICTIM_AGE"
le.fit(df_to_work[col])
df_to_work[col]=le.transform(df_to_work[col])
classes=le.classes_
transformed_class=le.transform(le.classes_)
for i in range(len(classes)):
    print(transformed_class[i]," -> represents -> ",classes[i])
sns.boxplot(x=df_to_work[col])


# In[276]:


# sns.boxplot(x=df_to_work['CRIME_CODE'])
col="VICTIM_SEX"
le.fit(df_to_work[col])
df_to_work[col]=le.transform(df_to_work[col])
classes=le.classes_
transformed_class=le.transform(le.classes_)
for i in range(len(classes)):
    print(transformed_class[i]," -> represents -> ",classes[i])
sns.boxplot(x=df_to_work[col])


# In[277]:


# sns.boxplot(x=df_to_work['CRIME_CODE'])
col="VICTIM_RACE"
le.fit(df_to_work[col])
df_to_work[col]=le.transform(df_to_work[col])
classes=le.classes_
transformed_class=le.transform(le.classes_)
for i in range(len(classes)):
    print(transformed_class[i]," -> represents -> ",classes[i])
sns.boxplot(x=df_to_work[col])

