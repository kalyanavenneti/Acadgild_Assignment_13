
# coding: utf-8

# # Problem Statement
# 
# Read the following data set: https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data

# In[45]:


import pandas as pd 
import sqlite3 as sqlite3
import sqlalchemy as sqlalchemy


# In[46]:


#  read data set
url="https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data"
df_adultData= pd.read_csv(url, header=None, index_col=False)

# Add columns to data frame 

df_adultData.columns=['age','workclass','fnlwgt','education','education_num','marital_status','occupation','relationship','race','sex','capital_gain','capital_loss','hours_per_week','native_country','income']
df_adultData = df_adultData.apply(lambda x: x.str.strip() if x.dtype == "object" else x) # strip whitespace from dataframe columns
df_adultData.head(2)


# # Task

# In[47]:


# Question 1: Create an sqlalchemy engine using a sample from the data set

# Create data engine using sqlalchemy

from sqlalchemy import create_engine
engine = create_engine('sqlite:///sqlalchemydb.db', echo=False)


# Create the table ADULTS_DATA using engine

engine.execute('''
    CREATE TABLE IF NOT EXISTS ADULTS_DATA (
         age INTEGER,
         workclass VARCHAR(20),
         fnlwgt INTEGER,
         education VARCHAR(20),
         education_num INTEGER,         
         marital_status VARCHAR(30),         
         occupation VARCHAR(20),
         relationship VARCHAR(20),
         race VARCHAR(20),
         sex VARCHAR(10),
         capital_gain INTEGER,
         capital_loss INTEGER,
         hours_per_week INTEGER,
         native_country VARCHAR(30),
         income VARCHAR(10)
        
    )
''')

# connect to database using sqllite 3

connection = sqlite3.connect('sqlalchemydb.db')  
cursor = connection.cursor()


# In[61]:


# insert data into table ADULTS_DATA

insert_query = "INSERT INTO ADULTS_DATA (age, workclass, fnlwgt, education, education_num, marital_status, occupation, relationship, race, sex, capital_gain, capital_loss, hours_per_week, native_country, income) values (%d,'%s', %d, '%s', %d, '%s','%s','%s','%s','%s',%d,%d,%d,'%s','%s')"

for index, row in df_adultData.iterrows():
    connection.execute(insert_query % (row['age'], row['workclass'], row['fnlwgt'], row['education'],row['education_num'],row['marital_status'],row['occupation'],row['relationship'],row['race'],row['sex'],row['capital_gain'],row['capital_loss'],row['hours_per_week'],row['native_country'],row['income']))

connection.commit()

# Sample select  Query

# select 5 records from the ADULTS_DATA table
sql_select="SELECT * FROM ADULTS_DATA LIMIT 5;"
conn=engine.connect()
select_adult_data=pd.read_sql_query(sql_select, conn) 
print("Sample data from ADULTS_DATA table  ")
select_adult_data


# In[48]:


# Question 2: Write two basic update queries

# Update Query 1


# In[62]:


select_query_1="SELECT* FROM ADULTS_DATA WHERE workclass='?' LIMIT 2;"
conn=engine.connect()
adult_data_workclass_1=pd.read_sql_query(select_query_1,conn)
print("Data before update")
adult_data_workclass_1


# In[34]:


update_query_1 ="UPDATE ADULTS_DATA SET workclass='Not Available' ,occupation='Not Available' WHERE workclass='?' and occupation='?'"
print("Update Query 1:\n",update_query_1 , "\n")

# query execution in database
connection = sqlite3.connect('sqlalchemydb.db') 
cursor = connection.cursor()
connection.execute(update_query_1)
connection.commit()

# verification of data after change
query_1="SELECT * FROM ADULTS_DATA WHERE workclass='Not Available'  LIMIT 2;"
conn=engine.connect()
adult_data_workclass_1=pd.read_sql_query(query_1, conn) 
print("Data after update")
adult_data_workclass_1


# In[63]:


# Update Query 2

select_query_2="SELECT * FROM ADULTS_DATA WHERE workclass='Private'  LIMIT 2;"
conn=engine.connect()
adult_data_workclass_2=pd.read_sql_query(select_query_2, conn) 
print("Data before update")
adult_data_workclass_2


# In[51]:


update_query_2 ="UPDATE ADULTS_DATA SET workclass='Private Job'  WHERE workclass='Private' "
print("Update Query 2: ",update_query_2,"\n")

# query execution in database
connection = sqlite3.connect('sqlalchemydb.db') 
cursor = connection.cursor()
connection.execute(update_query_2)
connection.commit()

# verification of data after change
query_2="SELECT * FROM ADULTS_DATA WHERE workclass='Private Job'  LIMIT 2;"
conn=engine
adult_data_workclass_2=pd.read_sql_query(query_2, conn) 
print("Data after update")
adult_data_workclass_2


# In[52]:


# Question 3: Write two delete queries

# Delete Query 1

delete_query_1="Delete from ADULTS_DATA WHERE native_country='?';"
print("Delete Query 1 :\n  ", delete_query_1,"\n")

# records verification before delete 
query_2="SELECT * FROM ADULTS_DATA WHERE native_country='?';"
conn=engine
adult_data_Country=pd.read_sql_query(query_2, conn) 
print("No. of records where native_country= '?' before delete : ", adult_data_Country['native_country'][adult_data_Country.native_country=='?'].count() , '\n')

# Deletion of data
connection = sqlite3.connect('sqlalchemydb.db') 
cursor = connection.cursor()
connection.execute(delete_query_1)
connection.commit()

# records verification after delete 
print("No of records where native_country=' ?' after delete:  ", connection.total_changes)


# In[53]:


# DELETE Query 2

delete_query_2="Delete from ADULTS_DATA WHERE occupation='?';"
print("Delete Query 2 :\n", delete_query_2 , "\n")

# records verification before delete 
query_2="SELECT * FROM ADULTS_DATA WHERE occupation='?';"
conn=engine
adult_data_occupation=pd.read_sql_query(query_2, conn) 
print("No. of records where occupation= '?' before delete : ", adult_data_occupation['occupation'][adult_data_occupation.occupation=='?'].count() , '\n')

# Deletion of data
connection = sqlite3.connect('sqlalchemydb.db') 
cursor = connection.cursor()
connection.execute(delete_query_2)
connection.commit()
print("No of records where occupation=' ?' after delete:  ", connection.total_changes)


# In[54]:


# Question 4: Write two filter queries

# Filter query 1

filter_query_1="SELECT age,workclass,fnlwgt , education , education_num,marital_status ,sex ,race ,native_country  FROM ADULTS_DATA"
filter_query_1=filter_query_1+ " WHERE sex='Male'"
filter_query_1=filter_query_1+ " AND marital_status='Divorced'"
filter_query_1=filter_query_1+ " AND workclass='Private Job'"
filter_query_1=filter_query_1+ " AND age=39 "
filter_query_1=filter_query_1+ " AND race='White';"

print("Filter Query 1 :\n\t\n",filter_query_1)

# querying the filtered query into database

conn=engine
adult_data_occupation=pd.read_sql_query(filter_query_1, conn) 
print("\nFiltered Data")
adult_data_occupation.head(5)


# In[55]:


# Filter Query 2

filter_query_2="SELECT COUNT(*) AS Female_In_United_States FROM ADULTS_DATA WHERE Sex='Female' AND native_country='United-States'; "


print("Filter Query 2 :\n\t\n",filter_query_2)

conn=engine
adult_data_filter_2=pd.read_sql_query(filter_query_2, conn) 
print("\nFiltered Data")
adult_data_filter_2.head(5)


# In[56]:


# Question 5: Write two function queries

# Funtion Query 1

def get_country_based_data(country,no_of_rows):
    '''This function will return data from ADULTS_DATA table based upon native_country and no. of rows to be fetched'''
    connection = sqlite3.connect('sqlalchemydb.db');
    cursor = connection.cursor();
    query="SELECT * FROM ADULTS_DATA WHERE native_country='%s' LIMIT %d " %(country,no_of_rows);
    df_people  = pd.read_sql_query(query, connection) # Query the database and convert data into dataframe
    print("Data from ADULTS_DATA for Country='%s'" % country , "and No. of rows fetched: %d "% no_of_rows)
    return df_people


# In[57]:


country='United-States'
no_of_rows=5
# Function execution 
get_country_based_data(country,no_of_rows)


# In[58]:


# Function Query 2

def get_Gender_Count(country):
    '''This function will return Gender data from ADULTS_DATA table based upon native_country'''
    connection = sqlite3.connect('sqlalchemydb.db');
    cursor = connection.cursor();
    query="SELECT native_country AS Country,sex as Gender, count(*) as Count  FROM ADULTS_DATA WHERE native_country='%s' GROUP BY native_country,sex ; " %(country);
    df_Gender  = pd.read_sql_query(query, connection) # Query the database and convert data into dataframe
    print("Gender Data for Country='%s'" % country)
    return df_Gender


# In[59]:


country='Cuba'
# Function execution 
get_Gender_Count(country)

