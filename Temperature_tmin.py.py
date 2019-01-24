#!/usr/bin/env python
# coding: utf-8

# In[1]:


# FINDING MINIMUM TEMPERATURE BY STATION ID
from pyspark import SparkConf,SparkContext


# In[4]:


conf = SparkConf().setMaster("local").setAppName("Temperature")
sc.stop()


# In[5]:


sc = SparkContext(conf = conf)


# In[16]:


lines = sc.textFile("file:///home/tushar/SparkCourse/1800.csv")
def parseLine(lines):
    fields = lines.split(",")
    stationId = str(fields[0])
    TempEntry = str(fields [2])
    temperature = float(fields[3])
    return (stationId,TempEntry,temperature)


# In[17]:


parsedLines = lines.map(parseLine)


# In[18]:


parsedLines.collect()


# In[24]:


####
### HERE WE ARE COLLECTING THE TMIN VALUES OF COLUMN FROM THE BELOW CODE BY USING ACTION(FILTER)
#
#   FILTER JUST RETRUN THE BOOLEAN VALUES

minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])


# In[26]:


minTemps.collect()


# In[28]:


###
# WE ARE CREATING THE KEY VALUE PAIRS FOR (STATION ID , TEMPERATURE)

newRdd = minTemps.map(lambda x : (x[0],x[2]))


# In[29]:


newRdd.collect()


# In[30]:


# WE ARE AGGREGATING for every STATION NAME BY ITS TEMPERATURE
rdd2 = newRdd.reduceByKey(lambda x,y : min(x,y))


# In[49]:


result = rdd2.collect()


# In[50]:


for results in int result1:
    print("The station",result[0], "minimum temperature are:",result[1])


# In[ ]:




