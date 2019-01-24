#!/usr/bin/env python
# coding: utf-8

# In[22]:


import re

from pyspark import SparkConf,SparkContext


# In[23]:


conf = SparkConf().setMaster("local").setAppName("CountingWords")


# In[24]:


sc.stop()


# In[25]:


sc = SparkContext(conf=conf)


# In[ ]:





# In[26]:


# WE ARE DIVING THE VARIABLES OR WORDS ON THE BASISCS OF SPECIAL CHARACTER 

# HERE IN THE CODE BELOW (W+) MEAN WE ARE SEARCHING FOR EACH WORD WITH THE CHARACTER
rdd = sc.textFile("file:///home/tushar/SparkCourse/9.2 Book.txt")
def NormaliseWords(lines):
    return re.compile(r'\W+', re.UNICODE).split(lines.lower())


# In[27]:


newrdd = rdd.flatMap(NormaliseWords)


# In[28]:


newrdd.collect()


# In[29]:





# PRINTING THE WORDSCOUNT BELOW WILL GIVE A DICTONARY OF HOW MANY WORDS ARE THERE ABOVE!
wordscount = newrdd.map(lambda x :(x, 1)).reduceByKey(lambda x, y : x + y)


# In[ ]:





# In[51]:


#sortedValue = wordscount.map(lambda x ,y : (y,x))


# In[52]:


#sortedValue = wordscount.map(lambda x : x.swap())


# In[53]:


#results3 = sortedValue.collect()


# In[ ]:





# In[ ]:




