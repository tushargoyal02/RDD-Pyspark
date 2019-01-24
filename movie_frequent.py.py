#!/usr/bin/env python
# coding: utf-8

# In[51]:


##
####    NUMBER OF TIMES THE MOVIES SEEN IN THE DATA BELOW

from pyspark import SparkConf,SparkContext
from pyspark.rdd import RDD


# In[52]:


conf = SparkConf().setMaster("local").setAppName("movie")


# In[53]:


sc.stop()


# In[54]:


sc = SparkContext(conf=conf)


# In[55]:


file = sc.textFile("/home/tushar/SparkCourse/ml-100k/u.data")


# In[56]:


rdd = file.map(lambda x: x.split()[1])


# In[57]:


rdd.collect()


# In[58]:


rdd1 = rdd.map(lambda x : (x,1))


# In[59]:


rdd1.collect()


# In[60]:


rdd2 = rdd1.reduceByKey(lambda x,y: x+y)
rdd2.collect()


# In[76]:


# WE ARE USING THE BELOW MAP TRANSFORMATION TO INTERCHANGE THE KEY VALUE PAIR OF RDD INTO NEW RDD

output = rdd2.map(lambda x: (x[1],x[0]))


# In[61]:


#output = rdd2.map( lambda x,y : (y,x)  )


# In[89]:


#result= output.collect()


# In[90]:


result = output.sortByKey().collect()


# In[94]:


print("THE NUMBER OF OCCURANCES FOR THE MOVIES WATCHED(MOVIE SEEN)")
for results in result:
    print("\n ", results)


# In[ ]:





# In[ ]:




