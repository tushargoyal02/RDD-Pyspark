#!/usr/bin/env python
# coding: utf-8

# In[35]:


from pyspark import SparkConf,SparkContext


# In[36]:


#rdd = sc.parallelize([1,2,3,4,5])


# In[37]:


#rdd.collect()


# In[38]:


#rdd.count()


# In[39]:


###master is set as local as we are keeping the spark on local machine
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram1")
sc.stop()


# In[40]:


sc = SparkContext(conf=conf)


# In[41]:


lines = sc.textFile("file:///home/tushar/SparkCourse/ml-100k/u.data")


# In[42]:


#WE are diving the dataset and splitting it on the basis of 3 column

# and new RDD is (RATINGS) created bcz of (LINES) RDD

ratings = lines.map(lambda x: x.split()[2])


# In[43]:


ratings.collect()


# In[50]:


# HERE countByKey is our action hence returning the (RESULT as [Python Object])

result = ratings.countByValue()


# In[51]:


import collections


# In[55]:


#we here used the collection package to sort key value pairs

sorted_values = collections.OrderedDict(sorted(result.items()))
print(sorted_values)


# In[49]:


for key,values in sorted_values.items():
    print("%s %i" % (key,values))


# In[ ]:




