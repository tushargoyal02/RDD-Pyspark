#!/usr/bin/env python
# coding: utf-8

# In[38]:


from pyspark import SparkConf,SparkContext


# In[39]:


conf = SparkConf().setMaster("local").setAppName("Friends_onAge")
sc.stop()


# In[40]:


sc = SparkContext(conf=conf)


# In[41]:


lines = sc.textFile("file:///home/tushar/SparkCourse/fakefriends.csv")


# In[48]:


def parseline(lines):
 # NOW SPLITTING THE DATA ACCORDING TO (,)
#AND AFTER WORDS TAKING the AGE AND FRIENDS VALUES IN THE CODE BELOW
    fields = lines.split(',')
    age = int(fields[2])
    friends = int(fields[3])
    return (age,friends)
    



# In[49]:


rdd = lines.map(parseline)


# In[50]:


rdd.collect()


# In[51]:


#### ITS IS USE TO PARTITION KEY FROM THE VALUES TO GENERATE THE NUMBER OF FRIENDS FOR PARTICULAR AGE!

totalByage =  rdd.mapValues(lambda x : (x,1))


# In[52]:


totalByage.collect()


# In[58]:


####
### WE ARE USING THE (REDUCEBYKEY) ACTION  TO ADD ALL UNIQUE KEYS 

        # all the previous values of (key) are added to occur with multiple times
    
                #(33, (385, 1)  (33, (2, 1)
                    #on applying reduce key we get (33,(387,2))

totalByage = rdd.mapValues(lambda x :(x,1)).reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1]))


# In[59]:


totalByage.collect()


# In[60]:


# in this we are calculating the average friends per age 

NumberOfFriendsByAge = totalByage.mapValues(lambda x : x[0] / x[1] )


# In[62]:


results = NumberOfFriendsByAge.collect()


# In[64]:


for result in results:
    print("\n",result)


# In[ ]:




