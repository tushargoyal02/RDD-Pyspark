#!/usr/bin/env python
# coding: utf-8

# In[129]:


# POPULAR HERO NAME WITH OTHER HERO


from pyspark import SparkConf,SparkContext


# In[130]:


conf = SparkConf().setMaster("local").setAppName("marvel")


# In[131]:


sc.stop()


# In[132]:


sc = SparkContext(conf = conf)


# In[133]:


def func1(line):
    element = line.split()
    return(element[0],len(element)-1)


# In[134]:


def func2(line):
    element = line.split('\"')
    
    #CONVERTING THE GIVEN NAME IN DATASET IN UTF-8 FORM
    return(int(element[0]),element[1].encode("utf8"))


# In[135]:


lines1 = sc.textFile("/home/tushar/SparkCourse/4.1 Marvel Graph.txt")


# In[136]:


rdd1 = lines1.map(func1)


# In[137]:


lines2 = sc.textFile("/home/tushar/SparkCourse/4.3 Marvel Names.txt")


# In[138]:


rdd2 = lines2.map(func2)


# 

# In[139]:


#rdd1.collect()

########################## [WE ARE REDUCING MULTIPLE OCCURANCE OF A CHAACTE ID]
friendsCharacter = rdd1.reduceByKey(lambda x,y: x+y)


# In[140]:


#rdd2.collect()


# In[141]:


# FLIPPING THE VALUE OF COUNT AND ID IN BELOW
flipped = friendsCharacter.map(lambda x: (x[1],x[0]))
    


# In[142]:


#IT IS GIVING MAX NUMBER OF COUNT FOR THE CHARACTER ID'S
mostpopular = flipped.max()


# In[187]:


print(mostpopular)
popularcount = str(mostpopular[0])


# In[188]:


rdd2.collect()

#mostPopularName = rdd2.lookup(mostpopular[0])


# In[177]:


out = rdd2.filter(lambda x : 859 in x )
name1= out.collect()
print(name1)
#print(mostPopularName)


# In[197]:


popularname = name1[0][1]
print(str(popularname))


# In[ ]:





# In[198]:


print(popularname ,"is the most popular hero with",popularcount )


# In[ ]:




