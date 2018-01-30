
# coding: utf-8

# In[2]:

from hdfs import *
client = Client("http://192.168.1.151:50070")


company_list = client.list("/edgarfiledata")
companies = []
datalist = []
#print(company_list)

#需要更多数据时扩大range范围即可
for i in range(1,10):
    if "10-K" in client.list("/edgarfiledata/"+company_list[i]):
        a = client.list("/edgarfiledata/"+company_list[i]+"/10-K")
    else:
        continue
    #print(a)
    b = client.list("/edgarfiledata/"+company_list[i]+"/10-K/"+a[-1])
        #print("--")
    #print(b)
    c = client.list("/edgarfiledata/"+company_list[i]+"/10-K/"+a[-1]+"/"+b[-1])
    # or 'impotentItems.txt'
    if 'item1Text.txt' in c:
        with client.read("/edgarfiledata/"+company_list[i]+"/10-K/"+a[-1]+"/"+b[-1]+"/"+"item1Text.txt", encoding = 'utf-8') as reader:
            data = reader.read()
            companies.append(company_list[i])
            datalist.append(data)
    #print(i)

print("Done")


# In[3]:

print(len(companies))


# In[6]:

import nltk,math
from textblob import TextBlob
import os,nltk,math,string
from collections import Counter
from nltk.corpus import brown
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import *
import re
import string
import time 

start = time.time()

def tf(word, count):
    return count[word] / sum(count.values())

def n_containing(word, countlist):
    return sum(1 for count in countlist if word in count)

def idf(word, countlist):
    return math.log(len(countlist) / (1 + n_containing(word, countlist)))

def tfidf(word, count, countlist):
    return tf(word, count) * idf(word, countlist)

def stem_tokens(tokens, stemmer):
    stemmed = []
    for item in tokens:
        stemmed.append(stemmer.stem(item))
    return stemmed

whole_phrase_list=[]
countlist=[]


for i in range(0,len(datalist)):
    
    
    
    wiki = TextBlob(datalist[i])
    phrases_list = list(wiki.noun_phrases)
    whole_phrase_list.append(phrases_list)
    
    divided_phrase = []
    for k in range(len(phrases_list)):
        divided_phrase.append(phrases_list[k].split())
    join_divided_phrase = sum(divided_phrase,[])
    total_words = word_tokenize(datalist[i])
    remained_words = list(set(total_words).difference(set(join_divided_phrase)))
    whole_used_words = phrases_list + remained_words
    
    
    count1 = Counter(whole_used_words)
    countlist.append(count1)
   

end =time.time()
print(end-start,"s")


# In[ ]:

import pandas as pd

dict_company = {}
for i in range(0,len(whole_phrase_list)):
    dict_company[companies[i]] = whole_phrase_list[i]
dfwords = pd.DataFrame.from_dict(dict_company, orient = 'index').transpose()
print(dfwords)


# # Tf-idf calculation

# In[9]:

for i, count in enumerate(countlist):
    print("Top words in {}".format(companies[i]))
    scores = {word: tfidf(word, count, countlist) for word in count}
    sorted_words = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    for word, score in sorted_words[:20]:
        print("\tWord: {}, TF-IDF: {}".format(word, round(score, 5)))

