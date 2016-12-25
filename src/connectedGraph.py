import requests
import json
import re
import pandas as pd
from collections import Counter
from itertools import islice
host='https://search-movies-5zcbuwmhuftqplir3dnm72jd4a.us-east-1.es.amazonaws.com/complete_movies/_search?size=10000'

host_store='http://search-tweetymap-gurowqxu56ejw6kii5afincr3y.us-east-1.es.amazonaws.com/graph_data_new/connected_movies'

data=requests.get(host)
result=json.loads(data.text)
genre_list=['Action', 'Adventure', 'Animation', 'Adult', 'Biography', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Family',\
			'Fantasy', 'Film-Noir', 'Game-Show', 'History', 'Horror', 'Music', 'Musical', 'Mystery', 'News',\
			'Reality-TV', 'Romance', 'Sci-Fi', 'Short', 'Sport', 'Talk-Show', 'Thriller', 'War',	'Western']
#print(len(genre_list))
alldata=[]
movie_genre=[]

# collect movies here
for i in range( len(result['hits']['hits'])):
	try:
		key=result['hits']['hits'][int(i)]['_source']['key']
		genre=result['hits']['hits'][int(i)]['_source']['genres']
		alldata.append(key)
		movie_genre.append(genre)
	except Exception as e:

		pass

n_movies=len(alldata)

edges_all=[]
# make edges
for gen in range(len(genre_list)):
	movie_link=[]
	for movie in range(len(alldata)):
		if genre_list[gen] in movie_genre[movie]:
			movie_link.append(alldata[movie])
	
	l_movies=len(movie_link)
	for i in range(l_movies):
		for j in range((i+1),l_movies):
			edge=[movie_link[i], movie_link[j], genre_list[gen]]
			edges_all.append(edge)

alldata_df = pd.DataFrame(alldata)
edges_all_df = pd.DataFrame(edges_all)
finaldata={}
    # "Return first n items of the iterable as a list"

for i in alldata:
	connected_movies=edges_all_df.loc[edges_all_df[0] == i]
	df1 = connected_movies.ix[:,1]
	# print(df1)
	listofalldata=list(df1.values.flatten())
	# print(listofalldata)
	listofconnected={}
	word_counter = {}
	for word in listofalldata:
		if word in word_counter:
			word_counter[word] += 1
		else:
			word_counter[word] = 1
	popular_words = sorted(word_counter, key = word_counter.get, reverse = True)
	top_10 = popular_words[:10]
	print(i)
	listofconnected[i]=top_10
	#print(listofconnected[i])
	final_n={'movie':{i:listofconnected[i]}}
	#print(final_n)
	requests.post(host_store,json=final_n)



