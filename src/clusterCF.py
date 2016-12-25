from __future__ import print_function
import sys
import time
import numpy as np
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans
from pyspark.mllib.recommendation import ALS
import math

def getValList(line):
	arr=line.strip().split(",")
	timee=int(str(arr[1]))
	timeDiff=currTime.value-timee
	index=1
	weekDiff=86400*7
	monthDiff=86400*30
	if timeDiff>=weekDiff and timeDiff<monthDiff :
		index=0.75
	elif timeDiff>=monthDiff and timeDiff<3*monthDiff :
		index=0.5
	elif timeDiff>=3*monthDiff and timeDiff<12*monthDiff :
		index=0.25
	else :
		index=0.1
	myList=[None]*27
	for i in range(4,len(arr)):
		myList[i-4]=float(arr[i])*index
	return str(myList)
	
def addLists(l1,l2):
	list3=[]
	list1=eval(str(l1))
	list2=eval(str(l2))
	for index in range(0,len(list1)):
		list3.append(list1[index]+list2[index])
	return str(list3)

def getKey(x):
	val=x[1]
	cluster=val[0]
	return cluster
	
def getVal(x):
	user=x[0]
	val=x[1]
	movie=val[1]
	list=[]
	list.append(user)
	list.append(movie)
	list.append(1)
	return str(list)
	
def getMovieId(x):
	val=x[1]
	movie=val[0]
	return movie
	
def getUserIndex(x):
	val=x[1]
	uIndex=val[1]
	return uIndex
	
def getUserIndex2(x):
	val=x[1]
	uIndex=val[0]
	return uIndex
	
def getMovieIndex(x):
	val=x[1]
	mIndex=val[1]
	return mIndex


if __name__ == "__main__":
	#Create SparkContext 
	sc = SparkContext(appName="PythonTest1")
	#Read first file: course data
	allData = sc.textFile("s3n://clickstreamdata-cloud/data/*.txt")
	curr = int(round(time.time()))
	currTime=sc.broadcast(curr)
	movieData=allData.map(lambda x: (x.strip().split(",")[2], x.strip().split(",")[3])).distinct()
	onlyMovie=movieData.map(lambda x: x[1]).distinct().zipWithIndex()
	revOnlyMovie=onlyMovie.map(lambda x: (x[1],x[0]))
	onlyUser=movieData.map(lambda x: x[0]).distinct().zipWithIndex()
	revOnlyUser=onlyUser.map(lambda x: (x[1],x[0]))
	m1Data=movieData.join(onlyUser).map(lambda x:(getMovieId(x),getUserIndex(x))).join(onlyMovie).map(lambda x:(getUserIndex2(x),getMovieIndex(x)))
	#m1Data.saveAsTextFile("s3://anitest1/spark1/m1Data/")
	#movieData.saveAsTextFile("s3://anitest1/spark1/outmovie/")
	timeData=allData.map(lambda x: (x.strip().split(",")[2], getValList(x))).reduceByKey(lambda a, b: addLists(a,b)).join(onlyUser).map(lambda x:(getUserIndex(x),getMovieId(x)))
	parsedData=timeData.map(lambda x:np.asarray(eval(str(x[1]))))
	clusters = KMeans.train(parsedData, 3, maxIterations=10, initializationMode="random",seed=50, initializationSteps=5, epsilon=1e-4)
	resultData=timeData.map(lambda x:(x[0],clusters.predict(eval(str(x[1])))))
	joinData=resultData.join(m1Data).map(lambda x: (getKey(x),getVal(x))).groupByKey().map(lambda x : (list(x[1])))
	clusterUserList=joinData.collect()
	overallRecc=[]
	for cluster in clusterUserList:
		cftable=sc.parallelize(cluster)
		cftableTokenized=cftable.map(lambda tokens: (eval(tokens)[0],eval(tokens)[1],eval(tokens)[2]))
		trainingData, validationData = cftableTokenized.randomSplit([8, 2])
		rank = 10
		numIterations = 10
		model = ALS.train(trainingData, rank, numIterations)
		validationTokenized=validationData.map(lambda x: (x[0],x[1]) )
		predictions = model.predictAll(validationTokenized).map(lambda r: ((r[0], r[1]), r[2]))
		ratesAndPreds =validationData.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
		ratesAndPreds.take(10)
		MSE = math.sqrt(ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
		allrecommendations=[]
		for i in set([eval(i)[0] for i in cluster]):
			myMovies=cftableTokenized.filter(lambda x: int(x[0])==i).map(lambda x: (x[0], x[1])).collect()
			movies=cftableTokenized.map(lambda x: x[1]).distinct().collect()
			myList=[]
			for m in movies:
				if m not in myMovies:
					myList.append(m)
			candidates =sc.parallelize(myList).map(lambda x: (i, x))
			predictions_new = model.predictAll(candidates).map(lambda x: (x[0],(x[1],x[2]))).join(revOnlyUser).map(lambda x: (x[1][0][0],(x[1][1],x[1][0][1]))).join(revOnlyMovie).map(lambda x: (x[1][0][0],x[1][1],x[1][0][1])).collect()
			recommendations = sorted(predictions_new, key=lambda x: x[2], reverse=True)[:5]
			allrecommendations.append(recommendations)
		overallRecc.append(allrecommendations)
	sc.parallelize(overallRecc).saveAsTextFile("s3://anitest1/spark1/collabfilteroutput/")
	sc.stop()