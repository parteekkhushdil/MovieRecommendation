import requests
import urllib.request
import boto3


# downloading from S3


s3 = boto3.resource('s3')
bucket = s3.Bucket('anitest1')
obj=bucket.Object('spark1/collabfilteroutput2/part-00011')

with open('part-00011', 'wb') as data:
	obj.download_fileobj(data)



# reading from .txt part


f=open("part-00011")
data=eval(f.read())
print(data)

host_store='http://search-tweetymap-gurowqxu56ejw6kii5afincr3y.us-east-1.es.amazonaws.com/cf_data_new/recommendations/'
for d in data:
	user=d[0][0]
	rec=[]
	for recommendations in d:
		rec.append(recommendations[1])
	data_es={d[0][0]:rec}
	requests.post(host_store,json=data_es)
print(data_es)

