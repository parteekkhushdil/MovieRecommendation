# MovieRecommendation
The project aims at developing a recommender system for movie streaming website. We tend to utilize the information provided about the users like what movies each user has watched, his favorite genres, actors etc. and also the information about movie tags, like genres, moods, keywords etc. We use two main algorithms, collaborative filtering to find similar users and connected components in a graph to find similar movies. We take in consideration what type of user is required to be recommended and for each use case we recommend movies accordingly.
####Dependencies
1. `Pyspark`
2. `pandas`
3. `itertools`
4. `Collections`
5. `boto3`
6. `numpy`
7. `math`

####To use project
1. To use the project, install the dependencies.
2. The code for connected components in a graph is given in `src/connectedGraph.py`.
3. The code for clustering and collaborative filtering is give in `src/clusterCF.py`.

####Youtube Link

`https://youtu.be/dzYKOqaBQwM`
