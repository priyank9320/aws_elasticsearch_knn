# aws_elasticsearch_knn
The descriptions of thousands of companies can be converted into vectors and K-Nearest-Neighbour (KNN) algorithm can be used to find simialr companies. To perfrom KNN we traditionally need to keep the vectors in the memory which is very inefficient. Using AWS Elasticsearch KNN resolves this issue. The code here shows how we can use this feature. (this code does not include how to train the vectors)

`elasticsearch_all_operations_for_vectors.ipynb` shows all the different commands required to interact with the elasticsearch cluster on AWS.


The `hander.py`, `requirements.txt`, `serverless.yml` files can be used to deploy a lambda function using the "Serverless Framework". This lambda function needs to be used along with AWS API gateway. In this code you can enter a registration of the company and the code searches for the vector for that company and then perfroms KNN search and provides the top results.