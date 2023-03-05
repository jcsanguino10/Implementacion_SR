# Implementacion de SR

To execute the Big Query queries, it is necessary to have a foler outside the project called
"credentials". Inside this folder should be a json file with the credentials. Pass the name
of the credential file to the connect_bigquery() function as a paramter.

To execute the Mongo queries, it is necessary to have a foler outside the project called
"credentials". Inside this folder should be a json file with the credentials. Pass the name
of the credential file to the connect_mongo_cluster() function as a paramter.

The analytics.js file contains the code to be added to a website in order to
retrieve an user's Google Analytics id.

To execute the recommendation system, execute the user_tutorial_recommender file
and change the id of the user to get the recommendations.
