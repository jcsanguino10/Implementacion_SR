from numpy import ndarray, array
from pymongo.database import Database
from google.cloud.bigquery import Client
import pandas as pd

from vector_operations import calculate_all_cosine_distances, find_vector_average
from tutorial_vectorization import read_vector_bytes
from gcf_mongo_queries import connect_mongo_cluster, get_gcf_tutorials_by_titles
from gcf_bigquery_queries import connect_bigquery, execute_all_course_names_from_user
from tutorial_vectorization import get_vector_csv


def calulate_vector_cosine_similarities(user_mean: ndarray, vector_ids: list[str], *vectors: ndarray) -> list[tuple[str, float]]:
	"""Calculate the cosine similarities between a user_mean vector and a list of vectors"""

	# Calucalte all the cosine
	cosine_distances: list[float] = calculate_all_cosine_distances(user_mean, *vectors)
	#print(len(cosine_distances) == len(vector_ids))

	# Zip each similarity with its vector id and the sort
	cosines_with_ids = list(zip(vector_ids, cosine_distances))
	sorted_cosines = sorted(cosines_with_ids, key=lambda x: x[1])

	return sorted_cosines


def get_tutorial_vectors(tutorial_ids: list) -> ndarray:
	"""Get the vectors of a tutorials from csv"""

	df = pd.read_csv("data/vector_collection.csv", header=0)

	# Get the vectors from the df that have an id in the tutorial_ids list
	vectors = df.loc[df["id"].isin(tutorial_ids), ["id", "vector"]].to_numpy()

	return vectors


def find_courses_from_id(csv: list[list[str]], *ids: str) -> list[tuple[str, str, str]]:
	"""Find the courses in the csv from a list of ids"""

	# This works by finding the row in the csv that matches each id for all ids in the parameters
	# Since there should only be one element that matches the id, the only element in the filter is taken
	found_courses = list(map(lambda *x: list(filter(lambda y: y[0] == x[0], csv))[0], ids))
	courses = [(course[0], course[1], course[2]) for course in found_courses]

	return courses


def calculate_recommenadations_from_course_names(db_bigquery: Client, db_mongo: Database, user: str) -> list[tuple[str, str, str]]:
	"""Get the names of the recommended courses for a user"""

	# Get all the course names from the courses that a user has seen
	user_course_names = execute_all_course_names_from_user(db_bigquery, user)

	# Get all the courses from mongo that have the title in the user_course_names list
	user_tutorials = get_gcf_tutorials_by_titles(db_mongo, user_course_names)
	# print(len(user_tutorials))
	# print(user_tutorials)

	# Get all the info from the csv and remove the header
	all_vectors_csv: list[list[str]] = get_vector_csv()[1:]

	# Get the ids of the tutorials in the csv
	all_ids_csv: list[str] = [row[0] for row in all_vectors_csv]



	#print(len(user_course_names) == len(user_tutorials))



	# Extract the ids of the tutorials
	user_tutorial_ids: list[str] = [str(tutorial["_id"]) for tutorial in user_tutorials]
	# print(len(user_tutorial_ids))
	# print(user_tutorial_ids)
	#print(list(map(lambda x: x[1], find_courses_from_id(all_vectors_csv, *user_tutorial_ids))))


	# Get the vectors of the tutorials from csv
	user_tutorial_vectors: ndarray = get_tutorial_vectors(user_tutorial_ids)
	# print(len(user_tutorial_vectors))
	# print(user_tutorial_vectors)

	# Transform the vectors into a list of ndarrays
	user_vectors: list[ndarray] = [read_vector_bytes(vector[1]) for vector in user_tutorial_vectors]
	# print(len(user_vectors))
	# print(user_vectors)

	# Calculate the mean vector of the user based on all the courses the user has seen
	user_mean_vec: ndarray = find_vector_average(*user_vectors)
	# print(len(user_mean_vec))
	# print(user_mean_vec)

	# Transform the mean vector into a numpy array
	user_mean_vector: ndarray = array(user_mean_vec)
	# print(len(user_mean_vector))
	# print(user_mean_vector)


	# print(user_tutorial_ids)

	#print(len(user_tutorials) == len(user_tutorial_vectors))






	# Get the ids of the tutorials that the user has not seen
	comparative_ids: list[str] = [iden for iden in all_ids_csv if iden not in user_tutorial_ids]

	# Get all the vectors of the tutorials that the user has not seen
	all_tutorials = get_tutorial_vectors(comparative_ids)

	# Transform the vectors into a list of ndarrays
	all_vecs = [read_vector_bytes(vector[1]) for vector in all_tutorials]
	all_vectors = array(all_vecs)

	# Calculate the cosine similarities between the user_mean_vector and the vectors of the tutorials that the user has not seen
	# The consines come sorted from most similar to least similar
	cosines = calulate_vector_cosine_similarities(user_mean_vector, comparative_ids, *all_vectors)

	cosines_ids_recommendation = list(map(lambda x: x[0], cosines[:5]))

	# Get the recommended courses titles
	# This works by taking the first element in the cosines list and then using it to filter the all_vectors_csv list to find the course name
	# Afterwards, since each filter element (list) has only 1 item, it is accessed and the course name is taken (index=1)
	#recommended_courses = list(map(lambda x: list(filter(lambda y: y[0] == x[0], all_vectors_csv))[0][1], cosines[:5]))
	#print(recommended_courses)

	recommended_courses = find_courses_from_id(all_vectors_csv, *cosines_ids_recommendation)
	#print(list(map(lambda x: x[1], recommended_courses)))

	return recommended_courses



if __name__ == "__main__":
	db_bigquery = connect_bigquery()
	db_mongo = connect_mongo_cluster()
	user = "968863862.1677002245"
	recommended_courses = calculate_recommenadations_from_course_names(db_bigquery, db_mongo, user)
	print(list(map(lambda x: x[1], recommended_courses)))



# Este es el URL -> a que curso pertenece

# Hay dimension que se llama "course_name" dentro de "page_view_lesson"
# pymongo se llama "title"