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
	print(len(cosine_distances) == len(vector_ids))

	# Zip each similarity with its vector id and the sort
	cosines_with_ids = list(zip(vector_ids, cosine_distances))
	sorted_cosines = sorted(cosines_with_ids, key=lambda x: x[1])

	return sorted_cosines


def get_tutorial_vectors(tutorial_ids: list) -> ndarray:
	"""Get the vectors of a tutorials from csv"""

	df = pd.read_csv("data/vector_collection.csv", index_col=0)

	# Get the vectors from the df that have an id in the tutorial_ids list
	vectors = df.loc[tutorial_ids, ["id", "vector"]].to_numpy()

	return vectors


def get_ids_and_vectors_from_course_names(db_bigquery: Client, db_mongo: Database, user: str) -> list[tuple[str, float]]:
	"""Get the ids and vectors from a list of course names"""

	user_courses: pd.DataFrame = execute_all_course_names_from_user(db_bigquery, user)
	user_course_names = user_courses.values.tolist()

	# Get the tutorials from the course names
	user_tutorials = get_gcf_tutorials_by_titles(db_mongo, user_course_names)
	user_tutorial_ids = [str(tutorial["_id"]) for tutorial in user_tutorials]
	user_tutorial_vectors: ndarray = get_tutorial_vectors(user_tutorial_ids)
	user_vectors = [read_vector_bytes(vector[:, 1:]) for vector in user_tutorial_vectors]
	user_mean_vec: ndarray = find_vector_average(*user_vectors)
	user_mean_vector: ndarray = array(user_mean_vec)


	print(len(user_tutorials) == len(user_tutorial_vectors))

	all_vectors_csv: list[list[str]] = get_vector_csv()
	all_ids = [row[0] for row in all_vectors_csv]
	comparative_ids: list[str] = [iden for iden in all_ids if iden not in user_tutorial_vectors]

	all_tutorials = get_tutorial_vectors(comparative_ids)
	all_ids = [row[0] for row in all_vectors_csv]
	all_vecs = [read_vector_bytes(vector[:, 1:]) for vector in all_tutorials]
	all_vectors = array(all_vecs)

	cosines = calulate_vector_cosine_similarities(user_mean_vector, comparative_ids, *all_vectors)

	return cosines[:5]



if __name__ == "__main__":
	db_bigquery = connect_bigquery()
	db_mongo = connect_mongo_cluster()
	user = "922293838.1677754973"
	print(get_ids_and_vectors_from_course_names(db_bigquery, db_mongo, user))



# Este es el URL -> a que curso pertenece

# Hay dimension que se llama "course_name" dentro de "page_view_lesson"
# pymongo se llama "title"