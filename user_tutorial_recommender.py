
from numpy import ndarray, mean
import tutorial_vectorization as tv
import gcf_queries as gq
from scipy.spatial.distance import cosine

def find_vector_average(*vectors: ndarray) -> ndarray:
	"""Finds the mean per column of the vectors"""
	return mean(vectors, axis=0)


def find_vector_distance(vector1: ndarray, vector2: ndarray) -> float:
	"""Finds the cosine distance between two vectors"""
	return float(cosine(vector1, vector2))


def calculate_all_cosine_distances(user_mean: ndarray, *vectors: ndarray) -> list[float]:
	"""Calculate the cosine distance between a user mean vector and a list of vectors"""
	return [find_vector_distance(user_mean, vector) for vector in vectors]


def calulate_vector_similarities(user_mean, vector_ids: list[str], *vectors: ndarray) -> list[tuple[float, str]]:
	cosine_distances = calculate_all_cosine_distances(user_mean, *vectors)
	print(len(cosine_distances) == len(vector_ids))
	cosines_with_ids = list(zip(cosine_distances, vector_ids))
	sorted_cosines = sorted(cosines_with_ids, key=lambda x: x[1])
	return sorted_cosines


if __name__ == "__main__":
	#calulate_vector_similarities()
	pass