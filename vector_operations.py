from numpy import ndarray, mean, fromstring
from scipy.spatial.distance import cosine

def find_vector_average(*vectors: ndarray) -> ndarray:
	"""Finds the mean per column of the vectors"""
	return mean(vectors, axis=0)


def find_vector_distance(vector1: ndarray, vector2: ndarray) -> float:
	"""Finds the cosine distance between two vectors"""
	return float(cosine(vector1, vector2))


def calculate_all_cosine_distances(target_vector: ndarray, *vectors: ndarray) -> list[float]:
	"""Calculate the cosine distance between a vector and a list of vectors"""
	return [find_vector_distance(target_vector, vector) for vector in vectors]


def read_vector_bytes(byte_seq: str, separator: str=";") -> ndarray:
	"""Read a vector from a byte sequence"""
	return fromstring(byte_seq, sep=separator)