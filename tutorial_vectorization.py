import certifi
from bs4 import BeautifulSoup
from bson.objectid import ObjectId
from functools import reduce
from pymongo import MongoClient
from pymongo.database import Database
from numpy import ndarray, array2string, fromstring
from sentence_transformers import SentenceTransformer
from typing import Generator
import csv


def mock_vectorization() -> Generator[str, None, None]:
	num = 0
	while True:
		yield str(num)
		num += 1


def connect_mongo_cluster() -> Database:
	# Database conection
	ca = certifi.where()
	cluster = MongoClient(host="mongodb+srv://read_db:Dak2ZIvwL7ZNqIt6@gcflearnfree.ivza6.azure.mongodb.net/gcfglobal?retryWrites=true&w=majority", tlsCAFile=ca)
	db = cluster["gcfglobal"]
	return db


def get_all_gcf_tutorials(db: Database) -> list[dict]:
	"""Function to the all tutorials from a mongo database"""
	query = {
		"published": "true",
		"translation": "false"
	}

	tutorial_collection = db.tutorial
	tutorials = list(tutorial_collection.find(query).limit(10))		# TODO: Remove limit

	return tutorials


def get_gcf_tutorial_by_id(db: Database, mongo_id: str) -> dict:
	"""Funciton to get the specified tutorial from a mongo database
	based on the id passed as a parameter"""

	# Define the query to get the desired tutorial
	query = {
		"_id": ObjectId("5b10486a6d5ad52ca4b700fe"),
		"published": "true",
		"translation": "false"
	}

	# Define the tutorial collection to use for the query
	tutorial_collection = db.tutorial

	# Query desired tutorial from mongo
	# Turn the cursor into a list and get the only element
	# This is necessary because that's the only way to access its attributes
	tutorial = list(tutorial_collection.find(query).limit(1))[0]

	return tutorial


def get_gcf_tutorial_by_title(db: Database, title: str) -> dict:
	"""Function to get the specified tutorial from a mongo database
	bvased on the title passed as a parameter"""

	# Define the queryto get the desired tutorial
	query = {
		"title": title,
		"published": "true",
		"translation": "false"
	}

	tutorial_collection = db.tutorial

	# Query desired tutorial from mongo
	# Turn the cursor into a list and get the only element
	# This is necessary because that's the only way to access its attributes
	tutorial = list(tutorial_collection.find(query).limit(1))[0]

	return tutorial


def get_tutorial_information(tutorial: dict) -> list:
	"""Get important attributes from a tutorial"""

	return [str(tutorial["_id"]), tutorial["language"]]


def get_gcf_tutorial_lesson_ids(tutorial: dict) -> list[str]:
	"""Get the ids of the lessons of a tutorial"""

	# From the "units" list attribute, get each lesson and get the ids
	# that are in that lesson --> result = a list of lists of ids
	all_lessons = [lesson["ids"].split(",") for lesson in tutorial["units"]]

	# Merge the previous list into a single list with all ids
	merged_list = reduce(lambda x, y: x + y, all_lessons) if all_lessons else []

	return merged_list


def get_gcf_lessons_html(db: Database, lesson_ids: list[str]) -> list[str]:
	"""Function to get the specified lessons from a mongo database
	based on the list of ObjectId items passed as parameters"""

	# Turn each id of the parameter into an ObjectId so that mongo can
	# identiy it as a valid id
	objectid_lesson_ids = [ObjectId(iden) for iden in lesson_ids]

	# Define the lesson collection to use for the query
	lesson_collection = db.lesson

	# Query desired lessons from mongo
	query = {
		"_id": {
			"$in": objectid_lesson_ids
		}
	}

	lessons = list(lesson_collection.find(query))

	# Get the htmls which are in publish:pages:1 for all lessons
	lessons_htmls = [lesson["publish"]["pages"]["1"] for lesson in lessons]

	return lessons_htmls


def remove_tags(html: str) -> str:
    # parse html content
    soup = BeautifulSoup(html, "html.parser")
    for data in soup(['style', 'script']):
        # Remove tags
        data.decompose()
    # return data by retrieving the tag content
    return ' '.join(soup.stripped_strings)


def transform_str_to_vector_bytes(lessons_str: str) -> str:
	# Transform a str to vector

	model = SentenceTransformer("all-MiniLM-L6-v2")
	embedding = model.encode(lessons_str)
	embedding_str = array2string(embedding, separator=';')[1:-1].replace('\n', '')
	print(type(embedding))
	return embedding_str


def create_vector_csv(attributes: list[list[str]], vectors: list[str]) -> None:
	"""Create a csv from all the attributes and vectors passed as parameters"""

	with open("data/vector_collection.csv", "w") as file:
		file.write("id, language, vector\n")
		for att, vec in zip(attributes, vectors):
			file.write(f"{att[0]}, {att[1]}, {vec}\n")


def append_vector_to_csv(attributes: list[str], vector: str) -> None:
	"""Append attributes and a vector to the csv"""

	with open("data/vector_collection.csv", "a") as file:
		file.write(f"{attributes[0]}, {attributes[1]}, {vector}\n")


def read_vector_bytes(byte_seq: str) -> ndarray:
	"""Read a vector from a byte sequence"""
	return fromstring(byte_seq, sep=';')


def update_vector_by_id_csv(id: str, vector: str) -> None:
	"""Update a vector in the csv by its id"""

	# Load the csv into a list of lists
	with open("data/vector_collection.csv", "r") as file:
		reader = csv.reader(file)
		vector_list = list(reader)

	# Update the vector
	for element in vector_list:
		if element[0] == id:
			element[2] = vector
			break

	# Write the updated list to the csv
	with open("data/vector_collection.csv", "w") as file:
		writer = csv.writer(file)
		writer.writerows(vector_list)


def load_vector_csv() -> list[list[str]]:
	"""Load the vector csv into a list of lists"""

	with open("data/vector_collection.csv", "r") as file:
		reader = csv.reader(file)
		vector_list = list(reader)

		for element in vector_list:
			print(element)
			print("\n\n")

		for element in vector_list:
			print(read_vector_bytes(element[2]))
			print("\n\n")

	return vector_list


def initial_vectorization(db: Database) -> None:
	"""Initial vectorization of all the lessons in the database"""

	# Get all tutorials
	all_tutorials: list[dict] = get_all_gcf_tutorials(db)

	# Get the necessary attributes of all tutorials
	all_tutorials_attributes: list[list[str]] = [get_tutorial_information(tutorial) for tutorial in all_tutorials]
	#print(all_tutorials_attributes)

	# For each tutorial get the list of all the lessons in that tutorial
	# So for each tutorial there will be a list of lessons
	# Resulting in a list of lists
	all_tutorials_lessons: list[list[str]] = [get_gcf_tutorial_lesson_ids(tutorial) for tutorial in all_tutorials]
	#print(all_tutorials_lessons)

	# For each list in the <all_tutotrials_lessons> list get the list of htmls of each lesson
	all_lessons_htmls: list[list[str]] = [get_gcf_lessons_html(db, lesson_ids) for lesson_ids in all_tutorials_lessons]
	#print(all_lessons_htmls)

	# For each list of htmls in list <all_lessons_htmls> remove the tags and
	# store each list in a list
	all_plain_text_htmls = [[remove_tags(html) for html in lessons_htmls] for lessons_htmls in all_lessons_htmls]
	#print(all_plain_text_htmls)

	# For each list of plain text htmls in list <all_plain_text_htmls> join them into one string
	# and each list in a list
	all_texts: list = [" ".join(text) for text in all_plain_text_htmls]
	#print(all_texts)

	# For each text in the list <all_texts> transform it into a vector
	# and store it in a list
	generator = mock_vectorization()
	all_vectors: list[str] = [transform_str_to_vector_bytes(text) for text in all_texts]
	#print(all_vectors)

	print(len(all_tutorials_attributes) == len(all_vectors))

	# Create the csv file with the attributes and vectors
	create_vector_csv(all_tutorials_attributes, all_vectors)


def update_vector_by_id(db: Database, mongo_id: str) -> None:
	tutorial: dict = get_gcf_tutorial_by_id(db, mongo_id)
	tutorial_attributes = get_tutorial_information(tutorial)
	lesson_ids: list = get_gcf_tutorial_lesson_ids(tutorial)
	lessons_htmls: list = get_gcf_lessons_html(db, lesson_ids)
	plain_text_html: list = [remove_tags(html) for html in lessons_htmls]
	text: str = " ".join(plain_text_html)
	vector = transform_str_to_vector_bytes(text)
	update_vector_by_id_csv(mongo_id, vector)



if __name__ == "__main__":
	db = connect_mongo_cluster()
	initial_vectorization(db)
	update_vector_by_id(db, "5b1048696d5ad52ca4b700d1")







# CSV layout

"""

tutorial_id, language, vector

"""