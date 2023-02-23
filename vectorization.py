import certifi
from bs4 import BeautifulSoup
from bson.objectid import ObjectId
from functools import reduce
from pymongo import MongoClient
from pymongo.database import Database
from numpy import ndarray
#from sentence_transformers import SentenceTransformer
import script_big_query as sbq
from typing import Generator


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
	based on the string passed as a parameter"""

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


def transform_str_to_vector(lessons_str: str) -> ndarray:
	# Transform a str to vector
	model = SentenceTransformer("paraphrase-multiligual-mpnet-base-v2")
	embedding = model.encode(lessons_str)
	return embedding


# TODO
def create_vector_csv(vectors: list) -> None:
	path = ""
	path = "../../../../Downloads"
	with open(f"{path if path else ''}/vector_collection.csv", "w") as file:
		file.write("id, vector\n")
		pass
# TODO
def append_vector_to_csv() -> None:
	with open("vector_collection.csv", "a"):
		pass


def initial_vectorization(db: Database):# -> list[ndarray]:			# TODO
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
	all_vectors = [next(generator) for text in all_texts] # TODO
	#print(all_vectors)

	print(len(all_tutorials_attributes) == len(all_vectors))


	# TODO: implement data saving into csv

	return all_vectors


def update_vector_by_id(db: Database, mongo_id: str):# -> ndarray:		# TODO
	tutorial: dict = get_gcf_tutorial_by_id(db, mongo_id)
	tutorial_attributes = get_tutorial_information(tutorial)
	lesson_ids: list = get_gcf_tutorial_lesson_ids(tutorial)
	lessons_htmls: list = get_gcf_lessons_html(db, lesson_ids)
	plain_text_html: list = [remove_tags(html) for html in lessons_htmls]
	text: str = " ".join(plain_text_html)

	generator = mock_vectorization()
	vector = next(generator)				# TODO
	return vector



if __name__ == "__main__":
	db = connect_mongo_cluster()
	vector = update_vector_by_id(db, "5b1048696d5ad52ca4b700d1")
	#init_vector_list = initial_vectorization(db)

	print(vector)
	#print(init_vector_list)






# CSV layout

"""

tutorial_id, language, vector

"""