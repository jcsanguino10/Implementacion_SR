from pymongo import MongoClient
from bson.objectid import ObjectId
import certifi
from bs4 import BeautifulSoup
import sentence_transformers as st


def get_gcf_tutorial_lessons(db, mongo_id: str) -> list:
	"""Funciton to get the specified tutorial from a mongo database
	based on the string passed as a parameter"""

	# Define the query to get the desired tutorial
	query = {
		#"_id": ObjectId("5b10486a6d5ad52ca4b700fe"),
		"published": "true",
		"translation": "false"
	}

	# Define the tutorial collection to use for the query
	tutorial_collection = db.tutorial

	# Query desired tutorial from mongo
	# Turn the cursor into a list and get the only element

	tutorial = list(tutorial_collection.find(query).limit(1))[0]

	lessons = tutorial.get("units")[0]

	# Break all the units into a list of ids
	lesson_ids = [ObjectId(x) for x in lessons.get("ids").split(",")]

	return lesson_ids


def get_gcf_lessons_html(db, lesson_ids: list[ObjectId]) -> list:
	"""Function to get the specified lessons from a mongo database
	based on the list of ObjectId items passed as parameters"""

	# Define the lesson collection to use for the query
	lesson_collection = db.lesson

	# Query desired lessons from mongo
	query = {
		"_id": {
			"$in": lesson_ids
		}
	}

	lessons = list(lesson_collection.find(query))
	lessons_htmls = [lesson.get("publish").get("pages").get("1") for lesson in lessons]

	return lessons_htmls


def remove_tags(html):
    # parse html content
    soup = BeautifulSoup(html, "html.parser")
    for data in soup(['style', 'script']):
        # Remove tags
        data.decompose()
    # return data by retrieving the tag content
    return ' '.join(soup.stripped_strings)


def transform_lessons_to_vector(units: list) -> str:
	return ""


def connect_mongo_cluster():
	# Database conection
	ca = certifi.where()
	cluster = MongoClient(host="mongodb+srv://read_db:Dak2ZIvwL7ZNqIt6@gcflearnfree.ivza6.azure.mongodb.net/gcfglobal?retryWrites=true&w=majority", tlsCAFile=ca)
	db = cluster["gcfglobal"]
	return db


if __name__ == "__main__":
	db = connect_mongo_cluster()
	lesson_ids: list = get_gcf_tutorial_lessons(db, "")
	lessons_htmls: list = get_gcf_lessons_html(db, lesson_ids)

	print(lessons_htmls)
