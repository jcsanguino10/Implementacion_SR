import certifi
from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.database import Database


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


def get_gcf_tutorials_by_titles(db: Database, titles: list[str]) -> list[dict]:
	"""Function to get the specified tutorials from a mongo database
	based on the list of titles passed as parameters"""

	# Define the query to get the desired tutorials
	query = {
		"title": {
			"$in": titles
		},
		"published": "true",
		"translation": "false"
	}

	tutorial_collection = db.tutorial

	# Query desired tutorials from mongo
	tutorials = list(tutorial_collection.find(query))

	return tutorials


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
