from google.cloud.bigquery import Client, QueryJob
from functools import reduce


def connect_bigquery(filename: str="") -> Client:
    """Connection to BigQuery client"""
    # Create a Client object to connect to BigQuery

    filename = filename if filename else "rsuniandes-2708b665443f.json"
    client = Client.from_service_account_json(f"../credentials/{filename}")

    print("Connection successfully established")

    return client


def execute_query(client: Client, qr: str) -> list[str]:
    """Execute a query through the big query client"""
    query = client.query(qr)
    return transform_query_to_list(query)


def execute_all_page_locations(client: Client) -> list[str]:
    """Query that returns all the page_locations from page_view_lesson events"""

    QUERY_ALL_PAGE_LOCATIONS = """
    SELECT expanded.value.string_value
    FROM rsuniandes.analytics_321906640.events_20230302 AS event
        CROSS JOIN UNNEST(event.event_params) AS expanded
            ON expanded.key = "page_location"
    WHERE event.event_name = 'page_view_lesson';
    """

    query = client.query(QUERY_ALL_PAGE_LOCATIONS)
    return transform_query_to_list(query)


def execute_all_page_locations_per_user(client: Client, user: str) -> list[str]:
    """Query that returns all the urls visited by a specific user"""

    QUERY_ALL_PAGE_LOCATIONS_PER_PSEUDO_ID = f"""
    SELECT expanded.value.string_value
    FROM rsuniandes.analytics_321906640.events_20230302 AS event
        CROSS JOIN UNNEST(event.event_params) AS expanded
            ON expanded.key = "page_location"
    WHERE event.event_name = 'page_view_lesson'
    AND user_pseudo_id = '{user}';
    """

    query = client.query(QUERY_ALL_PAGE_LOCATIONS_PER_PSEUDO_ID)
    return transform_query_to_list(query)


def execute_all_users_3_distinct_urls(client: Client) -> list[str]:
    """Query that returns all the users that have visited 3 or more distinct urls with
    its respective count of distinc urls"""

    QUERY_ALL_USERS_3_DISTINCT_URLS = """
    SELECT user_pseudo_id, COUNT(DISTINCT expanded.value.string_value) AS courses_number
    FROM rsuniandes.analytics_321906640.events_20230302 AS event
        CROSS JOIN UNNEST(event.event_params) AS expanded
            ON expanded.key = "page_location"
    WHERE event.event_name = 'page_view_lesson'
    GROUP BY user_pseudo_id
        HAVING courses_number >= 3
    ORDER BY courses_number ASC;
    """

    query = client.query(QUERY_ALL_USERS_3_DISTINCT_URLS)
    return transform_query_to_list(query)


def execute_all_course_names(client: Client) -> list[str]:
    """Query that returns all the course names from page_view_lesson events"""

    QUERY_ALL_COURSE_NAMES = """
    SELECT DISTINCT TRIM(expanded.value.string_value)
    FROM rsuniandes.analytics_321906640.events_20230302 AS event
      CROSS JOIN UNNEST(event.event_params) AS expanded
        ON expanded.key = "course_name"
    WHERE event.event_name = 'page_view_lesson';
    """

    query = client.query(QUERY_ALL_COURSE_NAMES)
    return transform_query_to_list(query)


def execute_all_course_names_from_user(client: Client, user: str) -> list[str]:
    """Query that returns all the course names from page_view_lesson events"""

    QUERY_ALL_COURSE_NAMES_FROM_USER = f"""
    SELECT DISTINCT TRIM(expanded.value.string_value)
    FROM rsuniandes.analytics_321906640.events_20230302 AS event
      CROSS JOIN UNNEST(event.event_params) AS expanded
        ON expanded.key = "course_name"
    WHERE event.event_name = 'page_view_lesson'
    AND user_pseudo_id = '{user}';
    """

    query = client.query(QUERY_ALL_COURSE_NAMES_FROM_USER)
    return transform_query_to_list(query)


def transform_query_to_list(query: QueryJob) -> list[str]:
    query_list = [list(row) for row in query]
    return list(reduce(lambda x, y: x + y, query_list))


if __name__ == "__main__":
    connection = connect_bigquery()
    print("\nStart Execute all page locations")
    #print(execute_all_page_locations(connection))
    print("Done Execute all page locations\n")

    print("\nStart Execute all page locations per user")
    #print(execute_all_page_locations_per_user(connection, "922293838.1677754973"))
    #print(execute_all_course_names_from_user(connection, "922293838.1677754973"))
    print("Done Execute all page locations per user\n")

    print("\nStart Execute all user 3 distinct urls")
    print(execute_all_users_3_distinct_urls(connection))
    print("Done Execute all user 3 distinct urls\n")


    print("\nExecution successful")




# rsuniandes.analytics_321906640.events_20230302