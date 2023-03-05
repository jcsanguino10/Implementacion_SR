from pandas import DataFrame
from google.cloud import bigquery


def connect_bigquery(filename: str="") -> bigquery.Client:
    """Connection to BigQuery client"""
    # Create a Client object to connect to BigQuery

    filename = filename if filename else "rsuniandes-2708b665443f.json"
    client = bigquery.Client.from_service_account_json(f"../credentials/{filename}")

    print("Connection successfully established")

    return client


def execute_query(client: bigquery.Client, query: str) -> DataFrame:
    """Execute a query through the big query client"""
    return client.query(query).to_dataframe()


def execute_all_page_locations(client: bigquery.Client) -> DataFrame:
    """Query that returns all the page_locations from page_view_lesson events"""

    QUERY_ALL_PAGE_LOCATIONS = """
    SELECT expanded.value.string_value
    FROM rsuniandes.analytics_321906640.events_20230302 AS event
        CROSS JOIN UNNEST(event.event_params) AS expanded
            ON expanded.key = "page_location"
    WHERE event.event_name = 'page_view_lesson';
    """

    return client.query(QUERY_ALL_PAGE_LOCATIONS).to_dataframe()


def execute_all_page_locations_per_user(client: bigquery.Client, user: str) -> DataFrame:
    """Query that returns all the urls visited by a specific user"""

    QUERY_ALL_PAGE_LOCATIONS_PER_PSEUDO_ID = f"""
    SELECT expanded.value.string_value
    FROM rsuniandes.analytics_321906640.events_20230302 AS event
        CROSS JOIN UNNEST(event.event_params) AS expanded
            ON expanded.key = "page_location"
    WHERE event.event_name = 'page_view_lesson'
    AND user_pseudo_id = '{user}';
    """

    return client.query(QUERY_ALL_PAGE_LOCATIONS_PER_PSEUDO_ID).to_dataframe()


def execute_all_users_3_distinct_urls(client: bigquery.Client) -> DataFrame:
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

    return client.query(QUERY_ALL_USERS_3_DISTINCT_URLS).to_dataframe()


def execute_all_course_names(client: bigquery.Client) -> DataFrame:
    """Query that returns all the course names from page_view_lesson events"""

    QUERY_ALL_COURSE_NAMES = """
    SELECT DISTINCT TRIM(expanded.value.string_value)
    FROM rsuniandes.analytics_321906640.events_20230302 AS event
      CROSS JOIN UNNEST(event.event_params) AS expanded
        ON expanded.key = "course_name"
    WHERE event.event_name = 'page_view_lesson';
    """

    return client.query(QUERY_ALL_COURSE_NAMES).to_dataframe()

def execute_all_course_names_from_user(client: bigquery.Client, user: str) -> DataFrame:
    """Query that returns all the course names from page_view_lesson events"""

    QUERY_ALL_COURSE_NAMES_FROM_USER = f"""
    SELECT DISTINCT TRIM(expanded.value.string_value)
    FROM rsuniandes.analytics_321906640.events_20230302 AS event
      CROSS JOIN UNNEST(event.event_params) AS expanded
        ON expanded.key = "course_name"
    WHERE event.event_name = 'page_view_lesson'
    AND user_pseudo_id = '{user}'';
    """

    return client.query(QUERY_ALL_COURSE_NAMES_FROM_USER).to_dataframe()


if __name__ == "__main__":
    connection = connect_bigquery()
    print("\nStart Execute all page locations")
    print(execute_all_page_locations(connection))
    print("Done Execute all page locations\n")

    print("\nStart Execute all page locations per user")
    print(execute_all_page_locations_per_user(connection, "922293838.1677754973"))
    print("Done Execute all page locations per user\n")

    print("\nStart Execute all user 3 distinct urls")
    print(execute_all_users_3_distinct_urls(connection))
    print("Done Execute all user 3 distinct urls\n")


    print("\nExecution successful")




# rsuniandes.analytics_321906640.events_20230302