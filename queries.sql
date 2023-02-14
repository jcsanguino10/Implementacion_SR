
# Query that returns all the page_locations from page_view_lesson events
SELECT expanded.value.string_value
FROM recommendation-system-355420.analytics_321906640.events_20221217 AS event
    CROSS JOIN UNNEST(event.event_params) AS expanded
        ON expanded.key = "page_location"
WHERE event.event_name = 'page_view_lesson';


# Query that returns all the urls visited by a specific user
SELECT expanded.value.string_value
FROM recommendation-system-355420.analytics_321906640.events_20221217 AS event
    CROSS JOIN UNNEST(event.event_params) AS expanded
        ON expanded.key = "page_location"
WHERE event.event_name = 'page_view_lesson'
AND user_pseudo_id = {user};

# Query that returns all the users that have visited 3 or more distinct urls
SELECT user_pseudo_id, COUNT(DISTINCT expanded.value.string_value) AS courses_number
FROM recommendation-system-355420.analytics_321906640.events_20221217 AS event
    CROSS JOIN UNNEST(event.event_params) AS expanded
        ON expanded.key = "page_location"
WHERE event.event_name = 'page_view_lesson'
GROUP BY user_pseudo_id
    HAVING courses_number >= 3
ORDER BY courses_number ASC;