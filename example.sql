-- python: Set things up
-- also more comments

/*
class Greeting(dict):
    pass


*/

-- name: fractal(width: int=30, height: int=20, iterations:int=40)
-- Makes an ASCII art fractal

-- Based on `outlandish example <https://sqlite.org/lang_with.html#outlandish_recursive_query_examples>`__

-- Increasing iterations does more CPU work which is useful in testing
-- when you need queries that take a lot of time.


WITH RECURSIVE
xaxis(x) AS (
    SELECT -2.0
    UNION ALL
    SELECT x + (3.2 / :width) FROM xaxis WHERE x < 1.2 - (3.2 / :width)
),
-- part of query
yaxis(y) AS (
    SELECT -1.0
    UNION ALL
    SELECT y + (2.0 / :height) FROM yaxis WHERE y < 1.0 - (2.0 / :height)
),
m(iter, cx, cy, x, y) AS (
    SELECT 0, x, y, 0.0, 0.0 FROM xaxis, yaxis
    UNION ALL
    SELECT iter+1, cx, cy, x*x-y*y + cx, 2.0*x*y + cy FROM m
    WHERE (x*x + y*y) < 4.0 AND iter < :iterations
),
m2(iter, cx, cy) AS (
    SELECT max(iter), cx, cy FROM m GROUP BY cy, cx
),
a(t) AS (
    SELECT group_concat(substr(' .:-=+*#%@', 1+min(iter/10, 9), 1), '')
    FROM m2 GROUP BY cy
)
SELECT group_concat(rtrim(t), x'0a') FROM a;

-- name: get_all_greetings() -> Greeting

-- Get all the greetings in the database

select greeting_id, greeting
  from greetings
 order by 1;

-- name: get_user_by_username
-- Get a user from the database using a named parameter
select user_id, username, name
  from users
  where username = :username;