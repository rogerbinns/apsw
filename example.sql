-- python: Set things up
-- also more comments

/*

"Module docstring"

import time
*/

-- name: fractal(width:int = 30, height:int = 20, iterations:int = 28) -> str
-- Makes an ASCII art fractal

-- Based on `outlandish example <https://sqlite.org/lang_with.html#outlandish_recursive_query_examples>`__
-- with improved appearance by Gemini

-- Increasing iterations does more CPU work which is useful in testing
-- when you need queries that take a lot of time.


WITH RECURSIVE
xaxis(x) AS (
    SELECT -2.0
    UNION ALL
    SELECT x + (3.2 / :width) FROM xaxis WHERE x < 1.2 - (3.2 / :width)
),
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
    SELECT group_concat(
        substr(' .:-=+*#%@',
            CASE WHEN iter = :iterations THEN 1 ELSE 2 + (iter * 8 / :iterations) END,
            1),
        '')
    FROM m2 GROUP BY cy ORDER BY cy
)
SELECT group_concat(rtrim(t), x'0a') FROM a;