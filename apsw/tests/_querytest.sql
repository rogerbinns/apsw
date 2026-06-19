
this should be ignored

-- python:

/*

def pytest(x):
    return x+1

async def apytest(x):
    return x+1
*/

-- name: select_2 -> int
-- cli checking

SELECT 2

--name:no_bind->Any

SELECT 3,4;

-- name: binding(x, y) -> Any

SELECT :x, $y

-- name: binding_locals(x, **locals) -> Any

SELECT $x, @y;