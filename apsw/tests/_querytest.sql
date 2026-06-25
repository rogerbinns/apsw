
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

-- name: res_zero -> Any

SELECT * from sqlite_schema WHERE name='no exist';

-- name: res_zero_opt -> Any | None

SELECT * from sqlite_schema WHERE name='no exist';

-- name: res_zero_literal -> Any | Literal['abc' + 'def']

SELECT * from sqlite_schema WHERE name='no exist';

-- name: res_zero_nested -> Any | ns_level1 . ns_level2 . ns_level3

SELECT * from sqlite_schema WHERE name='no exist';

--name: too_many -> int

SELECT 3;
SELECT 4;

--python:nested namespaces

class ns_level1:
    def __init__(self, **kwargs):
        self.kwargs=kwargs

    class ns_level2:
        def __init__(self, **kwargs):
            self.kwargs=kwargs

        class ns_level3:
            def __init__(self, **kwargs):
                self.kwargs=kwargs

--name: level1 -> ns_level1

SELECT 1 AS one, 2 as 'T W O';

--name: level2 -> ns_level1.ns_level2

SELECT 3 as '3', 4 as '';

--name:level3-> ns_level1    . ns_level2   . ns_level3

SELECT 5 as 'select', 6 as 'class';