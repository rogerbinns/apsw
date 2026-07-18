
this should be ignored
-- so should this
/*
  and this

  note that the spacing and inconsistencies
/*

-- python:

             /*
r"""a"b\n
d"""
              */

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

--name: no_ret

SELECT * FROM pragma_function_list;

--     name: list_ret -> list[Any]

SELECT * FROM pragma_function_list

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

--   name   : none_rows -> None

SELECT 3

--name:none->None

CREATE TABLE victim(x,y);

-- name: change_count() -> changes

INSERT INTO victim VALUES(1,2), (3,4), (5,6);

--name    :  iter1 -> Iterator[ns_level1.ns_level2.ns_level3]

SELECT 3 as 'three', 4 as 'four';
SELECT 'one' as 'one', 3.3 as 'two';

--name:iter2->  Iterator                  [                   Any   ]

SELECT 3 as 'three', 4 as 'four';
SELECT 'one' as 'one', 3.3 as 'two';

--name: p_binding(one: Orange[Red] = "a'\\\"\03") -> Any
-- binding with al the things (type, hairy value)

SELECT {one}

--   name   :p_id(a) ->list[    dict]

SELECT 3 AS {a:id}, 4 as B;
SELECT 3 AS b, 4 as {a:id};

-- name: p_conv->list[str]

SELECT {xyz!r};
SELECT {xyz!s};
SELECT {xyz!a};

--name     :p_eval

SELECT {a+b
+
3:eval}

-- name: p_evalfn(a_value) -> int

SELECT {len(a_value):eval}

-- name: p_eval_seq() -> Any

SELECT {iter(__builtins__):eval|seq}

--name:  p_seqid(name, x) -> ns_level1

CREATE TABLE {name:id}(a,b,c);
SELECT {x:seq    |
id} FROM sqlite_schema where name={name};

--name   :p_eval_seqid(name) -> ns_level1.ns_level2

SELECT {"tbl_name type".
split():eval|seq|id} FROM sqlite_schema
WHERE name={name};

-- name: p_literal(x) -> str

SELECT {x:literal}

--name: p_eval_literal(x) -> str

SELECT {x + "||" + "'?'":eval|literal}