# Notes for adding the SQLite session extension API


from typing import Iterator, TypeAlias
import collections.abc

# Streaming type aliases.

# read between 1 and supplied int number of bytes.  At EOF
# return None.  Raise exception for error.


SessionStreamInput : TypeAlias = Callable[[int], collections.abc.Buffer]
# called with each output chunk.  Raise exception for error.
SessionStreamOutput : TypeAlias = Callable[[memoryview], None]



# sqlite3session_config - expose as apsw.session_config
def apsw_session_config(op: int, *args):
    # module level
    # need to verify no connections are active
    ...

# apsw.Session
class Session:
    # sqlite3session_create
    def __init__(self, db: Connection, schema: str):
        # database has to keep ref to this like with
        # blob etc as this has to be closed before
        # the database is
        ...

    # sqlite3session_delete
    def close(self):  # also __del__
        ...

    # sqlite3session_attach
    def attach(self, name: str | None): ...

    # sqlite3session_table_filter
    def table_filter(Callable[[str], bool]): ...

    # sqlite3session_enable
    enabled : bool   # r/w

    # sqlite3session_indirect
    indirect: bool # r/w

    # sqlite3session_isempty
    is_empty: bool # ro

    # sqlite3session_memory_used
    memory_used: int # ro

    # sqlite3session_changeset_size
    changeset_size: int # ro

    # sqlite3session_object_config
    def config(self, op: int, *args):
        ...

    # sqlite3session_diff
    def diff(self, schema: str, table: str):
        ...


    #### OUTPUT
    #
    # These would ideally make a ChangeSet instead
    # and you could request that to serialize to
    # bytes or stream but it makes lifetime management
    # more complicated and Session keeps recording
    # so it wouldn't represent a point in time

    # sqlite3session_changeset
    def changeset(self) -> bytes:
        ...

    # sqlite3session_changeset_strm
    def changeset_stream(self, output: SessionStreamOutput):
        ...

    # sqlite3session_patchset
    def patchset(self) -> bytes:
        ...

    # sqlite3session_patchset_strm
    def patchset_stream(self, output: SessionStreamOutput):
        ...


class ChangesetIter:
    # instances made by Changeset.iter()

    # sqlite3changeset_start, -strm, _v2
    def __init__(self):
        ...


    def next(self) -> TableChange:
        # sqlite3changeset_next to advance
        # construct TableChange
        ...

    # sqlite3changeset_finalize
    def __del__(self):
        ...



# apsw.Changeset
class Changeset:
    # note can also be a patchset

    # all methods are static, you can't make instances of this,
    # so it essentially serves as a namespace




    @staticmethod
    def iter(changeset: bytes | SessionStreamInput, flags: int = 0) -> Iterator[TableChange]:
        # see ChangesetIter above
        ...

    # sqlite3changeset_apply
    # sqlite3changeset_apply_v2 if flags is non-zero
    # _strm suffix if source is callable
    # perhaps default conflict that aborts all on conflict
    @staticmethod
    def apply(source: bytes | SessionStreamInput, connection,
                    filter: Callable[[str], bool] | None,
                    conflict: Callable[[int,TableChange], int],
                    *,
                    flags: int = 0):
        # conflict also needs sqlite3changeset_conflict
        # sqlite3changeset_fk_conflicts

        # TableChange is provided in conflict and as Iterator.  Looks
        # like they are the same, but will check when implementing
        ...


    # sqlite3changeset_upgrade - not clear what this actually does
    @staticmethod
    def upgrade(connection, schema: str, source: bytes) -> bytes:
        ...


    # sqlite3changeset_invert
    @staticmethod
    def invert(bytes) -> bytes:
        ...

    # sqlite3changeset_invert_strm
    @staticmethod
    def invert_stream(source: SessionStreamInput, output: SessionStreamOutput):
        ...


    # sqlite3changeset_concat
    @staticmethod
    def concat(first: bytes, second: bytes) -> bytes:
        ...

    # sqlite3changeset_concat_strm
    @staticmethod
    def concat_stream(first: SessionStreamInput, second: SessionStreamInput, output: SessionStreamOutput):
        ...


class ChangesetBuilder:
    # wraps the changegroup apis

    # sqlite3changegroup_new
    def __init__(self):
        # store valid state flag - see schema for how it becomes invalid
        ...

    # sqlite3changegroup_delete
    def close(self): # Also __del__
        ...

    # sqlite3changegroup_schema
    def schema(self, connection, schema: str):
        ...


    # sqlite3changegroup_add
    # sqlite3changegroup_add_strm
    def add(self, changeset: bytes | SessionStreamInput): ...

    # sqlite3changegroup_add_change
    # adds from iterator or apply conflict callback
    def add_change(self, change: TableChange):
        ...

class TableChange:
    # each entry in changeset_iter
    # value to xConflict callback in apply

    # must have hidden pointer to sqlite3_changeset_iter so changegroup.add_change can be
    # called and appropriately go out of scope when the iterator advances

    # combine these for return value - PyStructSequence?
    # - sqlite3changeset_op
    # - for update or delete, iterate each column sqlite3changeset_old
    # - for update or insert, iterate each column sqlite3changeset_new
    # sqlite3changeset_pk?
    ...
