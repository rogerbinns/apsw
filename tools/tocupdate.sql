insert into toc values
    -- fts5 stuff
    ('FTS5_TOKENIZE_DOCUMENT', 'constant', 0, 'FTS5 Tokenize Reason', 'fts5.html#custom_tokenizers'),
    ('FTS5_TOKENIZE_QUERY', 'constant', 0, 'FTS5 Tokenize Reason', 'fts5.html#custom_tokenizers'),
    ('FTS5_TOKENIZE_PREFIX', 'constant', 0, 'FTS5 Tokenize Reason', 'fts5.html#custom_tokenizers'),
    ('FTS5_TOKENIZE_AUX', 'constant', 0, 'FTS5 Tokenize Reason', 'fts5.html#custom_tokenizers'),

    ('FTS5_TOKEN_COLOCATED', 'constant', 0, 'FTS5 Token Flag', 'fts5.html#synonym_support'),

    -- error code and authorizer code
    ('SQLITE_OK', 'constant', 0, 'Authorizer Return Codes', 'c3ref/c_deny.html'),
    -- error code and conflict
    ('SQLITE_IGNORE', 'constant', 0, 'Conflict resolution modes', 'c3ref/c_fail.html'),
    ('SQLITE_ABORT', 'constant', 0, 'Conflict resolution modes', 'c3ref/c_fail.html'),

    -- lots of session stuff, constants first
    ("SQLITE_CHANGESETAPPLY_NOSAVEPOINT", 'constant', 0, "Flags for sqlite3changeset_apply_v2", "session/c_changesetapply_fknoaction.html"),
    ("SQLITE_CHANGESETAPPLY_INVERT", 'constant', 0, "Flags for sqlite3changeset_apply_v2", "session/c_changesetapply_fknoaction.html"),
    ("SQLITE_CHANGESETAPPLY_IGNORENOOP", 'constant', 0, "Flags for sqlite3changeset_apply_v2", "session/c_changesetapply_fknoaction.html"),
    ("SQLITE_CHANGESETAPPLY_FKNOACTION", 'constant', 0, "Flags for sqlite3changeset_apply_v2", "session/c_changesetapply_fknoaction.html"),

    ("SQLITE_CHANGESETSTART_INVERT", 'constant', 0, "Flags for sqlite3changeset_start_v2", "session/c_changesetstart_invert.html"),

    ("SQLITE_CHANGESET_OMIT", 'constant', 0, "Constants Returned By The Conflict Handler", "session/c_changeset_abort.html"),
    ("SQLITE_CHANGESET_REPLACE", 'constant', 0, "Constants Returned By The Conflict Handler", "session/c_changeset_abort.html"),
    ("SQLITE_CHANGESET_ABORT", 'constant', 0, "Constants Returned By The Conflict Handler", "session/c_changeset_abort.html"),

    ("SQLITE_CHANGESET_DATA", 'constant', 0, "Constants Passed To The Conflict Handler", "session/c_changeset_conflict.html"),
    ("SQLITE_CHANGESET_NOTFOUND", 'constant', 0, "Constants Passed To The Conflict Handler", "session/c_changeset_conflict.html"),
    ("SQLITE_CHANGESET_CONFLICT", 'constant', 0, "Constants Passed To The Conflict Handler", "session/c_changeset_conflict.html"),
    ("SQLITE_CHANGESET_CONSTRAINT", 'constant', 0, "Constants Passed To The Conflict Handler", "session/c_changeset_conflict.html"),
    ("SQLITE_CHANGESET_FOREIGN_KEY", 'constant', 0, "Constants Passed To The Conflict Handler", "session/c_changeset_conflict.html"),

    ("SQLITE_SESSION_CONFIG_STRMSIZE", 'constant', 0, "Values for sqlite3session_config", "session/c_session_config_strmsize.html"),

    ("SQLITE_SESSION_OBJCONFIG_SIZE", 'constant', 0, "Options for sqlite3session_object_config", "session/c_session_objconfig_rowid.html"),
    ("SQLITE_SESSION_OBJCONFIG_ROWID", 'constant', 0, "Options for sqlite3session_object_config", "session/c_session_objconfig_rowid.html"),

    -- functions

    ("sqlite3session_changeset_size", "function", 0, "Return An Upper-limit For The Size Of The Changeset", "session/sqlite3session_changeset_size.html"),
    ("sqlite3session_config", "function", 0, "Configure global parameters", "session/sqlite3session_config.html"),
    ("sqlite3session_delete", "function", 0, "Delete A Session Object", "session/sqlite3session_delete.html"),
    ("sqlite3session_diff", "function", 0, "Load The Difference Between Tables Into A Session", "session/sqlite3session_diff.html"),
    ("sqlite3session_enable", "function", 0, "Enable Or Disable A Session Object", "session/sqlite3session_enable.html"),
    ("sqlite3session_indirect", "function", 0, "Set Or Clear the Indirect Change Flag", "session/sqlite3session_indirect.html"),
    ("sqlite3session_isempty", "function", 0, "Test if a changeset has recorded any changes", "session/sqlite3session_isempty.html"),
    ("sqlite3session_memory_used", "function", 0, "Query for the amount of heap memory used by a session object", "session/sqlite3session_memory_used.html"),
    ("sqlite3session_table_filter", "function", 0, "Set a table filter on a Session Object", "session/sqlite3session_table_filter.html"),
    ("sqlite3session_attach", "function", 0, "Attach A Table To A Session Object", "session/sqlite3session_attach.html"),
    ("sqlite3session_object_config", "function", 0, "Configure a Session Object", "session/sqlite3session_object_config.html"),
    ("sqlite3session_changeset", "function", 0, "Generate A Changeset From A Session Object", "session/sqlite3session_changeset.html"),
    ("sqlite3session_patchset", "function", 0, "Generate A Patchset From A Session Object", "session/sqlite3session_patchset.html"),
    ("sqlite3session_patchset_strm", "function", 0, "Streaming Versions of API functions", "session/sqlite3changegroup_add_strm.html"),
    ("sqlite3session_changeset_strm", "function", 0, "Streaming Versions of API functions", "session/sqlite3changegroup_add_strm.html"),
    ("sqlite3changeset_invert", "function", 0, "Invert A Changeset", "session/sqlite3changeset_invert.html"),
    ("sqlite3changeset_start", "function", 0, "Create An Iterator To Traverse A Changeset", "session/sqlite3changeset_start.html"),
    ("sqlite3changeset_start_v2", "function", 0, "Create An Iterator To Traverse A Changeset", "session/sqlite3changeset_start.html"),
    ("sqlite3changeset_start_strm", "function", 0, "Streaming Versions of API functions", "session/sqlite3changegroup_add_strm.html"),
    ("sqlite3changeset_start_v2_strm", "function", 0, "Streaming Versions of API functions", "session/sqlite3changegroup_add_strm.html")


    ;

delete from toc where name='SQLITE_TRACE' and title='SQL Trace Event Codes';
delete from toc where name='SQLITE_CONFIG_ROWID_IN_VIEW';