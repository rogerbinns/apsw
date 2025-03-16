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
    ('SQLITE_CHANGESETAPPLY_NOSAVEPOINT', 'constant', 0, 'Flags for sqlite3changeset_apply_v2', 'session/c_changesetapply_fknoaction.html'),
    ('SQLITE_CHANGESETAPPLY_INVERT', 'constant', 0, 'Flags for sqlite3changeset_apply_v2', 'session/c_changesetapply_fknoaction.html'),
    ('SQLITE_CHANGESETAPPLY_IGNORENOOP', 'constant', 0, 'Flags for sqlite3changeset_apply_v2', 'session/c_changesetapply_fknoaction.html'),
    ('SQLITE_CHANGESETAPPLY_FKNOACTION', 'constant', 0, 'Flags for sqlite3changeset_apply_v2', 'session/c_changesetapply_fknoaction.html'),

    ('SQLITE_CHANGESETSTART_INVERT', 'constant', 0, 'Flags for sqlite3changeset_start_v2', 'session/c_changesetstart_invert.html'),

    ('SQLITE_CHANGESET_OMIT', 'constant', 0, 'Constants Returned By The Conflict Handler', 'session/c_changeset_abort.html'),
    ('SQLITE_CHANGESET_REPLACE', 'constant', 0, 'Constants Returned By The Conflict Handler', 'session/c_changeset_abort.html'),
    ('SQLITE_CHANGESET_ABORT', 'constant', 0, 'Constants Returned By The Conflict Handler', 'session/c_changeset_abort.html'),

    ('SQLITE_CHANGESET_DATA', 'constant', 0, 'Constants Passed To The Conflict Handler', 'session/c_changeset_conflict.html'),
    ('SQLITE_CHANGESET_NOTFOUND', 'constant', 0, 'Constants Passed To The Conflict Handler', 'session/c_changeset_conflict.html'),
    ('SQLITE_CHANGESET_CONFLICT', 'constant', 0, 'Constants Passed To The Conflict Handler', 'session/c_changeset_conflict.html'),
    ('SQLITE_CHANGESET_CONSTRAINT', 'constant', 0, 'Constants Passed To The Conflict Handler', 'session/c_changeset_conflict.html'),
    ('SQLITE_CHANGESET_FOREIGN_KEY', 'constant', 0, 'Constants Passed To The Conflict Handler', 'session/c_changeset_conflict.html'),

    ('SQLITE_SESSION_CONFIG_STRMSIZE', 'constant', 0, 'Values for sqlite3session_config', 'session/c_session_config_strmsize.html'),

    ('SQLITE_SESSION_OBJCONFIG_SIZE', 'constant', 0, 'Options for sqlite3session_object_config', 'session/c_session_objconfig_rowid.html'),
    ('SQLITE_SESSION_OBJCONFIG_ROWID', 'constant', 0, 'Options for sqlite3session_object_config', 'session/c_session_objconfig_rowid.html'),

    -- functions

    ('sqlite3session_changeset_size', 'function', 0, 'Return An Upper-limit For The Size Of The Changeset', 'session/sqlite3session_changeset_size.html'),
    ('sqlite3session_config', 'function', 0, 'Configure global parameters', 'session/sqlite3session_config.html'),
    ('sqlite3session_create', 'function', 0, 'Create A New Session Object', 'session/sqlite3session_create.html'),
    ('sqlite3session_delete', 'function', 0, 'Delete A Session Object', 'session/sqlite3session_delete.html'),
    ('sqlite3session_diff', 'function', 0, 'Load The Difference Between Tables Into A Session', 'session/sqlite3session_diff.html'),
    ('sqlite3session_enable', 'function', 0, 'Enable Or Disable A Session Object', 'session/sqlite3session_enable.html'),
    ('sqlite3session_indirect', 'function', 0, 'Set Or Clear the Indirect Change Flag', 'session/sqlite3session_indirect.html'),
    ('sqlite3session_isempty', 'function', 0, 'Test if a changeset has recorded any changes', 'session/sqlite3session_isempty.html'),
    ('sqlite3session_memory_used', 'function', 0, 'Query for the amount of heap memory used by a session object', 'session/sqlite3session_memory_used.html'),
    ('sqlite3session_table_filter', 'function', 0, 'Set a table filter on a Session Object', 'session/sqlite3session_table_filter.html'),
    ('sqlite3session_attach', 'function', 0, 'Attach A Table To A Session Object', 'session/sqlite3session_attach.html'),
    ('sqlite3session_object_config', 'function', 0, 'Configure a Session Object', 'session/sqlite3session_object_config.html'),
    ('sqlite3session_changeset', 'function', 0, 'Generate A Changeset From A Session Object', 'session/sqlite3session_changeset.html'),
    ('sqlite3session_patchset', 'function', 0, 'Generate A Patchset From A Session Object', 'session/sqlite3session_patchset.html'),
    ('sqlite3session_patchset_strm', 'function', 0, 'Streaming Versions of API functions', 'session/sqlite3changegroup_add_strm.html'),
    ('sqlite3session_changeset_strm', 'function', 0, 'Streaming Versions of API functions', 'session/sqlite3changegroup_add_strm.html'),
    ('sqlite3changeset_invert', 'function', 0, 'Invert A Changeset', 'session/sqlite3changeset_invert.html'),
    ('sqlite3changeset_concat', 'function', 0, 'Concatenate Two Changeset Objects', 'session/sqlite3changeset_concat.html'),
    ('sqlite3changeset_start', 'function', 0, 'Create An Iterator To Traverse A Changeset', 'session/sqlite3changeset_start.html'),
    ('sqlite3changeset_start_v2', 'function', 0, 'Create An Iterator To Traverse A Changeset', 'session/sqlite3changeset_start.html'),
    ('sqlite3changeset_start_strm', 'function', 0, 'Streaming Versions of API functions', 'session/sqlite3changegroup_add_strm.html'),
    ('sqlite3changeset_start_v2_strm', 'function', 0, 'Streaming Versions of API functions', 'session/sqlite3changegroup_add_strm.html'),
    ('sqlite3changeset_new', 'function', 0, 'Obtain new.* Values From A Changeset Iterator', 'session/sqlite3changeset_new.html'),
    ('sqlite3changeset_old', 'function', 0, 'Obtain old.* Values From A Changeset Iterator', 'session/sqlite3changeset_old.html'),
    ('sqlite3changeset_op', 'function', 0, 'Obtain The Current Operation From A Changeset Iterator', 'session/sqlite3changeset_op.html'),
    ('sqlite3changeset_conflict', 'function', 0, 'Obtain Conflicting Row Values From A Changeset Iterator', 'session/sqlite3changeset_conflict.html'),
    ('sqlite3changeset_fk_conflicts', 'function', 0, 'Determine The Number Of Foreign Key Constraint Violations', 'session/sqlite3changeset_fk_conflicts.html'),
    ('sqlite3changeset_pk', 'function', 0, 'Obtain The Primary Key Definition Of A Table', 'session/sqlite3changeset_pk.html'),
    ('sqlite3changeset_apply', 'function', 0, 'Apply A Changeset To A Database', 'session/sqlite3changeset_apply.html'),
    ('sqlite3changeset_apply_v2', 'function', 0, 'Apply A Changeset To A Database', 'session/sqlite3changeset_apply.html'),
    ('sqlite3changeset_apply_strm', 'function', 0, 'Streaming Versions of API functions.', 'session/sqlite3changegroup_add_strm.html'),
    ('sqlite3changeset_apply_v2_strm', 'function', 0, 'Streaming Versions of API functions.', 'session/sqlite3changegroup_add_strm.html'),
    ('sqlite3changeset_invert_strm', 'function', 0, 'Streaming Versions of API functions', 'session/sqlite3changegroup_add_strm.html'),
    ('sqlite3changeset_concat_strm', 'function', 0, 'Streaming Versions of API functions', 'session/sqlite3changegroup_add_strm.html'),
    ('sqlite3changegroup_add', 'function', 0, 'Add A Changeset To A Changegroup', 'session/sqlite3changegroup_add.html'),
    ('sqlite3changegroup_add_change', 'function', 0, 'Add A Single Change To A Changegroup', 'session/sqlite3changegroup_add_change.html'),
    ('sqlite3changegroup_add_strm', 'function', 0, 'Streaming Versions of API functions.', 'session/sqlite3changegroup_add_strm.html'),
    ('sqlite3changegroup_delete', 'function', 0, 'Delete A Changegroup Object', 'session/sqlite3changegroup_delete.html'),
    ('sqlite3changegroup_new', 'function', 0, 'Create A New Changegroup Object', 'session/sqlite3changegroup_new.html'),
    ('sqlite3changegroup_output', 'function', 0, 'Obtain A Composite Changeset From A Changegroup', 'session/sqlite3changegroup_output.html'),
    ('sqlite3changegroup_output_strm', 'function', 0, 'Streaming Versions of API functions.', 'session/sqlite3changegroup_add_strm.html'),
    ('sqlite3changegroup_schema', 'function', 0, 'Add a Schema to a Changegroup', 'session/sqlite3changegroup_schema.html'),
    ('sqlite3rebaser_create', 'function', 1, 'Create a changeset rebaser object.', 'session/sqlite3rebaser_create.html'),
    ('sqlite3rebaser_delete', 'function', 1, 'Delete a changeset rebaser object.', 'session/sqlite3rebaser_delete.html'),
    ('sqlite3rebaser_configure', 'function', 1, 'Configure a changeset rebaser object.', 'session/sqlite3rebaser_configure.html'),
    ('sqlite3rebaser_rebase', 'function', 1, 'Rebase a changeset', 'session/sqlite3rebaser_rebase.html'),
    ('sqlite3rebaser_rebase_strm', 'function', 1, 'Streaming Versions of API functions.', 'session/sqlite3changegroup_add_strm.html')

    ;

-- treat rebase as not experimental
update toc set status = 0 where name like 'sqlite3rebaser_%';

delete from toc where name='SQLITE_TRACE' and title='SQL Trace Event Codes';
delete from toc where name='SQLITE_CONFIG_ROWID_IN_VIEW';