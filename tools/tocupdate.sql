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
    ('SQLITE_ABORT', 'constant', 0, 'Conflict resolution modes', 'c3ref/c_fail.html')

    ;

delete from toc where name='SQLITE_TRACE' and title='SQL Trace Event Codes';
delete from toc where name='SQLITE_CONFIG_ROWID_IN_VIEW';