#!/bin/sh
#
# Update a repository's function names to follow the PEP 8 compliant
# scheme introduced in APSW version 3.42.2.
# Run the script in the root directory of the Git repository that
# requires the update.
# Before committing the results, inspect them carefully, and test
# the resulting code.
#
# Requires an installed version of git-subst;
# see https://github.com/dspinellis/git-subst/
#

set -eu

git subst '\<apswversion\(' 'apsw_version('
git subst '\<blobopen\(' 'blob_open('
git subst '\<cacheflush\(' 'cache_flush('
git subst '\<collationneeded\(' 'collation_needed('
git subst '\<createaggregatefunction\(' 'create_aggregate_function('
git subst '\<createcollation\(' 'create_collation('
git subst '\<createmodule\(' 'create_module('
git subst '\<createscalarfunction\(' 'create_scalar_function('
git subst '\<enableloadextension\(' 'enable_load_extension('
git subst '\<enablesharedcache\(' 'enable_shared_cache('
git subst '\<exceptionfor\(' 'exception_for('
git subst '\<exectrace\(' 'exec_trace('
git subst '\<filecontrol\(' 'file_control('
git subst '\<getautocommit\(' 'get_autocommit('
git subst '\<getconnection\(' 'get_connection('
git subst '\<getdescription\(' 'get_description('
git subst '\<getexectrace\(' 'get_exec_trace('
git subst '\<getrowtrace\(' 'get_row_trace('
git subst '\<loadextension\(' 'load_extension('
git subst '\<memoryhighwater\(' 'memory_high_water('
git subst '\<memoryused\(' 'memory_used('
git subst '\<overloadfunction\(' 'overload_function('
git subst '\<pagecount\(' 'page_count('
git subst '\<readinto\(' 'read_into('
git subst '\<releasememory\(' 'release_memory('
git subst '\<rowtrace\(' 'row_trace('
git subst '\<setauthorizer\(' 'set_authorizer('
git subst '\<setbusyhandler\(' 'set_busy_handler('
git subst '\<setbusytimeout\(' 'set_busy_timeout('
git subst '\<setcommithook\(' 'set_commit_hook('
git subst '\<setexectrace\(' 'set_exec_trace('
git subst '\<setprofile\(' 'set_profile('
git subst '\<setprogresshandler\(' 'set_progress_handler('
git subst '\<setrollbackhook\(' 'set_rollback_hook('
git subst '\<setrowtrace\(' 'set_row_trace('
git subst '\<setupdatehook\(' 'set_update_hook('
git subst '\<setwalhook\(' 'set_wal_hook('
git subst '\<softheaplimit\(' 'soft_heap_limit('
git subst '\<sqlite3pointer\(' 'sqlite3_pointer('
git subst '\<sqlitelibversion\(' 'sqlite_lib_version('
git subst '\<totalchanges\(' 'total_changes('
git subst '\<vfsnames\(' 'vfs_names('
