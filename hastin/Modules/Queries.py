"""PostgreSQL and PgBouncer query definitions for Hastin monitoring dashboard."""

from dataclasses import dataclass


@dataclass
class PgBouncerQueries:
    """Query definitions for PgBouncer monitoring."""

    # Version detection
    show_version: str = "SHOW VERSION"

    # Aggregate stats per database
    show_stats: str = "SHOW STATS"

    # Pool info (clients and servers per pool)
    show_pools: str = "SHOW POOLS"

    # Client connections
    show_clients: str = "SHOW CLIENTS"

    # Backend server connections
    show_servers: str = "SHOW SERVERS"

    # Configuration
    show_config: str = "SHOW CONFIG"

    # Database definitions
    show_databases: str = "SHOW DATABASES"

    # List of active sockets
    show_lists: str = "SHOW LISTS"


@dataclass
class PostgresQueries:
    """Query definitions for PostgreSQL monitoring."""

    # Processlist (pg_stat_activity)
    processlist: str = """
        SELECT
            pid,
            usename AS user,
            datname AS database,
            COALESCE(client_addr::text, 'local') AS host,
            application_name AS application,
            state,
            EXTRACT(EPOCH FROM (now() - query_start))::int AS time,
            wait_event_type,
            wait_event,
            query,
            backend_type
        FROM pg_stat_activity
        WHERE pid != pg_backend_pid()
          AND state IS NOT NULL
          AND backend_type = 'client backend'
        ORDER BY query_start ASC NULLS LAST
    """

    # Processlist with additional filters placeholder
    processlist_filtered: str = """
        SELECT
            pid,
            usename AS user,
            datname AS database,
            COALESCE(client_addr::text, 'local') AS host,
            application_name AS application,
            state,
            EXTRACT(EPOCH FROM (now() - query_start))::int AS time,
            wait_event_type,
            wait_event,
            query,
            backend_type
        FROM pg_stat_activity
        WHERE pid != pg_backend_pid()
          AND state IS NOT NULL
          AND backend_type = 'client backend'
          {filters}
        ORDER BY query_start ASC NULLS LAST
    """

    # Server info
    server_info: str = """
        SELECT
            version() AS version,
            current_setting('server_version') AS server_version,
            current_setting('server_version_num')::int AS server_version_num,
            pg_postmaster_start_time() AS start_time,
            EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time()))::int AS uptime_seconds,
            current_database() AS current_db,
            COALESCE(inet_server_addr()::text, 'local') AS server_addr,
            COALESCE(inet_server_port(), 5432) AS server_port,
            current_user AS current_user,
            pg_is_in_recovery() AS is_replica
    """

    # Connection stats
    connection_stats: str = """
        SELECT
            count(*) AS total_connections,
            count(*) FILTER (WHERE state = 'active') AS active,
            count(*) FILTER (WHERE state = 'idle') AS idle,
            count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_transaction,
            count(*) FILTER (WHERE state = 'idle in transaction (aborted)') AS idle_in_transaction_aborted,
            count(*) FILTER (WHERE wait_event_type IS NOT NULL AND state = 'active') AS waiting,
            (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') AS max_connections,
            (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'autovacuum worker') AS autovacuum_workers,
            (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'background worker') AS background_workers
        FROM pg_stat_activity
        WHERE backend_type = 'client backend'
    """

    # Database stats
    database_stats: str = """
        SELECT
            datname,
            numbackends,
            xact_commit,
            xact_rollback,
            blks_read,
            blks_hit,
            CASE WHEN blks_read + blks_hit > 0
                 THEN round(100.0 * blks_hit / (blks_read + blks_hit), 2)
                 ELSE 0 END AS cache_hit_ratio,
            tup_returned,
            tup_fetched,
            tup_inserted,
            tup_updated,
            tup_deleted,
            conflicts,
            deadlocks,
            temp_files,
            temp_bytes,
            blk_read_time,
            blk_write_time
        FROM pg_stat_database
        WHERE datname = current_database()
    """

    # Streaming replication status (on primary)
    replication_status_primary: str = """
        SELECT
            pid,
            usename,
            application_name,
            COALESCE(client_addr::text, 'local') AS client_addr,
            state,
            sent_lsn,
            write_lsn,
            flush_lsn,
            replay_lsn,
            pg_wal_lsn_diff(sent_lsn, replay_lsn) AS replication_lag_bytes,
            sync_state,
            sync_priority
        FROM pg_stat_replication
        ORDER BY application_name
    """

    # WAL receiver status (on replica)
    replication_status_replica: str = """
        SELECT
            pid,
            status,
            receive_start_lsn,
            receive_start_tli,
            received_lsn,
            received_tli,
            last_msg_send_time,
            last_msg_receipt_time,
            latest_end_lsn,
            latest_end_time,
            pg_wal_lsn_diff(latest_end_lsn, received_lsn) AS lag_bytes,
            slot_name,
            sender_host,
            sender_port,
            conninfo
        FROM pg_stat_wal_receiver
    """

    # Logical replication subscriptions
    logical_subscriptions: str = """
        SELECT
            subname,
            pid,
            received_lsn,
            latest_end_lsn,
            pg_wal_lsn_diff(latest_end_lsn, received_lsn) AS lag_bytes,
            last_msg_send_time,
            last_msg_receipt_time
        FROM pg_stat_subscription
    """

    # Replication slots
    replication_slots: str = """
        SELECT
            slot_name,
            plugin,
            slot_type,
            database,
            active,
            active_pid,
            xmin,
            catalog_xmin,
            restart_lsn,
            confirmed_flush_lsn,
            pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS slot_lag_bytes
        FROM pg_replication_slots
        ORDER BY slot_name
    """

    # Lock monitoring (blocked queries)
    blocked_queries: str = """
        SELECT
            blocked_locks.pid AS blocked_pid,
            blocked_activity.usename AS blocked_user,
            blocked_activity.datname AS database,
            blocked_locks.locktype AS lock_type,
            blocked_locks.mode AS blocked_mode,
            blocking_locks.pid AS blocking_pid,
            blocking_activity.usename AS blocking_user,
            blocking_locks.mode AS blocking_mode,
            blocked_activity.query AS blocked_query,
            blocking_activity.query AS blocking_query,
            EXTRACT(EPOCH FROM (now() - blocked_activity.query_start))::int AS blocked_duration,
            blocked_activity.wait_event_type,
            blocked_activity.wait_event
        FROM pg_locks blocked_locks
        JOIN pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
        JOIN pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
            AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
            AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
            AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
            AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
            AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
            AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
            AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
            AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
            AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
            AND blocking_locks.pid != blocked_locks.pid
        JOIN pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
        WHERE NOT blocked_locks.granted
        ORDER BY blocked_activity.query_start
    """

    # pg_stat_statements (optional - requires extension)
    statement_stats: str = """
        SELECT
            queryid,
            LEFT(query, 200) AS query_preview,
            calls,
            total_exec_time,
            mean_exec_time,
            min_exec_time,
            max_exec_time,
            stddev_exec_time,
            rows,
            shared_blks_hit,
            shared_blks_read,
            shared_blks_written,
            temp_blks_read,
            temp_blks_written,
            CASE WHEN shared_blks_hit + shared_blks_read > 0
                 THEN round(100.0 * shared_blks_hit / (shared_blks_hit + shared_blks_read), 2)
                 ELSE 0 END AS cache_hit_ratio
        FROM pg_stat_statements
        ORDER BY total_exec_time DESC
        LIMIT 50
    """

    # Statement type counts from pg_stat_statements (SELECT, INSERT, UPDATE, DELETE)
    statement_type_counts: str = """
        SELECT
            COALESCE(SUM(calls) FILTER (WHERE query ~* '^\\s*(select|with.*select)'), 0) AS select_calls,
            COALESCE(SUM(calls) FILTER (WHERE query ~* '^\\s*insert'), 0) AS insert_calls,
            COALESCE(SUM(calls) FILTER (WHERE query ~* '^\\s*update'), 0) AS update_calls,
            COALESCE(SUM(calls) FILTER (WHERE query ~* '^\\s*delete'), 0) AS delete_calls
        FROM pg_stat_statements
    """

    # system_stats extension queries (for system metrics collection)
    system_stats_cpu: str = """
        SELECT * FROM sys_cpu_info()
    """

    system_stats_memory: str = """
        SELECT * FROM sys_memory_info()
    """

    system_stats_disk: str = """
        SELECT * FROM sys_disk_info()
    """

    system_stats_load: str = """
        SELECT * FROM sys_load_avg_info()
    """

    # Permission check
    permission_check: str = """
        SELECT
            has_table_privilege('pg_stat_activity', 'SELECT') AS can_select_activity,
            pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER') AS has_read_all_stats,
            pg_has_role(current_user, 'pg_monitor', 'MEMBER') AS has_pg_monitor,
            (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) AS is_superuser
    """

    # Environment detection (cloud providers)
    environment_detection: str = """
        SELECT
            -- AWS
            current_setting('rds.extensions', true) IS NOT NULL AS is_rds,
            EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'aurora_stat_utils') AS is_aurora,
            current_setting('rds.logical_replication', true) AS rds_logical_replication,
            -- Google Cloud
            current_setting('cloudsql.iam_authentication', true) IS NOT NULL AS is_cloud_sql,
            current_setting('alloydb.logical_decoding', true) IS NOT NULL
                OR EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'google_columnar_engine') AS is_alloydb,
            -- Azure
            current_setting('azure.extensions', true) IS NOT NULL AS is_azure,
            EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'citus') AS is_citus,
            -- Supabase (pgsodium or supautils extensions)
            EXISTS (SELECT 1 FROM pg_extension WHERE extname IN ('pgsodium', 'supautils')) AS is_supabase,
            -- Neon
            current_setting('neon.timeline_id', true) IS NOT NULL
                OR EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'neon') AS is_neon,
            -- Timescale
            EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') AS has_timescaledb,
            -- Crunchy Bridge (crunchy_check or pg_cron from crunchy)
            EXISTS (SELECT 1 FROM pg_extension WHERE extname LIKE 'crunchy%') AS is_crunchy
    """

    # PostgreSQL settings (variables command for fetch_status_and_variables)
    variables: str = """
        SELECT name, setting
        FROM pg_settings
        ORDER BY name
    """

    # Background writer stats
    bgwriter_stats: str = """
        SELECT
            checkpoints_timed,
            checkpoints_req,
            checkpoint_write_time,
            checkpoint_sync_time,
            buffers_checkpoint,
            buffers_clean,
            maxwritten_clean,
            buffers_backend,
            buffers_backend_fsync,
            buffers_alloc
        FROM pg_stat_bgwriter
    """

    # Active locks (pg_locks with activity info)
    locks: str = """
        SELECT
            l.pid,
            l.locktype,
            d.datname AS database,
            COALESCE(c.relname, l.relation::text) AS relation,
            l.mode,
            l.granted,
            a.wait_event,
            a.state,
            LEFT(a.query, 200) AS query
        FROM pg_locks l
        LEFT JOIN pg_stat_activity a ON l.pid = a.pid
        LEFT JOIN pg_database d ON l.database = d.oid
        LEFT JOIN pg_class c ON l.relation = c.oid
        WHERE l.pid != pg_backend_pid()
        ORDER BY l.granted, l.pid
        LIMIT 100
    """

    # List databases
    databases: str = """
        SELECT datname
        FROM pg_database
        WHERE datistemplate = false
        ORDER BY datname
    """

    # User statistics (connections by user)
    user_stats: str = """
        SELECT
            usename,
            count(*) FILTER (WHERE state = 'active') AS active,
            count(*) FILTER (WHERE state = 'idle') AS idle,
            count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_transaction,
            count(*) AS total
        FROM pg_stat_activity
        WHERE backend_type = 'client backend'
        GROUP BY usename
        ORDER BY total DESC
    """

    # Table sizes
    table_sizes: str = """
        SELECT
            schemaname,
            tablename,
            pg_total_relation_size(schemaname || '.' || tablename) AS total_size,
            pg_table_size(schemaname || '.' || tablename) AS table_size,
            pg_indexes_size(schemaname || '.' || tablename) AS index_size,
            (SELECT reltuples::bigint FROM pg_class
                WHERE oid = (schemaname || '.' || tablename)::regclass) AS row_estimate
        FROM pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC
        LIMIT 50
    """
