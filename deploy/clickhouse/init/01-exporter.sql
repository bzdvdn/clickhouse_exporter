# Demo-only exporter user. In production use a password and grant SELECT only
# on the databases you monitor.
CREATE USER IF NOT EXISTS exporter IDENTIFIED WITH no_password HOST ANY;

-- SELECT on *.* is what makes system.parts, system.tables, system.mutations,
-- system.merges and system.replicas show the production tables. With SELECT on
-- system.* only, those tables are filtered by table-level access and appear empty.
GRANT SELECT ON *.* TO exporter;

ALTER USER exporter SETTINGS
    readonly = 2,
    log_queries = 0,
    log_query_threads = 0,
    max_execution_time = 5,
    max_threads = 2;
