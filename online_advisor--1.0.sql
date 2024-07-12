\echo Use "CREATE EXTENSION online_advisor" to load this file. \quit

CREATE TYPE index_candidate as (n_filtered float8, n_called bigint, elapsed_sec float8, create_index text);

CREATE FUNCTION propose_indexes(combine boolean default true, reset boolean default false)
RETURNS SETOF index_candidate
AS 'MODULE_PATHNAME', 'propose_indexes'
LANGUAGE C PARALLEL SAFE;

create view proposed_indexes as select * from propose_indexes();

