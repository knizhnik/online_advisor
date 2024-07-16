\echo Use "CREATE EXTENSION online_advisor" to load this file. \quit

-- Proposed indexes to avoid seqcans

CREATE TYPE index_candidate as (n_filtered float8, n_called bigint, elapsed_sec float8, create_index text);

CREATE FUNCTION propose_indexes(combine boolean default true, reset boolean default false)
RETURNS SETOF index_candidate
AS 'MODULE_PATHNAME', 'propose_indexes'
LANGUAGE C PARALLEL SAFE;

create view proposed_indexes as select * from propose_indexes();

-- Proposed extended statistics to fix misetimation

CREATE TYPE statistic_candidate as (misestimation float8, n_called bigint, elapsed_sec float8, create_statistics text);

CREATE FUNCTION propose_statistics(combine boolean default true, reset boolean default false)
RETURNS SETOF statistic_candidate
AS 'MODULE_PATHNAME', 'propose_statistics'
LANGUAGE C PARALLEL SAFE;

create view proposed_statistics as select * from propose_statistics();

