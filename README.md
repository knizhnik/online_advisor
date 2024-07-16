# online advisor

`online_advisor` is simple Postgres extension which tries to advice you creation of some extra indexes and extended statistics
which can increase speed of your queries.

### How it works:
`online_advisor` use mostly the same technique as standard Postgres extension `auto_explain`.
It sets up executor hook to enable instrumentation and analyze instrumentation results at the end of query execution.
Right now `online_advisor` detects two patters:
1. Quals filtering large number of records. It is assumed that such quals can be replaced with index scan.
2. Misestimated clauses: nodes with number of estimated rows significantly smaller than actual number of returned rows.
It usually caused by lack/inaccuracy of statistics or lack of knowledge about correlation between columxns. It can be addressed
by creating extended (multicolumn) statistics.

### What it does:
Create index proposals:
1. Detects plan nodes with number of filtered records exceeds some threshold.
2. Collects columns used in this predicate.
3. Proposes index creation on this columns.
4. Tries to cover different sets of columns with one compound index.
5. Checks if such index already exists.

Create extended statistic proposals:
1. Detects plan node with actual/estimated returned rows number exceeds some threshold.
2. Collects columns accessed by this node.
3. Proposes create extended statistics statement fort this columns.
4. Checks if such statistics already available.


### What it doesn't do:
1. Checks operators used in the predicate. For example, query predicate `x > 10 and y < 100` can not be optimized using compound index on (x,y). But right now `online_advisor` doesn't detect it.
2. Suggests indexes for table joins or eliminating sort for order by clauses.
3. Estimate effect of adding suggested index. There is another extension - hypothetical indexes https://github.com/HypoPG/hypopg# which allows to do it. It can be used in conjunction with `online_advisor`.
4. `online_advisor` doesn't create indexes or extended statistics itself. It just makes recommendations to create them. Please also notice that
to make optimizer use created indexes or statistics, you should better explicitly call `vacuum analyze` for this table.

### Requirements:
1. `online_advisor` should be included in `preload_shared_libraries` list.
2. You should `create extension `online_advisor` in each database you want to inspect.

### Tuning:
`online_advisor` has the following GUCs:
- "online_advisor.filtered_threshold": specifies threshold for number of filtered records (default 1000)
- "online_advisor.misestimation_threshold": threshold for actual/estimated #rows ratio (default 10)
- "online_advisor.min_rows": minimal number of returns nodes for which misestimation is considered (default 1000)
- "online_advisor.max_proposals": maximal number of tracked clauses and so number of proposed indexes/statistics (default 1000).
- "online_advisor.do_instrumentation": allows to switch on/off instrumentation and so collecting of data by `online_advisor`.

### Results:
1. Results can be obtained through `proposed_indexes` and `proposed_statistics` views having the following columns:
```sql
CREATE TYPE index_candidate as (
    n_filtered    float8, -- total number of filtered columns
	n_called      bigint, -- how much time this combination of columns was used
	elapsed_sec   float8, -- elapsed time spent in this nodes
	create_index  text    -- statement to create proposed index
);

CREATE TYPE statistic_candidate as (
    misestimation float8, -- maximal actual/estimated ratio
	n_called      bigint, -- how much time this combination of columns was used
	elapsed_sec   float8, -- elapsed time spent in this nodes
	create_statistics text    -- statement to create proposed extended statistics
);

```
2. Suggested indexes and statistics are reported separately for each database.
3. By default `online_advisor` tries to propose single compound index covering different use cases.
For example, queries `select * from T where x=?` and `select * from T where x=? and y=?`
can use one compound index (x,y). But queries `select * from T where x=? and y=?` and
`select * from T where x=? and z=?` can not use compound index (x,y,z).
`online_advisor` also provides function `propose_indexes(combine boolean default true)`
which is used by `proposed_indexes` view. You can call it directly with `combine=false` to report
information separately for each set of columns and prevent `online_advisor` from
covering several cases using compound index.



