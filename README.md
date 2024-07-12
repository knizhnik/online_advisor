# online advisor

`online_advisor` is simple Postgres extension which tries to advice you creation of some extra indexes
which can increase speed of your queries.

### How it works:
`online_advisor` use mostly the same technique as standard Postgres extension `auto_explain`.
It sets up executor hook to enable instrumentation and at the end of query execution
detects quals filtering large number of records. It is assumed that such quals can be
replaced with index scan.

### What it does:
1. Detects plan nodes with number of filtered records exceeds some threshold.
2. Collects columns used in this predicate.
3. Propose index creation on this columns.
4. Tries to cover different sets of columns with one compound index.

### What it doesn't do:
1. Checks operators used in the predicate. For example, query predicate `x > 10 and y < 100` can not be optimized using compund index on (x,y). But right now `online_advisor` doesn't detect it.
2. Suggests indexes for table joins or eliminating sort for order by clauses.
3. Estimate effect of adding suggested index. There is another extension - hypothetical indexes https://github.com/HypoPG/hypopg# which allows to do it. It can be used in conjunction with `online_advisor`.

### Requirements:
1. `online_advisor` should be included in `preload_shared_libraries` list.
2. You should `create extension online_advisor` in each database you want to inspect.

### Tuning:
`online_advisor` has the following GUCs:
- "online_advisor.filtered_threshold": specifies threshold for number of filtered records (default 1000)
- "online_advisor.max_indexes" - maximal number of tracked filter clauses and so number of proposed indexes (default 128).
- "online_advisor.do_instrumentation" - allows to switch on/off instrumentation and so updating of statistics.

### Results:
1. Results can be obtained through `proposed_indexes` view having the following columns:
```sql
CREATE TYPE index_candidate as (
    n_filtered   float8, -- total number of filtered columns
	n_called     bigint, -- how much time this combination of columns was used
	elapsed_sec  float8, -- elapsed time spent in this nodes
	create_index text    -- statement to create proposed index
);

```
2. Suggested indexes are reported separately for each database.
3. By default `online_advisor` tries to propose single compound index covering different use cases.
For example, queries `select * from T where x=?` and `select * from T where x=? and y=?`
can use one compound index (x,y). But queries `select * from T where x=? and y=?` and
`select * from T where x=? and z=?` can not use compound index (x,y,z).
`online_advisor` also provides function `propose_indexes(combine boolean default true)`
which is used by `proposed_indexes` view. You can call it directly with `combine=false` to report
information separately for each set of columns and prevent `online_advisor` from
covering several cases using compound index.



