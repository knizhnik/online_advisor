/*-------------------------------------------------------------------------
 *
 * online_advisor.c
 *
 *
 * Copyright (c) 2008-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  online_advisor/online_advisor.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "access/hash.h"
#include "access/attnum.h"
#include "access/skey.h"
#include "access/relscan.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/tableam.h"
#include "commands/defrem.h"
#include "executor/instrument.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/planmain.h"
#include "parser/parsetree.h"
#include "port/pg_bitutils.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/ruleutils.h"

PG_MODULE_MAGIC;

/* GUC variables */
static bool do_instrumentation = false;
static int  filtered_threshold = 1000;
static int  max_proposals = 1000;
static double misestimation_threshold = 10.0;
static double min_rows = 1000;

/* Saved hook values in case of unload */
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
#if PG_VERSION_NUM>=150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
#endif

/*
 * online_advisor maintains bitmapset in shared memory,
 * so better to make them fixed size. Size of bitmaopset determines
 * maximal number of attributes online_advise can handle.
 * 128 seems tp be larger enough.
 */
#define FIXED_SET_SIZE 128 /* must be power of 2 */
#define FIXED_SET_WORDS (FIXED_SET_SIZE/BITS_PER_BITMAPWORD)

#define WORDNUM(x)	((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x)	((x) % BITS_PER_BITMAPWORD)

typedef struct {
	bitmapword words[FIXED_SET_WORDS];
} FixedBitmapset;

static void
fbms_add_member(FixedBitmapset* set, int mbr)
{
	Assert(mbr < FIXED_SET_SIZE);
	set->words[WORDNUM(mbr)] |= (bitmapword)1 << BITNUM(mbr);
}

/*
 * fbms_next_member - find next member of a set
 *
 * Returns smallest member greater than "prevbit", or -1 if there is none.
 * "prevbit" must NOT be less than -1, or the behavior is unpredictable.
 *
 * This is intended as support for iterating through the members of a set.
 * The typical pattern is
 *
 *			x = -1;
 *			while ((x = bms_next_member(inputset, x)) >= 0)
 *				process member x;
 *
 */
static int
fbms_next_member(FixedBitmapset const* set, int prevbit)
{
	int			wordnum;
	bitmapword	mask;
	prevbit += 1;
	mask = (~(bitmapword) 0) << BITNUM(prevbit);
	for (wordnum = WORDNUM(prevbit); wordnum < FIXED_SET_WORDS; wordnum++)
	{
		bitmapword	w = set->words[wordnum];

		/* ignore bits before prevbit */
		w &= mask;

		if (w != 0)
			return wordnum * BITS_PER_BITMAPWORD + bmw_rightmost_one_pos(w);

		/* in subsequent words, consider all bits */
		mask = (~(bitmapword) 0);
	}
	return -1;
}

/*
 * fbms_is_subset - is A a subset of B?
 */
static bool
fbms_is_subset(const FixedBitmapset *a, const FixedBitmapset *b)
{
	for (size_t i = 0; i < FIXED_SET_WORDS; i++)
	{
		if ((a->words[i] & ~b->words[i]) != 0)
			return false;
	}
	return true;
}

/*
 * fbms_is_member - is mbr a member of A?
 */
static bool
fbms_is_member(int mbr, const FixedBitmapset *set)
{
	Assert(mbr < FIXED_SET_SIZE);
	return (set->words[WORDNUM(mbr)] & ((bitmapword)1 << BITNUM(mbr))) != 0;
}


/*
 * fbms_num_members - count members of set
 */
static int
fbms_num_members(const FixedBitmapset *a)
{
	int	result = 0;
	for (size_t i = 0; i < FIXED_SET_WORDS; i++)
	{
		bitmapword	w = a->words[i];

		/* No need to count the bits in a zero word */
		if (w != 0)
			result += bmw_popcount(w);
	}
	return result;
}


typedef struct
{
	uint64			n_calls;    /* number of queries using such filter */
	double			agg;        /* aggregate value associated with this clause (number of filtered rows or maximal misestimation) */
	double			elapsed;    /* total time in seconds spent in filtering this condfition */
	Oid				dbid;       /* database identifier */
	Oid				relid;      /* relation ID */
	FixedBitmapset	key_set;    /* Set of variable used in filter clause */
} FilterClause;

typedef struct
{
	size_t          n_clauses;
	FilterClause*   filter_clauses;
} Proposal;

typedef struct
{
	slock_t			spinlock; /* Spinlock to synchronize access */
	Proposal        indexes;    /* index proposals */
	Proposal        statistics; /* extended statistics proposals */
} AdvisorState;

AdvisorState* state;

static void advisor_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void advisor_ExecutorEnd(QueryDesc *queryDesc);

static size_t
advisor_shmem_size(void)
{
	return sizeof(AdvisorState) + max_proposals*2*sizeof(FilterClause);
}

static void
advisor_shmem_request(void)
{
#if PG_VERSION_NUM>=150000
	if (prev_shmem_request_hook) {
		prev_shmem_request_hook();
    }
#endif

	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in advisor_shmem_startup().
	 */
	RequestAddinShmemSpace(advisor_shmem_size());
}

static void
advisor_shmem_startup(void)
{
	bool found;

	if (prev_shmem_startup_hook)
	{
		prev_shmem_startup_hook ();
    }
	/*
	 * Create or attach to the shared memory state
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	state = ShmemInitStruct("online_advisor",
							advisor_shmem_size(),
							&found);
	if (!found)
	{
		state->indexes.filter_clauses = (FilterClause*)(state + 1);
		state->indexes.n_clauses = 0;
		state->statistics.filter_clauses = state->indexes.filter_clauses + max_proposals;
		state->statistics.n_clauses = 0;
		SpinLockInit(&state->spinlock);
	}
	LWLockRelease(AddinShmemInitLock);
}


/*
 * Module load callback
 */
void
_PG_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.  If not, fall out without hooking into any of
	 * the main system.  (We don't throw error here because it seems useful to
	 * allow the online_advizor functions to be created even when the
	 * module isn't active.  The functions must protect themselves against
	 * being called then, however.)
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	/* Define custom GUC variables. */
	DefineCustomIntVariable("online_advisor.filtered_threshold",
							"Minimum number of filtered records which triggers index suggestion.",
							"Zero disdable this rule",
							&filtered_threshold,
							1000,
							1, INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("online_advisor.max_proposals",
							"Maximal number of clauses which are tracked by online_advistor and so number of proposals to create indexes/extended statistics.",
							NULL,
							&max_proposals,
							1000,
							1, INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("online_advisor.do_instrumentation",
							 "Perform query instrumentation to get number of filtered records.",
							 NULL,
							 &do_instrumentation,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("online_advisor.misestimation_threshold",
							 "Threshold for actual/estimated #rows ratio triggering extended statistic suggestion.",
							 "Zero disables this rule",
							 &misestimation_threshold,
							 10.0,
							 0.0,
							 INT_MAX,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("online_advisor.min_rows",
							 "Minimal number of rows to check misestimation",
							 "Zero disables this rule",
							 &min_rows,
							 1000.0,
							 0.0,
							 INT_MAX,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	MarkGUCPrefixReserved("online_advisor");

#if PG_VERSION_NUM>=150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = advisor_shmem_request;
#else
	advisor_shmem_request();
#endif
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = advisor_shmem_startup;


	/* Install hooks. */
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = advisor_ExecutorStart;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = advisor_ExecutorEnd;
}

static void AnalyzeNode(QueryDesc *queryDesc, PlanState *planstate);

/**
 * Try to add multicolumn statistic for specified subplans.
 */
static void
AnalyzeSubPlans(QueryDesc *queryDesc, List *plans)
{
	ListCell   *lst;

	foreach(lst, plans)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lst);

		AnalyzeNode(queryDesc, sps->planstate);
	}
}

/**
 * Try to add multicolumn statistic for plan subnodes.
 */
static void
AnalyzeMemberNodes(QueryDesc *queryDesc, PlanState **planstates, int nsubnodes)
{
	for (int j = 0; j < nsubnodes; j++)
	{
		AnalyzeNode(queryDesc, planstates[j]);
	}
}

typedef double (*aggregate_func)(double acc, double val);

static double agg_sum(double acc, double val)
{
	return acc + val;
}

static double agg_max(double acc, double val)
{
	return acc > val ? acc : val;
}


/**
 * Check number of filtered records for the qual
 */
static void
AddProposal(Proposal* prop, QueryDesc *queryDesc, List* qual, double value, size_t min_vars, aggregate_func aggregate)
{
	List* rtable = queryDesc->plannedstmt->rtable;
	List *vars = NULL;
	ListCell* lc;

	/* Extract vars from all quals */
	foreach (lc, qual)
	{
		Node* node = (Node*)lfirst(lc);
		if (IsA(node, RestrictInfo))
			node = (Node*)((RestrictInfo*)node)->clause;
		vars = list_concat(vars, pull_vars_of_level(node, 0));
	}

	/* Loop until we considered all vars */
	while (vars != NULL)
	{
		ListCell *cell;
		Index relno = 0;
		FixedBitmapset colmap;
		memset(&colmap, 0, sizeof(colmap));

		/* Contruct set of used vars */
		foreach (cell, vars)
		{
			Node* node = (Node *) lfirst(cell);
			if (IsA(node, Var))
			{
				Var *var = (Var *) node;
				int varno = IS_SPECIAL_VARNO(var->varno) ? var->varnosyn : var->varno;
				if (relno == 0 || varno == relno)
				{
					int varattno = IS_SPECIAL_VARNO(var->varno) ? var->varattnosyn : var->varattno;
					relno = varno;
					if (var->varattno > 0 &&
						var->varattno < FIXED_SET_SIZE &&
						!fbms_is_member(varattno, &colmap) &&
						varno >= 1 && /* not synthetic var */
						varno <= list_length(rtable))
					{
						RangeTblEntry *rte = rt_fetch(varno, rtable);
						if (rte->rtekind == RTE_RELATION)
						{
							fbms_add_member(&colmap, varattno);
						}
					}
				}
				else
				{
					continue;
				}
			}
			vars = foreach_delete_current(vars, cell);
		}
		if (fbms_num_members(&colmap) >= min_vars)
		{
			RangeTblEntry *rte = rt_fetch(relno, rtable);
			Oid relid = rte->relid;
			bool found = false;
			int i;
			int min = -1;
			for (i = 0; i < prop->n_clauses; i++)
			{
				if (prop->filter_clauses[i].relid == relid &&
					prop->filter_clauses[i].dbid == MyDatabaseId &&
					memcmp(&prop->filter_clauses[i].key_set, &colmap, sizeof(colmap)) == 0)
				{
					found = true;
					break;
				}
				if (min < 0 || prop->filter_clauses[i].agg < prop->filter_clauses[min].agg)
				{
					min = i;
				}
			}
			if (!found)
			{
				if (i == max_proposals)
				{
					/* Replace clause with smallest aggregate value */
					Assert(min != -1);
					i = min;
				}
				else
				{
					prop->n_clauses += 1;
				}
				memcpy(&prop->filter_clauses[i].key_set, &colmap, sizeof(colmap));
				prop->filter_clauses[i].relid = relid;
				prop->filter_clauses[i].dbid = MyDatabaseId;
				prop->filter_clauses[i].agg = 0.0;
				prop->filter_clauses[i].n_calls = 0;
				prop->filter_clauses[i].elapsed = 0.0;
			}
			prop->filter_clauses[i].agg = aggregate(prop->filter_clauses[i].agg, value);
			prop->filter_clauses[i].n_calls += 1;
			prop->filter_clauses[i].elapsed += queryDesc->totaltime->total;
		}
	}
}

/**
 * Propose multicolumn statistic for quals with bug misestimation
 */
static void
ProposeMultiColumnStatisticForQual(QueryDesc *queryDesc, PlanState *planstate, List* qual)
{
	double misestimation = planstate->instrument->tuplecount / planstate->plan->plan_rows;
	AddProposal(&state->statistics, queryDesc, qual, misestimation, 2, agg_max);
}

/**
 * Check number of filtered records for the qual
 */
static void
ProposeIndexForQual(QueryDesc *queryDesc, PlanState *planstate, List* qual)
{
	AddProposal(&state->indexes, queryDesc, qual, planstate->instrument->nfiltered1, 1, agg_sum);
}

/**
 * Traverse node
 */
static void
AnalyzeNode(QueryDesc *queryDesc, PlanState *planstate)
{
	Plan	   *plan = planstate->plan;
	double rows = planstate->instrument->tuplecount;
    /*	planstate->instrument->ntuples / planstate->instrument->nloops; ??? */
	if (misestimation_threshold != 0 &&
		plan->plan_rows != 0 &&
		rows / plan->plan_rows >= misestimation_threshold &&
		(min_rows == 0 || rows >= min_rows))
	{
		elog(LOG, "[online-advisor]: Misestimation %f for statement \"%s\", node \"%s\": %f expected, %f actual",
			 rows / plan->plan_rows,
			 queryDesc->sourceText, nodeToString(plan),
			 plan->plan_rows, rows);
		/* quals, sort keys, etc */
		switch (nodeTag(plan))
		{
			case T_IndexScan:
				ProposeMultiColumnStatisticForQual(queryDesc, planstate, ((IndexScan *) plan)->indexqualorig);
				break;
			case T_IndexOnlyScan:
				ProposeMultiColumnStatisticForQual(queryDesc, planstate, ((IndexOnlyScan *) plan)->indexqual);
				break;
			case T_BitmapIndexScan:
				ProposeMultiColumnStatisticForQual(queryDesc, planstate, ((BitmapIndexScan *) plan)->indexqualorig);
				break;
			case T_NestLoop:
				ProposeMultiColumnStatisticForQual(queryDesc, planstate, ((NestLoop *) plan)->join.joinqual);
				break;
			case T_MergeJoin:
				ProposeMultiColumnStatisticForQual(queryDesc, planstate, ((MergeJoin *) plan)->mergeclauses);
				ProposeMultiColumnStatisticForQual(queryDesc, planstate, ((MergeJoin *) plan)->join.joinqual);
				break;
			case T_HashJoin:
				ProposeMultiColumnStatisticForQual(queryDesc, planstate, ((HashJoin *) plan)->hashclauses);
				ProposeMultiColumnStatisticForQual(queryDesc, planstate, ((HashJoin *) plan)->join.joinqual);
				break;
			default:
				break;
		}
		ProposeMultiColumnStatisticForQual(queryDesc, planstate, plan->qual);
	}
	if (filtered_threshold != 0 && plan->qual)
	{
		double n_filtered = planstate->instrument->nfiltered1;
		/* Consider only clauses filtering more than threshold */
		if (n_filtered >= filtered_threshold)
		{
			elog(LOG, "[online-advisor]: Too many filtered rows %f for statement \"%s\", node \"%s\"",
				 n_filtered, queryDesc->sourceText, nodeToString(plan));
			ProposeIndexForQual(queryDesc, planstate, plan->qual);
		}
	}
	/* initPlan-s */
	if (planstate->initPlan)
		AnalyzeSubPlans(queryDesc, planstate->initPlan);

	/* lefttree */
	if (outerPlanState(planstate))
		AnalyzeNode(queryDesc, outerPlanState(planstate));

	/* righttree */
	if (innerPlanState(planstate))
		AnalyzeNode(queryDesc, innerPlanState(planstate));

	/* special child plans */
	switch (nodeTag(plan))
	{
		case T_Append:
			AnalyzeMemberNodes(queryDesc,
							   ((AppendState *) planstate)->appendplans,
							   ((AppendState *) planstate)->as_nplans);
			break;
		case T_MergeAppend:
			AnalyzeMemberNodes(queryDesc,
							   ((MergeAppendState *) planstate)->mergeplans,
							   ((MergeAppendState *) planstate)->ms_nplans);
			break;
		case T_BitmapAnd:
			AnalyzeMemberNodes(queryDesc,
							   ((BitmapAndState *) planstate)->bitmapplans,
							   ((BitmapAndState *) planstate)->nplans);
			break;
		case T_BitmapOr:
			AnalyzeMemberNodes(queryDesc,
							   ((BitmapOrState *) planstate)->bitmapplans,
							   ((BitmapOrState *) planstate)->nplans);
			break;
		case T_SubqueryScan:
			AnalyzeNode(queryDesc,
						((SubqueryScanState *) planstate)->subplan);
			break;
		default:
			break;
	}
}


/*
 * ExecutorStart hook: start up logging if needed
 */
static void
advisor_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (do_instrumentation)
	{
		queryDesc->instrument_options |= INSTRUMENT_TIMER;
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (do_instrumentation)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
		queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_TIMER, false);
		MemoryContextSwitchTo(oldcxt);
	}
}

/*
 * ExecutorEnd hook: analyze result
 */
static void
advisor_ExecutorEnd(QueryDesc *queryDesc)
{
	if (queryDesc->totaltime)
	{
		MemoryContext oldcxt;

		/*
		 * Make sure we operate in the per-query context, so any cruft will be
		 * discarded later during ExecutorEnd.
		 */
		oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);

		/*
		 * Make sure stats accumulation is done.  (Note: it's okay if several
		 * levels of hook all do this.)
		 */
		InstrEndLoop(queryDesc->totaltime);

		/*
		 * Update information about clauses under spinlock
		 */
		if (do_instrumentation)
		{
			SpinLockAcquire(&state->spinlock);
			AnalyzeNode(queryDesc, queryDesc->planstate);
			SpinLockRelease(&state->spinlock);
		}
		MemoryContextSwitchTo(oldcxt);
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}


static int
compare_number_of_keys(void const* a, void const* b)
{
	return fbms_num_members(&((FilterClause*)a)->key_set) - fbms_num_members(&((FilterClause*)b)->key_set);
}

typedef char* (*create_statement_func)(char const* schema, char const* table, char const* columns);

static char*
create_index(char const* schema, char const* table, char const* columns)
{
	return psprintf("CREATE INDEX ON %s.%s(%s)", schema, table, columns);
}

static char*
create_statistics(char const* schema, char const* table, char const* columns)
{
	return psprintf("CREATE STATISTICS ON %s FROM %s.%s", columns, schema, table);
}



typedef bool (*check_if_exists_func)(Oid relid, FixedBitmapset const* keys);

static bool
check_if_index_exists(Oid relid, FixedBitmapset const* keys)
{
	StringInfoData buf;
	int attno  = -1;
	char sep = '{';
	bool found = false;
	int rc;

	initStringInfo(&buf);
	appendStringInfo(&buf, "select relname from pg_class join pg_index on pg_class.oid=pg_index.indexrelid where indrelid=%d and '", relid);

	while ((attno = fbms_next_member(keys, attno)) >= 0)
	{
		appendStringInfoChar(&buf, sep);
		appendStringInfo(&buf, "%d", attno);
		sep = ',';
	}
	appendStringInfoString(&buf, "}'::smallint[] <@ indkey::smallint[]");

	SPI_connect();
	rc = SPI_execute(buf.data, true, 0);
	if (rc != SPI_OK_SELECT)
	{
		elog(LOG, "[online-advisor]: Select failed with status %d", rc);
	}
	else if (SPI_processed)
	{
		char* indname = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
		elog(LOG, "[online-advisor]: Index %s already exists", indname);
		found = true;
	}
	SPI_finish();
	pfree(buf.data);
	return found;
}

static bool
check_if_statistic_exists(Oid relid, FixedBitmapset const* keys)
{
	StringInfoData buf;
	int attno  = -1;
	char sep = '{';
	bool found = false;
	int rc;

	initStringInfo(&buf);
	appendStringInfo(&buf, "select stxname from pg_statistic_ext where stxrelid=%d and '", relid);

	while ((attno = fbms_next_member(keys, attno)) >= 0)
	{
		appendStringInfoChar(&buf, sep);
		appendStringInfo(&buf, "%d", attno);
		sep = ',';
	}
	appendStringInfoString(&buf, "}'::smallint[] <@ stxkeys::smallint[]");

	SPI_connect();
	rc = SPI_execute(buf.data, true, 0);
	if (rc != SPI_OK_SELECT)
	{
		elog(LOG, "[online-advisor]: Select failed with status %d", rc);
	}
	else if (SPI_processed)
	{
		char* stxname = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
		elog(LOG, "[online-advisor]: Statistic %s already exists", stxname);
		found = true;
	}
	SPI_finish();
	pfree(buf.data);
	return found;
}

/*
 * Context fot set-returning function
 */
typedef struct
{
	TupleDesc		tupdesc;
	size_t			curpos;
	size_t			n_clauses;
	bool*			visited;
	FilterClause*	clauses;
} FunctionCallContext;

#define ONLINE_ADVISOR_NATTRS 4

static Datum
get_proposals(PG_FUNCTION_ARGS, Proposal* prop, create_statement_func create_statement, check_if_exists_func check_if_exists, aggregate_func aggregate)
{
	FuncCallContext *funcctx;
	Datum		result;
	MemoryContext oldcontext;
	FunctionCallContext *fctx;	/* User function context. */
	HeapTuple	tuple;
	bool 		combine = PG_ARGISNULL(0) ? true : PG_GETARG_BOOL(0);
	bool 		reset = PG_ARGISNULL(1) ? false : PG_GETARG_BOOL(1);
	size_t 		n_clauses;


	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Create a user function context for cross-call persistence */
		fctx = (FunctionCallContext *) palloc(sizeof(FunctionCallContext));

		/* Construct a tuple descriptor for the result rows. */
		get_call_result_type(fcinfo, NULL, &fctx->tupdesc);

		fctx->curpos = 0;

		if (state)
		{
			/* Copy content from shared memory under spinlock */
			SpinLockAcquire(&state->spinlock);
			n_clauses = prop->n_clauses;
			fctx->clauses = (FilterClause*) palloc0(n_clauses*sizeof(FilterClause));
			memcpy(fctx->clauses, prop->filter_clauses, n_clauses*sizeof(FilterClause));
			if (reset)
			{
				prop->n_clauses = 0;
			}
			SpinLockRelease(&state->spinlock);

			fctx->visited = (bool*)palloc0(n_clauses*sizeof(bool));
			qsort(fctx->clauses, n_clauses, sizeof(FilterClause), compare_number_of_keys);

			fctx->n_clauses = n_clauses;
		}
		else
		{
			fctx->n_clauses = 0;
		}
		funcctx->user_fctx = fctx;

		/* Return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	/* Get the saved state */
	fctx = funcctx->user_fctx;
	n_clauses =  fctx->n_clauses;

	for (size_t i = fctx->curpos; i < n_clauses; i++)
	{
		if (fctx->clauses[i].dbid == MyDatabaseId && !fctx->visited[i])
		{
			Datum	values[ONLINE_ADVISOR_NATTRS];
			bool	nulls[ONLINE_ADVISOR_NATTRS] = { false, false, false, false };
			Oid 	relid = fctx->clauses[i].relid;
			StringInfoData buf;
			char sep = ' ';
			int attno  = -1;
			FixedBitmapset* keys = &fctx->clauses[i].key_set;
			size_t k = i;

			initStringInfo(&buf);

			/* Append all attributes (order doesn't matter) */
			while ((attno = fbms_next_member(keys, attno)) >= 0)
			{
				char* attname = get_attname(relid, attno, false);
				appendStringInfoChar(&buf, sep);
				appendStringInfoString(&buf, attname);
				sep = ',';
			}
			if (combine) /* find all clauses which can be handled by one compound index */
			{
				for (size_t j = i+1; j < n_clauses; j++) {
					if (!fctx->visited[j] &&
						relid == fctx->clauses[j].relid &&
						MyDatabaseId == fctx->clauses[j].dbid &&
						fbms_is_subset(&fctx->clauses[k].key_set, &fctx->clauses[j].key_set))
					{
						/* Append extra nattroibutes for compound index */
						Assert(attno == -1);
						while ((attno = fbms_next_member(&fctx->clauses[j].key_set, attno)) >= 0)
						{
							if (!fbms_is_member(attno, keys))
							{
								char* attname = get_attname(relid, attno, false);
								appendStringInfoChar(&buf, sep);
								appendStringInfoString(&buf, attname);
								sep = ',';
							}
						}
						keys = &fctx->clauses[j].key_set;
						fctx->visited[j] = true;
						fctx->clauses[i].agg = aggregate(fctx->clauses[i].agg, fctx->clauses[j].agg);
						fctx->clauses[i].n_calls += fctx->clauses[j].n_calls;
						fctx->clauses[i].elapsed += fctx->clauses[j].elapsed;
						k = j;
					}
				}
			}
			fctx->curpos = i+1;
			if (check_if_exists(relid, &fctx->clauses[k].key_set))
			{
				pfree(buf.data);
				continue;
			}
			values[0] = Float8GetDatum(fctx->clauses[i].agg);
			values[1] = UInt64GetDatum(fctx->clauses[i].n_calls);
			values[2] = Float8GetDatum(fctx->clauses[i].elapsed);
			values[3] = CStringGetTextDatum(create_statement(get_namespace_name(get_rel_namespace(relid)),
															 get_rel_name(relid),
															 buf.data+1)); /* skip first space */
			pfree(buf.data);

			/* Build and return the tuple. */
			tuple = heap_form_tuple(fctx->tupdesc, values, nulls);
			result = HeapTupleGetDatum(tuple);
			SRF_RETURN_NEXT(funcctx, result);
		}
	}
	SRF_RETURN_DONE(funcctx);
}

PG_FUNCTION_INFO_V1(propose_indexes);
PG_FUNCTION_INFO_V1(propose_statistics);

Datum
propose_indexes(PG_FUNCTION_ARGS)
{
	return get_proposals(fcinfo, &state->indexes, create_index, check_if_index_exists, agg_sum);
}

Datum
propose_statistics(PG_FUNCTION_ARGS)
{
	return get_proposals(fcinfo, &state->statistics, create_statistics, check_if_statistic_exists, agg_max);
}


