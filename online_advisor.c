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
static int  max_indexes = 128;

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
	double			n_filtered; /* total number of filtered rows using subset of this key set */
	double			elapsed;    /* total time in seconds spent in filtering this condfition */
	Oid				dbid;       /* database identifier */
	Oid				relid;      /* relation ID */
	FixedBitmapset	key_set;    /* Set of variable used in filter clause */
} FilterClause;

typedef struct
{
	slock_t			spinlock; /* Spinlock to synchronize access */
	size_t			n_clauses;
	FilterClause	filter_clauses[FLEXIBLE_ARRAY_MEMBER];
} AdvisorState;

AdvisorState* state;

static void advisor_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void advisor_ExecutorEnd(QueryDesc *queryDesc);

static size_t
advisor_shmem_size(void)
{
	return offsetof(AdvisorState, filter_clauses) + (size_t)max_indexes*sizeof(FilterClause);
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
		state->n_clauses = 0;
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
							"Sets the minimum number of filtered records which triggers index suggestion.",
							NULL,
							&filtered_threshold,
							1000,
							1, INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("online_advisor.max_indexes",
							"Maximum number of indexes advisor can suggest.",
							NULL,
							&max_indexes,
							128,
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
 * Try to add multicolumn statistics for specified subplans.
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
 * Try to add multicolumn statistics for plan subnodes.
 */
static void
AnalyzeMemberNodes(QueryDesc *queryDesc, PlanState **planstates, int nsubnodes)
{
	for (int j = 0; j < nsubnodes; j++)
	{
		AnalyzeNode(queryDesc, planstates[j]);
	}
}

/**
 * Check number of filtered records for the qual
 */
static void
AnalyzeQual(QueryDesc *queryDesc, PlanState *planstate, List* qual)
{
	List *vars = NULL;
	ListCell* lc;
	List* rtable = queryDesc->plannedstmt->rtable;

	/* Cpnsider only clauses filtering more than thrershold */
	double n_filtered = planstate->instrument->nfiltered1;
	if (n_filtered < filtered_threshold)
	{
		return;
	}

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
		bool has_vars = false;
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
							has_vars = true;
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
		if (has_vars)
		{
			RangeTblEntry *rte = rt_fetch(relno, rtable);
			Oid relid = rte->relid;
			bool found = false;
			int i;
			int min = -1;
			for (i = 0; i < state->n_clauses; i++)
			{
				if (state->filter_clauses[i].relid == relid &&
					state->filter_clauses[i].dbid == MyDatabaseId &&
					memcmp(&state->filter_clauses[i].key_set, &colmap, sizeof(colmap)) == 0)
				{
					found = true;
					break;
				}
				if (min < 0 || state->filter_clauses[i].n_filtered < state->filter_clauses[min].n_filtered)
				{
					min = i;
				}
			}
			if (!found)
			{
				if (i == max_indexes)
				{
					/* Replace clause with smallest number of fitlered records */
					Assert(min != -1);
					i = min;
				}
				else
				{
					state->n_clauses += 1;
				}
				memcpy(&state->filter_clauses[i].key_set, &colmap, sizeof(colmap));
				state->filter_clauses[i].relid = relid;
				state->filter_clauses[i].dbid = MyDatabaseId;
				state->filter_clauses[i].n_filtered = 0;
				state->filter_clauses[i].n_calls = 0;
				state->filter_clauses[i].elapsed = 0;
			}
			state->filter_clauses[i].n_filtered += n_filtered;
			state->filter_clauses[i].n_calls += 1;
			state->filter_clauses[i].elapsed += queryDesc->totaltime->total;
		}
	}
}

/**
 * Traverse node
 */
static void
AnalyzeNode(QueryDesc *queryDesc, PlanState *planstate)
{
	Plan	   *plan = planstate->plan;

	if (do_instrumentation && plan->qual)
	{
		Assert(planstate->instrument);
		AnalyzeQual(queryDesc, planstate, plan->qual);
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
		 * Update information about index clauses under spinlock */
		SpinLockAcquire(&state->spinlock);
		AnalyzeNode(queryDesc, queryDesc->planstate);
		SpinLockRelease(&state->spinlock);

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

PG_FUNCTION_INFO_V1(propose_indexes);

Datum
propose_indexes(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	Datum		result;
	MemoryContext oldcontext;
	FunctionCallContext *fctx;	/* User function context. */
	HeapTuple	tuple;
	bool 		combine = PG_ARGISNULL(0) ? true : PG_GETARG_BOOL(0);
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
			n_clauses = state->n_clauses;
			fctx->clauses = (FilterClause*) palloc0(n_clauses*sizeof(FilterClause));
			memcpy(fctx->clauses, state->filter_clauses, n_clauses*sizeof(FilterClause));
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
			char sep = '(';
			int attno  = -1;
			FixedBitmapset* keys = &fctx->clauses[i].key_set;

			initStringInfo(&buf);
			appendStringInfo(&buf, "CREATE INDEX ON %s.%s",
							 get_namespace_name(get_rel_namespace(relid)),
							 get_rel_name(relid));

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
				size_t k = i;
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
						fctx->clauses[i].n_filtered += fctx->clauses[j].n_filtered;
						fctx->clauses[i].n_calls += fctx->clauses[j].n_calls;
						fctx->clauses[i].elapsed += fctx->clauses[j].elapsed;
						k = j;
					}
				}
			}
			appendStringInfoChar(&buf, ')');
			values[0] = Float8GetDatum(state->filter_clauses[i].n_filtered);
			values[1] = UInt64GetDatum(state->filter_clauses[i].n_calls);
			values[2] = Float8GetDatum(state->filter_clauses[i].elapsed);
			values[3] = CStringGetTextDatum(buf.data);
			pfree(buf.data);

			/* Build and return the tuple. */
			tuple = heap_form_tuple(fctx->tupdesc, values, nulls);
			result = HeapTupleGetDatum(tuple);
			fctx->curpos = i+1;
			SRF_RETURN_NEXT(funcctx, result);
		}
	}
	SRF_RETURN_DONE(funcctx);
}
