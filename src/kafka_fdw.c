/*-------------------------------------------------------------------------
 *
 *                 kafka Foreign Data Wrapper for PostgreSQL
 *
 * Copyright (c) 2016 Jony Vesterman Cohen
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Jony Vesterman Cohen <jony.cohenjo@gmail.com>
 *
 * IDENTIFICATION
 *		  kafka_fdw/src/kafka_fdw.c
 *
 *-------------------------------------------------------------------------
 */

 /* Debug mode */
 #define DEBUG

#include "postgres.h"

#include "funcapi.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

// includes for librdkafka
#include <librdkafka/rdkafka.h>
#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>

PG_MODULE_MAGIC;

/*
 * SQL functions
 */
extern Datum kafka_fdw_handler(PG_FUNCTION_ARGS);
extern Datum kafka_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(kafka_fdw_handler);
PG_FUNCTION_INFO_V1(kafka_fdw_validator);


/* callback functions */
#if (PG_VERSION_NUM >= 90200)
static void kafkaGetForeignRelSize(PlannerInfo *root,
						   RelOptInfo *baserel,
						   Oid foreigntableid);

static void kafkaGetForeignPaths(PlannerInfo *root,
						 RelOptInfo *baserel,
						 Oid foreigntableid);

static ForeignScan *kafkaGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
						List *scan_clauses);
#else
static FdwPlan *kafkaPlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel);
#endif

static void kafkaBeginForeignScan(ForeignScanState *node,
						  int eflags);

static TupleTableSlot *kafkaIterateForeignScan(ForeignScanState *node);

static void kafkaReScanForeignScan(ForeignScanState *node);

static void kafkaEndForeignScan(ForeignScanState *node);

#if (PG_VERSION_NUM >= 90300)
static void kafkaAddForeignUpdateTargets(Query *parsetree,
								 RangeTblEntry *target_rte,
								 Relation target_relation);

static List *kafkaPlanForeignModify(PlannerInfo *root,
						   ModifyTable *plan,
						   Index resultRelation,
						   int subplan_index);

static void kafkaBeginForeignModify(ModifyTableState *mtstate,
							ResultRelInfo *rinfo,
							List *fdw_private,
							int subplan_index,
							int eflags);

static TupleTableSlot *kafkaExecForeignInsert(EState *estate,
						   ResultRelInfo *rinfo,
						   TupleTableSlot *slot,
						   TupleTableSlot *planSlot);

static TupleTableSlot *kafkaExecForeignUpdate(EState *estate,
						   ResultRelInfo *rinfo,
						   TupleTableSlot *slot,
						   TupleTableSlot *planSlot);

static TupleTableSlot *kafkaExecForeignDelete(EState *estate,
						   ResultRelInfo *rinfo,
						   TupleTableSlot *slot,
						   TupleTableSlot *planSlot);

static void kafkaEndForeignModify(EState *estate,
						  ResultRelInfo *rinfo);

static int	kafkaIsForeignRelUpdatable(Relation rel);

#endif

static void kafkaExplainForeignScan(ForeignScanState *node,
							struct ExplainState * es);

#if (PG_VERSION_NUM >= 90300)
static void kafkaExplainForeignModify(ModifyTableState *mtstate,
							  ResultRelInfo *rinfo,
							  List *fdw_private,
							  int subplan_index,
							  struct ExplainState * es);
#endif

#if (PG_VERSION_NUM >= 90200)
static bool kafkaAnalyzeForeignTable(Relation relation,
							 AcquireSampleRowsFunc *func,
							 BlockNumber *totalpages);
#endif

#if (PG_VERSION_NUM >= 90500)

static void kafkaGetForeignJoinPaths(PlannerInfo *root,
							 RelOptInfo *joinrel,
							 RelOptInfo *outerrel,
							 RelOptInfo *innerrel,
							 JoinType jointype,
							 JoinPathExtraData *extra);


static RowMarkType kafkaGetForeignRowMarkType(RangeTblEntry *rte,
							   LockClauseStrength strength);

static HeapTuple kafkaRefetchForeignRow(EState *estate,
						   ExecRowMark *erm,
						   Datum rowid,
						   bool *updated);

static List *kafkaImportForeignSchema(ImportForeignSchemaStmt *stmt,
							 Oid serverOid);

#endif


/*
 * structures used by the FDW
 *
 * These next two are not actually used by kafka, but something like this
 * will be needed by anything more complicated that does actual work.
 *
 */
 static rd_kafka_t *rk;
 static rd_kafka_t *rkConsumer = NULL;
 static rd_kafka_t *rkProducer = NULL;
 // static rd_kafka_t *global_rk;
/*
 * Describes the valid options for objects that use this wrapper.
 */
struct KafkaFdwOption
{
	const char *optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};

static struct KafkaFdwOption valid_options[] =
{

	/* Connection options */
	{"address", ForeignServerRelationId},
	{"port", ForeignServerRelationId},

	/* table options */
	{"offset", ForeignTableRelationId},
  {"topic", ForeignTableRelationId},
	{"tablekeyprefix", ForeignTableRelationId},
	{"tablekeyset", ForeignTableRelationId},
	{"tabletype", ForeignTableRelationId},
  {"timeout", ForeignTableRelationId},
  {"partition", ForeignTableRelationId}
};

typedef enum
{
	PG_kafka_SCALAR_TABLE = 0,
	PG_kafka_HASH_TABLE,
	PG_kafka_LIST_TABLE,
	PG_kafka_SET_TABLE,
	PG_kafka_ZSET_TABLE
} kafka_table_type;

typedef struct kafkaTableOptions
{
	char	   *address;
	int			port;
  int			timeout;
	char	   *topic;
	int			partition;
	char	   *keyprefix;
	char	   *keyset;
	int    offset;
	kafka_table_type table_type;
} kafkaTableOptions;


/*
 * This is what will be set and stashed away in fdw_private and fetched
 * for subsequent routines.
 */
typedef struct
{
  rd_kafka_topic_t *context;
	rd_kafka_message_t *reply;
	char	   *address;
	int			port;
	int			partition;
} kafkaFdwPlanState;


/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */

typedef struct kafkaFdwExecutionState
{
	AttInMetadata *attinmeta;
	rd_kafka_topic_t *context;
	rd_kafka_message_t *reply;
	long long	row;
	char	   *address;
	int			port;
	char	   *topic;
	int			partition;
	char	   *keyprefix;
	char	   *keyset;
	char	   *qual_value;
	int	  offset;
  int     timeout;
	kafka_table_type table_type;
	char	   *cursor_search_string;
	char	   *cursor_id;
  rd_kafka_message_t **messages;
  int     curr_message;
  int     max_message;
	MemoryContext mctxt;
} kafkaFdwExecutionState;

typedef struct KafkaFdwModifyState
{
	rd_kafka_topic_t *context;
	char	   *address;
	int			port;
	char	   *password;
	int			database;
	char	   *keyprefix;
	char	   *keyset;
	char	   *qual_value;
	char	   *singleton_key;
	Relation	rel;
	kafka_table_type table_type;
	List	   *target_attrs;
	int		   *targetDims;
	int			p_nums;
	int			keyAttno;
	Oid			array_elem_type;
	FmgrInfo   *p_flinfo;
} KafkaFdwModifyState;

/* initial cursor */
#define ZERO "0"
/* kafka default is 10 - let's fetch 1000 at a time */
#define COUNT " COUNT 1000"

/*
 * Helper functions
 */
static bool kafkaIsValidOption(const char *option, Oid context);
static void kafkaGetOptions(Oid foreigntableid, kafkaTableOptions *options);
// static void kafkaGetQual(Node *node, TupleDesc tupdesc, char **key,
// 						 char **value, bool *pushdown);
// static char *process_kafka_array(kafkaReply *reply, kafka_table_type type);
// static void check_reply(rd_kafka_message_t *reply, kafkaContext *context,
// 						int error_code, char *message, char *arg);
// static void metadata_print (const char *topic, const struct rd_kafka_metadata *metadata);
static char *flatten_tup(TupleTableSlot *slot, int *msglen);


Datum
kafka_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	elog(DEBUG1, "entering function %s", __func__);

	/*
	 * assign the handlers for the FDW
	 *
	 * This function might be called a number of times. In particular, it is
	 * likely to be called for each INSERT statement. For an explanation, see
	 * core postgres file src/optimizer/plan/createplan.c where it calls
	 * GetFdwRoutineByRelId(().
	 */

	/* Required by notations: S=SELECT I=INSERT U=UPDATE D=DELETE */

	/* these are required */
#if (PG_VERSION_NUM >= 90200)
	fdwroutine->GetForeignRelSize = kafkaGetForeignRelSize; /* S U D */
	fdwroutine->GetForeignPaths = kafkaGetForeignPaths;		/* S U D */
	fdwroutine->GetForeignPlan = kafkaGetForeignPlan;		/* S U D */
#endif
	fdwroutine->BeginForeignScan = kafkaBeginForeignScan;	/* S U D */
	fdwroutine->IterateForeignScan = kafkaIterateForeignScan;		/* S */
	fdwroutine->ReScanForeignScan = kafkaReScanForeignScan; /* S */
	fdwroutine->EndForeignScan = kafkaEndForeignScan;		/* S U D */

	/* remainder are optional - use NULL if not required */
	/* support for insert / update / delete */
#if (PG_VERSION_NUM >= 90300)
	fdwroutine->IsForeignRelUpdatable = kafkaIsForeignRelUpdatable;
	fdwroutine->AddForeignUpdateTargets = kafkaAddForeignUpdateTargets;		/* U D */
	fdwroutine->PlanForeignModify = kafkaPlanForeignModify; /* I U D */
	fdwroutine->BeginForeignModify = kafkaBeginForeignModify;		/* I U D */
	fdwroutine->ExecForeignInsert = kafkaExecForeignInsert; /* I */
	fdwroutine->ExecForeignUpdate = kafkaExecForeignUpdate; /* U */
	fdwroutine->ExecForeignDelete = kafkaExecForeignDelete; /* D */
	fdwroutine->EndForeignModify = kafkaEndForeignModify;	/* I U D */
#endif

	/* support for EXPLAIN */
	fdwroutine->ExplainForeignScan = kafkaExplainForeignScan;		/* EXPLAIN S U D */
#if (PG_VERSION_NUM >= 90300)
	fdwroutine->ExplainForeignModify = kafkaExplainForeignModify;	/* EXPLAIN I U D */
#endif

#if (PG_VERSION_NUM >= 90200)
	/* support for ANALYSE */
	fdwroutine->AnalyzeForeignTable = kafkaAnalyzeForeignTable;		/* ANALYZE only */
#endif


#if (PG_VERSION_NUM >= 90500)
	/* Support functions for IMPORT FOREIGN SCHEMA */
	fdwroutine->ImportForeignSchema = kafkaImportForeignSchema;

	/* Support for scanning foreign joins */
	fdwroutine->GetForeignJoinPaths = kafkaGetForeignJoinPaths;

	/* Support for locking foreign rows */
	fdwroutine->GetForeignRowMarkType = kafkaGetForeignRowMarkType;
	fdwroutine->RefetchForeignRow = kafkaRefetchForeignRow;

#endif


	PG_RETURN_POINTER(fdwroutine);
}

Datum
kafka_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	char	   *svr_address = NULL;
	int			svr_port = 0;
	char	   *svr_password = NULL;
	int			svr_database = 0;
  int			timeout = 100;
  int			partition = 0;
	kafka_table_type tabletype = PG_kafka_SCALAR_TABLE;
	char	   *tablekeyprefix = NULL;
	char	   *tablekeyset = NULL;
	char	   *singletonkey = NULL;
	ListCell   *cell;

#ifdef DEBUG
	elog(NOTICE, "kafka_fdw_validator");
#endif

	/*
	 * Check that only options supported by kafka_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!kafkaIsValidOption(def->defname, catalog))
		{
			struct KafkaFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->optname);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 errhint("Valid options in this context are: %s",
							 buf.len ? buf.data : "<none>")
					 ));
		}

		if (strcmp(def->defname, "address") == 0)
		{
			if (svr_address)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options: "
									   "address (%s)", defGetString(def))
								));

			svr_address = strVal(def->arg);
      //elog(DEBUG1,"%s: Got server: %s",__func__,svr_address);
		}
		else if (strcmp(def->defname, "port") == 0)
		{
			if (svr_port)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: port (%s)",
                defGetInt32(def))
								//defGetString(def))
						 ));

      //NodeSetTag(def->arg,T_Integer); // what am I missing?!? why do I need this??
			svr_port = strtol(strVal(def->arg),NULL,10); // should I use defGetInt32	??
      //svr_port = defGetInt32(def);
      //elog(DEBUG1,"%s: Got port number: %d",__func__,svr_port);
		}
		if (strcmp(def->defname, "password") == 0)
		{
			if (svr_password)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: password")
								));

			svr_password = defGetString(def);
		}
		else if (strcmp(def->defname, "database") == 0)
		{
			if (svr_database)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: database "
								"(%s)", defGetString(def))
						 ));

			svr_database = atoi(defGetString(def));
		}
    else if (strcmp(def->defname, "timeout") == 0)
		{
      timeout = strtol(strVal(def->arg),NULL,10);
		}
    else if (strcmp(def->defname, "partition") == 0)
		{
      partition = strtol(strVal(def->arg),NULL,10);
		}
		else if (strcmp(def->defname, "singleton_key ") == 0)
		{
			if (tablekeyset)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting options: tablekeyset(%s) and "
								"singleton_key (%s)", tablekeyset,
								defGetString(def))
						 ));
			if (tablekeyprefix)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting options: tablekeyprefix(%s) and "
								"singleton_key (%s)", tablekeyprefix,
								defGetString(def))
						 ));
			if (singletonkey)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: "
								"singleton_key (%s)", defGetString(def))
						 ));

			singletonkey = defGetString(def);
		}
		else if (strcmp(def->defname, "tablekeyprefix") == 0)
		{
			if (tablekeyset)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting options: tablekeyset(%s) and "
								"tablekeyprefix (%s)", tablekeyset,
								defGetString(def))
						 ));
			if (singletonkey)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting options: singleton_key(%s) and "
								"tablekeyprefix (%s)", singletonkey,
								defGetString(def))
						 ));
			if (tablekeyprefix)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: "
								"tablekeyprefix (%s)", defGetString(def))
						 ));

			tablekeyprefix = defGetString(def);
		}
		else if (strcmp(def->defname, "tablekeyset") == 0)
		{
			if (tablekeyprefix)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
					   errmsg("conflicting options: tablekeyprefix (%s) and "
							  "tablekeyset (%s)", tablekeyprefix,
							  defGetString(def))
						 ));
			if (singletonkey)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting options: singleton_key(%s) and "
								"tablekeyset (%s)", singletonkey,
								defGetString(def))
						 ));
			if (tablekeyset)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: "
								"tablekeyset (%s)", defGetString(def))
						 ));

			tablekeyset = defGetString(def);
		}
		else if (strcmp(def->defname, "tabletype") == 0)
		{
			char	   *typeval = defGetString(def);

			if (tabletype)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: tabletype "
								"(%s)", typeval)));
			if (strcmp(typeval, "hash") == 0)
				tabletype = PG_kafka_HASH_TABLE;
			else if (strcmp(typeval, "list") == 0)
				tabletype = PG_kafka_LIST_TABLE;
			else if (strcmp(typeval, "set") == 0)
				tabletype = PG_kafka_SET_TABLE;
			else if (strcmp(typeval, "zset") == 0)
				tabletype = PG_kafka_ZSET_TABLE;
			else
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid tabletype (%s) - must be hash, "
								"list, set or zset", typeval)));
		}
	}

	PG_RETURN_VOID();
}

#if (PG_VERSION_NUM >= 90200)
static void
kafkaGetForeignRelSize(PlannerInfo *root,
						   RelOptInfo *baserel,
						   Oid foreigntableid)
{
	/*
	 * Obtain relation size estimates for a foreign table. This is called at
	 * the beginning of planning for a query that scans a foreign table. root
	 * is the planner's global information about the query; baserel is the
	 * planner's information about this table; and foreigntableid is the
	 * pg_class OID of the foreign table. (foreigntableid could be obtained
	 * from the planner data structures, but it's passed explicitly to save
	 * effort.)
	 *
	 * This function should update baserel->rows to be the expected number of
	 * rows returned by the table scan, after accounting for the filtering
	 * done by the restriction quals. The initial value of baserel->rows is
	 * just a constant default estimate, which should be replaced if at all
	 * possible. The function may also choose to update baserel->width if it
	 * can compute a better estimate of the average result row width.
	 */

	kafkaFdwPlanState *fdw_private;

	elog(DEBUG1, "entering function %s", __func__);

	baserel->rows = 0;

	fdw_private = palloc0(sizeof(kafkaFdwPlanState));
	baserel->fdw_private = (void *) fdw_private;

	/* initialize required state in fdw_private */
  // int64_t lo, hi;
  //                       rd_kafka_resp_err_t err;
  //
	// 		/* Only query for hi&lo partition watermarks */
  //
	// 		if ((err = rd_kafka_query_watermark_offsets(
	// 			     rk, topic, partition, &lo, &hi, 5000))) {
	// 			fprintf(stderr, "%% query_watermark_offsets() "
	// 				"failed: %s\n",
	// 				rd_kafka_err2str(err));
	// 			exit(1);
	// 		}
  //
	// 		printf("%s [%d]: low - high offsets: "
	// 		       "%"PRId64" - %"PRId64"\n",
	// 		       topic, partition, lo, hi);
  //
	// 		rd_kafka_destroy(rk);
	// 		exit(0);

}

static void
kafkaGetForeignPaths(PlannerInfo *root,
						 RelOptInfo *baserel,
						 Oid foreigntableid)
{
	/*
	 * Create possible access paths for a scan on a foreign table. This is
	 * called during query planning. The parameters are the same as for
	 * GetForeignRelSize, which has already been called.
	 *
	 * This function must generate at least one access path (ForeignPath node)
	 * for a scan on the foreign table and must call add_path to add each such
	 * path to baserel->pathlist. It's recommended to use
	 * create_foreignscan_path to build the ForeignPath nodes. The function
	 * can generate multiple access paths, e.g., a path which has valid
	 * pathkeys to represent a pre-sorted result. Each access path must
	 * contain cost estimates, and can contain any FDW-private information
	 * that is needed to identify the specific scan method intended.
	 */

	/*
	 * kafkaFdwPlanState *fdw_private = baserel->fdw_private;
	 */

	Cost		startup_cost,
				total_cost;

	elog(DEBUG1, "entering function %s", __func__);

	startup_cost = 0;
	total_cost = startup_cost + baserel->rows;

	/* Create a ForeignPath node and add it as only possible path */
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,		/* no pathkeys */
                   NULL, /* no required_outer */
									 NULL,		/* no outer rel either */
									 NIL));		/* no fdw_private data */
}



static ForeignScan *
kafkaGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
						List *scan_clauses)
{
	/*
	 * Create a ForeignScan plan node from the selected foreign access path.
	 * This is called at the end of query planning. The parameters are as for
	 * GetForeignRelSize, plus the selected ForeignPath (previously produced
	 * by GetForeignPaths), the target list to be emitted by the plan node,
	 * and the restriction clauses to be enforced by the plan node.
	 *
	 * This function must create and return a ForeignScan plan node; it's
	 * recommended to use make_foreignscan to build the ForeignScan node.
	 *
	 */

	Index		scan_relid = baserel->relid;

	/*
	 * We have no native ability to evaluate restriction clauses, so we just
	 * put all the scan_clauses into the plan node's qual list for the
	 * executor to check. So all we have to do here is strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 */

	elog(DEBUG1, "entering function %s", __func__);

	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Create the ForeignScan node */
#if(PG_VERSION_NUM < 90500)
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							NIL);		/* no private state either */
#else
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							NIL,	/* no private state either */
							NIL, /* no tlist info */
              NIL, /* no recheck quals */
              NULL /* no outer plan */);
#endif

}

#endif


static void
kafkaBeginForeignScan(ForeignScanState *node,
						  int eflags)
{
	/*
	 * Begin executing a foreign scan. This is called during executor startup.
	 * It should perform any initialization needed before the scan can start,
	 * but not start executing the actual scan (that should be done upon the
	 * first call to IterateForeignScan). The ForeignScanState node has
	 * already been created, but its fdw_state field is still NULL.
	 * Information about the table to scan is accessible through the
	 * ForeignScanState node (in particular, from the underlying ForeignScan
	 * plan node, which contains any FDW-private information provided by
	 * GetForeignPlan). eflags contains flag bits describing the executor's
	 * operating mode for this plan node.
	 *
	 * Note that when (eflags & EXEC_FLAG_EXPLAIN_ONLY) is true, this function
	 * should not perform any externally-visible actions; it should only do
	 * the minimum required to make the node state valid for
	 * ExplainForeignScan and EndForeignScan.
	 *
	 */
   rd_kafka_topic_t *rkt;
  StringInfoData broker_str;
 	char *brokers = "localhost:9092";
 	char *topic = NULL;
 	int partition = 0;// RD_KAFKA_PARTITION_UA;
 	int opt;
  int res;
 	rd_kafka_conf_t *conf;
 	rd_kafka_topic_conf_t *topic_conf;
 	char errstr[512];
 	const char *debug = NULL;
 	int64_t start_offset = 0;
  int report_offsets = 0;
 	int do_conf_dump = 0;
 	char tmp[16];
  int batch_size = 1000;
  kafkaTableOptions table_options;
  kafkaFdwExecutionState *festate;
  rd_kafka_message_t **rkmessages = NULL;

	//elog(DEBUG1, "entering function %s", __func__);
  start_offset = RD_KAFKA_OFFSET_BEGINNING;
  //start_offset = RD_KAFKA_OFFSET_STORED
  /* Topic configuration */
  topic_conf = rd_kafka_topic_conf_new();

  /* Fetch options  */
  //elog(DEBUG1, "%s: getting options", __func__);
	kafkaGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
					&table_options);
  topic = table_options.topic;
  start_offset = table_options.offset;

  //elog(DEBUG1, "%s: broker string, %s:%d", __func__,table_options.address,table_options.port);
  initStringInfo(&broker_str);
  appendStringInfo(&broker_str,"%s:%d",table_options.address,table_options.port);
  brokers = broker_str.data;


	/* Create Kafka handle */
  if(rkConsumer == NULL)
  {
    /* Kafka configuration */
    conf = rd_kafka_conf_new();
    elog(DEBUG1, "%s: setting up configuration", __func__);
    if (rd_kafka_conf_set(conf, "group.id","kafka_fdw",NULL,0) != RD_KAFKA_CONF_OK)
      ereport(ERROR,
        (errno,
         errmsg("%s: Failed to configure group id: %s\n",__func__,errstr),
             errdetail("kafka_fdw:800.")));
    ; // bring back to 1M
    if (rd_kafka_conf_set(conf, "queued.min.messages", "1000000", NULL, 0) != RD_KAFKA_CONF_OK)
      ereport(ERROR,
        (errno,
         errmsg("%s: Failed to configure queued.min.messages: %s\n",__func__,errstr),
             errdetail("kafka_fdw:805.")));
    elog(DEBUG1,"%s: Creating new consumer");
    if (!(rkConsumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf,
					errstr, sizeof(errstr)))) {
            ereport(ERROR,
                (errno,
                 errmsg("%% Failed to create new consumer: %s\n",errstr),
                     errdetail("fdw:764.")));
		}

    elog(DEBUG1, "%s: setting up brokers: %s", __func__, brokers);
    /* Add brokers */
    if (rd_kafka_brokers_add(rkConsumer, brokers) == 0) {
      ereport(ERROR,
          (errno,
           errmsg("%% No valid brokers specified\n"),
               errdetail("fdw:772.")));
    }
  }
  else
  {
    elog(DEBUG1,"%s: Using existing consumer",__func__);
  }

    //elog(DEBUG1, "%s: creating the topic: %s", __func__, topic);
		/* Create topic */
		rkt = rd_kafka_topic_new(rkConsumer, topic, topic_conf);

    // if(rkt==NULL){
    //   elog(DEBUG1, "%s: rkt is null", __func__);
    // }else{
    //   elog(DEBUG1, "%s: rkt topic is: %s", __func__, rd_kafka_topic_name (rkt));
    // }

    if (rd_kafka_consume_start(rkt, partition, start_offset) == -1){

      ereport(ERROR,
       (errno,
        errmsg("%% Failed to start consuming: %s\n",
         rd_kafka_err2str(rd_kafka_errno2err(errno))),
            errdetail("fdw:834.")));
    }

    rkmessages = (rd_kafka_message_t **)palloc(sizeof(*rkmessages) * batch_size);


    /* Stash away the state info we have already */
  	festate = (kafkaFdwExecutionState *) palloc(sizeof(kafkaFdwExecutionState));
  	node->fdw_state = (void *) festate;

    festate->context = rkt;
	festate->reply = NULL;
	festate->row = 0;
	festate->address = table_options.address;
	festate->port = table_options.port;
	festate->keyprefix = table_options.keyprefix;
	festate->keyset = table_options.keyset;
	festate->offset = table_options.offset;
  festate->timeout = table_options.timeout;
  festate->partition = table_options.partition;
	festate->table_type = table_options.table_type;
	festate->cursor_id = NULL;
	festate->cursor_search_string = NULL;
  festate->messages = rkmessages;
  festate->curr_message = 0;
  festate->max_message = -1;

	//festate->qual_value = pushdown ? qual_value : NULL;

	/* OK, we connected. If this is an EXPLAIN, bail out now */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

  festate->attinmeta =
  		TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att);


}


static TupleTableSlot *
kafkaIterateForeignScan(ForeignScanState *node)
{
	/*
	 * Fetch one row from the foreign source, returning it in a tuple table
	 * slot (the node's ScanTupleSlot should be used for this purpose). Return
	 * NULL if no more rows are available. The tuple table slot infrastructure
	 * allows either a physical or virtual tuple to be returned; in most cases
	 * the latter choice is preferable from a performance standpoint. Note
	 * that this is called in a short-lived memory context that will be reset
	 * between invocations. Create a memory context in BeginForeignScan if you
	 * need longer-lived storage, or use the es_query_cxt of the node's
	 * EState.
	 *
	 * The rows returned must match the column signature of the foreign table
	 * being scanned. If you choose to optimize away fetching columns that are
	 * not needed, you should insert nulls in those column positions.
	 *
	 * Note that PostgreSQL's executor doesn't care whether the rows returned
	 * violate any NOT NULL constraints that were defined on the foreign table
	 * columns — but the planner does care, and may optimize queries
	 * incorrectly if NULL values are present in a column declared not to
	 * contain them. If a NULL value is encountered when the user has declared
	 * that none should be present, it may be appropriate to raise an error
	 * (just as you would need to do in the case of a data type mismatch).
	 */
  rd_kafka_topic_t *rkt;
 	rd_kafka_conf_t *conf;
 	rd_kafka_topic_conf_t *topic_conf;
  AttInMetadata *attinmeta;
 	char errstr[512];
 	const char *debug = NULL;
 	int64_t start_offset = 0;
         int report_offsets = 0;
 	int do_conf_dump = 0;
  int partition;
  int timeout;
 	char tmp[16];
  int run = 1;
  HeapTuple	tuple;
  char	  **values;
  bool		found = false;


kafkaFdwExecutionState *festate = (kafkaFdwExecutionState *) node->fdw_state;
rkt = festate->context;
attinmeta = festate->attinmeta;
partition = festate->partition;
timeout = festate->timeout;
elog(DEBUG1, "%s: table options: partition: %d,timeout: %d ", __func__,partition,timeout);
	/*
	 * kafkaFdwExecutionState *festate = (kafkaFdwExecutionState *)
	 * node->fdw_state;
	 */
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	// elog(DEBUG1, "entering function %s", __func__);

  elog(DEBUG1, "%s: clearing slot ", __func__);
  ExecClearTuple(slot);

  // if(rkt==NULL){
  //   elog(DEBUG1, "%s: rkt is null", __func__);
  // }else{
    elog(DEBUG1, "%s: rkt topic is: %s", __func__, rd_kafka_topic_name (rkt));
  // }


	/* Start consuming */


while (run ) {
  int k;
  int r;
  int i = 0;
  int batch_size = 1000;
  rd_kafka_message_t **rkmessages = NULL;
  rkmessages = festate->messages;
  partition = 0;
  char *cols = NULL;
  char *col = NULL;
  char *tofree;

  // check if we need to fetch more messages:
  elog(DEBUG1, "%s: check if we need to fetch more messages: curr: %d, max: %d ", __func__,festate->curr_message , festate->max_message);
  if (festate->max_message == -1 || festate->curr_message > festate->max_message){
    elog(DEBUG1, "%s: fetching more messages.", __func__);
    r = rd_kafka_consume_batch(rkt, partition, timeout, rkmessages, batch_size);
    if (r < 1){
      elog(DEBUG1, "%s: damn r < 1. r=%d", __func__,r);
      // if(r==0){
      //     elog(DEBUG1, "%s: didn't get anything, continue.", __func__);
      //     continue;
      //   }


      elog(DEBUG1, "%s: empty partition ", __func__);
      found = false;
      run =0;
      elog(DEBUG1, "%s: return NULL ", __func__);
      return ExecClearTuple(slot);
    }
    elog(DEBUG1, "%s: rkt topic is: %d messages fetched.", __func__, r);
    festate->curr_message = 0;
    festate->max_message = r-1;
  }
  elog(DEBUG1, "%s: no more messages: ", __func__);
  k = festate->curr_message;
  elog(DEBUG1, "%s: consuming message at offset: %d", __func__,k);


  /* Check for Errors*/
  if (rkmessages[k]->err) {
    elog(DEBUG1, "%s: error detected message at offset: %d", __func__,k);
		// if (rkmessages[k]->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
    //                     cnt.offset = rkmessages[k]->offset;
    //
    //                     if (verbosity >= 1)
    //                             printf("%% Consumer reached end of "
    //                                    "%s [%"PRId32"] "
    //                                    "message queue at offset %"PRId64"\n",
    //                                    rd_kafka_topic_name(rkmessage->rkt),
    //                                    rkmessage->partition, rkmessage->offset);
    //
		// 	if (exit_eof)
		// 		run = 0;
    //
		// 	return ExecClearTuple(slot);
		// }
    //
		// printf("%% Consume error for topic \"%s\" [%"PRId32"] "
		//        "offset %"PRId64": %s\n",
		//        rd_kafka_topic_name(rkmessage->rkt),
		//        rkmessage->partition,
		//        rkmessage->offset,
		//        rd_kafka_message_errstr(rkmessage));
    //
    //             if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
    //                 rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
    //                     run = 0;
    //
    //             cnt.msgs_dr_err++;
		return ExecClearTuple(slot);
	}
  /* If we got so far - we can consume */


      // msg_consume(rkmessages[k],NULL);
      elog(DEBUG1, "%s: creating tuple with : %d", __func__,attinmeta->tupdesc->natts);

      values = (char **) palloc(sizeof(char *) * attinmeta->tupdesc->natts);
      // cols = (char *)rkmessages[k]->payload;
      tofree = cols = strndup((char *)rkmessages[k]->payload,rkmessages[k]->len);
      // assert(cols != NULL);
      // col = strsep(cols, ",");

      while ((col = strsep(&cols, ",")) != NULL)
      {
        elog(DEBUG1, "%s: setting tuple %d with : %s", __func__,i,col);
        values[i] = psprintf("%s", col);
        i++;
      }
      free(tofree);

      values[i] = psprintf("%d", rkmessages[k]->len);
      values[i+1] = psprintf("%d", rkmessages[k]->offset);
      tuple = BuildTupleFromCStrings(festate->attinmeta, values);
      ExecStoreTuple(tuple, slot, InvalidBuffer, false);

      rd_kafka_message_destroy(  rkmessages[k]);

    festate->curr_message = festate->curr_message + 1;
    	run = 0;
      return slot;
    }


	/* get the next record, if any, and fill in the slot */

  elog(DEBUG1, "%s: return ", __func__);
	/* then return the slot */
  if(!found) {
    elog(DEBUG1, "%s: return NULL ", __func__);
    return ExecClearTuple(slot);
  }
	return slot;
}


static void
kafkaReScanForeignScan(ForeignScanState *node)
{
	/*
	 * Restart the scan from the beginning. Note that any parameters the scan
	 * depends on may have changed value, so the new scan does not necessarily
	 * return exactly the same rows.
	 */

	elog(DEBUG1, "entering function %s", __func__);

  // call rd_kafka_resp_err_t rd_kafka_seek (rd_kafka_topic_t *rkt,
  //                                    int32_t partition,
  //                                    int64_t offset,
  //                                    int timeout_ms);


}


static void
kafkaEndForeignScan(ForeignScanState *node)
{
	/*
	 * End the scan and release resources. It is normally not important to
	 * release palloc'd memory, but for example open files and connections to
	 * remote servers should be cleaned up.
	 */
   rd_kafka_topic_t *rkt;
   kafkaFdwExecutionState *festate = (kafkaFdwExecutionState *) node->fdw_state;
   rkt = festate->context;

	elog(DEBUG1, "entering function %s", __func__);

  /* Stop consuming */
  rd_kafka_consume_stop(rkt, 0);
  // rd_kafka_consume_stop(rkt, partition);

	/* Destroy topic */
	rd_kafka_topic_destroy(rkt);

  pfree(festate->messages);

	/* Destroy handle */
	// rd_kafka_destroy(rkConsumer);
  // rkConsumer = NULL;

}


#if (PG_VERSION_NUM >= 90300)
static void
kafkaAddForeignUpdateTargets(Query *parsetree,
								 RangeTblEntry *target_rte,
								 Relation target_relation)
{
	/*
	 * UPDATE and DELETE operations are performed against rows previously
	 * fetched by the table-scanning functions. The FDW may need extra
	 * information, such as a row ID or the values of primary-key columns, to
	 * ensure that it can identify the exact row to update or delete. To
	 * support that, this function can add extra hidden, or "junk", target
	 * columns to the list of columns that are to be retrieved from the
	 * foreign table during an UPDATE or DELETE.
	 *
	 * To do that, add TargetEntry items to parsetree->targetList, containing
	 * expressions for the extra values to be fetched. Each such entry must be
	 * marked resjunk = true, and must have a distinct resname that will
	 * identify it at execution time. Avoid using names matching ctidN or
	 * wholerowN, as the core system can generate junk columns of these names.
	 *
	 * This function is called in the rewriter, not the planner, so the
	 * information available is a bit different from that available to the
	 * planning routines. parsetree is the parse tree for the UPDATE or DELETE
	 * command, while target_rte and target_relation describe the target
	 * foreign table.
	 *
	 * If the AddForeignUpdateTargets pointer is set to NULL, no extra target
	 * expressions are added. (This will make it impossible to implement
	 * DELETE operations, though UPDATE may still be feasible if the FDW
	 * relies on an unchanging primary key to identify rows.)
	 */

	elog(DEBUG1, "entering function %s", __func__);

}


static List *
kafkaPlanForeignModify(PlannerInfo *root,
						   ModifyTable *plan,
						   Index resultRelation,
						   int subplan_index)
{
	/*
	 * Perform any additional planning actions needed for an insert, update,
	 * or delete on a foreign table. This function generates the FDW-private
	 * information that will be attached to the ModifyTable plan node that
	 * performs the update action. This private information must have the form
	 * of a List, and will be delivered to BeginForeignModify during the
	 * execution stage.
	 *
	 * root is the planner's global information about the query. plan is the
	 * ModifyTable plan node, which is complete except for the fdwPrivLists
	 * field. resultRelation identifies the target foreign table by its
	 * rangetable index. subplan_index identifies which target of the
	 * ModifyTable plan node this is, counting from zero; use this if you want
	 * to index into plan->plans or other substructure of the plan node.
	 *
	 * If the PlanForeignModify pointer is set to NULL, no additional
	 * plan-time actions are taken, and the fdw_private list delivered to
	 * BeginForeignModify will be NIL.
	 */

	elog(DEBUG1, "entering function %s", __func__);

	return NULL;
}


static void
kafkaBeginForeignModify(ModifyTableState *mtstate,
							ResultRelInfo *rinfo,
							List *fdw_private,
							int subplan_index,
							int eflags)
{
	/*
	 * Begin executing a foreign table modification operation. This routine is
	 * called during executor startup. It should perform any initialization
	 * needed prior to the actual table modifications. Subsequently,
	 * ExecForeignInsert, ExecForeignUpdate or ExecForeignDelete will be
	 * called for each tuple to be inserted, updated, or deleted.
	 *
	 * mtstate is the overall state of the ModifyTable plan node being
	 * executed; global data about the plan and execution state is available
	 * via this structure. rinfo is the ResultRelInfo struct describing the
	 * target foreign table. (The ri_FdwState field of ResultRelInfo is
	 * available for the FDW to store any private state it needs for this
	 * operation.) fdw_private contains the private data generated by
	 * PlanForeignModify, if any. subplan_index identifies which target of the
	 * ModifyTable plan node this is. eflags contains flag bits describing the
	 * executor's operating mode for this plan node.
	 *
	 * Note that when (eflags & EXEC_FLAG_EXPLAIN_ONLY) is true, this function
	 * should not perform any externally-visible actions; it should only do
	 * the minimum required to make the node state valid for
	 * ExplainForeignModify and EndForeignModify.
	 *
	 * If the BeginForeignModify pointer is set to NULL, no action is taken
	 * during executor startup.
	 */
   rd_kafka_topic_t *rkt;
  StringInfoData broker_str;
   char *brokers = "192.168.99.100:9092";
   char mode = 'C';
   char *topic = NULL;
   int partition = 0;// RD_KAFKA_PARTITION_UA;
   int opt;
   rd_kafka_conf_t *conf;
   rd_kafka_topic_conf_t *topic_conf;
   char errstr[512];
   const char *debug = NULL;
   int64_t start_offset = 0;
  int report_offsets = 0;
   int do_conf_dump = 0;
   char tmp[16];
  int batch_size = 1000;
  kafkaTableOptions table_options;
  kafkaFdwExecutionState *festate;
  rd_kafka_message_t **rkmessages = NULL;

 //elog(DEBUG1, "entering function %s", __func__);
  start_offset = RD_KAFKA_OFFSET_BEGINNING;

  /* Topic configuration */
  topic_conf = rd_kafka_topic_conf_new();

  /* Kafka configuration */
  conf = rd_kafka_conf_new();

  /* Producer config */
  rd_kafka_conf_set(conf, "queue.buffering.max.messages", "500000",
        NULL, 0);
  rd_kafka_conf_set(conf, "message.send.max.retries", "3", NULL, 0);
  rd_kafka_conf_set(conf, "retry.backoff.ms", "500", NULL, 0);

  /* Fetch options  */
  //elog(DEBUG1, "%s: getting options", __func__);
 kafkaGetOptions(RelationGetRelid(rinfo->ri_RelationDesc),
         &table_options);
  topic = table_options.topic;

  elog(DEBUG1, "%s: broker string, %s:%d", __func__,table_options.address,table_options.port);
  initStringInfo(&broker_str);
  appendStringInfo(&broker_str,"%s:%d",table_options.address,table_options.port);
  brokers = broker_str.data;

	elog(DEBUG1, "entering function %s", __func__);

    if(rkProducer == NULL)
    {
      elog(DEBUG1, "%s: creating producer", __func__);
      /* Create Kafka handle */
      if (!(rkProducer = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
  					errstr, sizeof(errstr)))) {
              ereport(ERROR,
                  (errno,
                   errmsg("%% Failed to createKafka producer: %s\n",errstr),
                       errdetail("kafka_fdw:1260.")));
  		}

      elog(DEBUG1, "%s: setting up brokers: %s", __func__, brokers);
  		/* Add brokers */
  		if (rd_kafka_brokers_add(rkProducer, brokers) < 1) {
        ereport(ERROR,
            (errno,
             errmsg("%% No valid brokers specified\n"),
                 errdetail("fdw:772.")));
  		}
    }
    else
    {
      elog(DEBUG1, "%s: using existing producer", __func__);
    }

    elog(DEBUG1, "%s: creating the topic: %s", __func__, topic);
		/* Create topic */
		rkt = rd_kafka_topic_new(rkProducer, topic, topic_conf);



  /* Stash away the state info we have already */
  festate = (kafkaFdwExecutionState *) palloc(sizeof(kafkaFdwExecutionState));
  rinfo->ri_FdwState = (void *) festate;

  festate->context = rkt;
  festate->reply = NULL;
  festate->row = 0;
  festate->address = table_options.address;
  festate->port = table_options.port;
  festate->keyprefix = table_options.keyprefix;
  festate->keyset = table_options.keyset;
  festate->offset = table_options.offset;
  festate->timeout = table_options.timeout;
  festate->partition = table_options.partition;
  festate->table_type = table_options.table_type;
  festate->cursor_id = NULL;
  festate->cursor_search_string = NULL;
  festate->messages = rkmessages;
  festate->curr_message = 0;
  festate->max_message = -1;

  elog(DEBUG2, "%s: DONE.", __func__);

}


static TupleTableSlot *
kafkaExecForeignInsert(EState *estate,
						   ResultRelInfo *rinfo,
						   TupleTableSlot *slot,
						   TupleTableSlot *planSlot)
{
	/*
	 * Insert one tuple into the foreign table. estate is global execution
	 * state for the query. rinfo is the ResultRelInfo struct describing the
	 * target foreign table. slot contains the tuple to be inserted; it will
	 * match the rowtype definition of the foreign table. planSlot contains
	 * the tuple that was generated by the ModifyTable plan node's subplan; it
	 * differs from slot in possibly containing additional "junk" columns.
	 * (The planSlot is typically of little interest for INSERT cases, but is
	 * provided for completeness.)
	 *
	 * The return value is either a slot containing the data that was actually
	 * inserted (this might differ from the data supplied, for example as a
	 * result of trigger actions), or NULL if no row was actually inserted
	 * (again, typically as a result of triggers). The passed-in slot can be
	 * re-used for this purpose.
	 *
	 * The data in the returned slot is used only if the INSERT query has a
	 * RETURNING clause. Hence, the FDW could choose to optimize away
	 * returning some or all columns depending on the contents of the
	 * RETURNING clause. However, some slot must be returned to indicate
	 * success, or the query's reported rowcount will be wrong.
	 *
	 * If the ExecForeignInsert pointer is set to NULL, attempts to insert
	 * into the foreign table will fail with an error message.
	 *
	 */

   rd_kafka_topic_t *rkt;

   int rows;
   int ret;
   char *sbuf;
   char *key = NULL;
   int outq;
   int keylen = 0;
   int msgsize = 0;
   off_t rof = 0;
   char errstr[512];
   int sendflags = 0;
   int partition = 0; //partitions ? partitions[0] : RD_KAFKA_PARTITION_UA;

   elog(DEBUG1, "%s: execute foreign table insert.", __func__);

   kafkaFdwExecutionState *festate = (kafkaFdwExecutionState *) rinfo->ri_FdwState;
   MemoryContext oldcontext;

   rkt = festate->context;

	 elog(DEBUG1, "%s: execute foreign table insert on %d", __func__, RelationGetRelid(rinfo->ri_RelationDesc));
   sbuf = flatten_tup(slot,&msgsize);
   elog(DEBUG1,"%s: slot values to send: (%s)",__func__,sbuf);
   elog(DEBUG1,"%s: slot length to send: (%d)",__func__,msgsize);


   /* Force duplication of payload */
   sendflags |= RD_KAFKA_MSG_F_COPY;

  //  rd_kafka_poll(rk, 1000);


     ret = rd_kafka_produce(rkt, partition,
           sendflags, sbuf, msgsize,
           key, keylen, NULL);
     elog(DEBUG1, "%s: produce returned: %d",__func__, ret);
     if (ret !=0 )
     {
       if (errno == ESRCH)
         ereport(ERROR,
                  (errno,
                   errmsg("%% No such partition:" "%"PRId32"\n", partition),
                       errdetail("kafka_fdw:1458.")));
       else if (errno != ENOBUFS )
         ereport(ERROR,
                (errno,
                 errmsg("%% produce error: %s%s\n",
                        rd_kafka_err2str(
                          rd_kafka_errno2err(
                            errno)),
                        errno == ENOBUFS ?
                        " (backpressure)":"")));

    }
       /* Poll to handle delivery reports */
       rd_kafka_poll(rkProducer, 0);
   //
  //                              print_stats(rk, mode, otype, compression);
  //    }
   //
  //    msgs_wait_cnt++;
  //    cnt.msgs++;
  //    cnt.bytes += msgsize;
   //
  //                      if (rate_sleep)
  //                              usleep(rate_sleep);
   //

  //  cnt.msgs -= outq;
  //  cnt.bytes -= msgsize * outq;
   //
  //  cnt.t_end = t_end;
//##############################################################
  // 	++fdw_state->rowcount;
  // 	dml_in_transaction = true;

	// MemoryContextReset(festate->temp_cxt);
	// oldcontext = MemoryContextSwitchTo(festate->temp_cxt);
  //
  // 	/* extract the values from the slot and store them in the parameters */
  // 	setModifyParameters(fdw_state->paramList, slot, planSlot, fdw_state->oraTable, fdw_state->session);
  //
  // 	/* execute the INSERT statement and store RETURNING values in oraTable's columns */
  // 	rows = oracleExecuteQuery(fdw_state->session, fdw_state->oraTable, fdw_state->paramList);
  //
  // 	if (rows != 1)
  // 		ereport(ERROR,
  // 				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
  // 				errmsg("INSERT on Oracle table added %d rows instead of one in iteration %lu", rows, fdw_state->rowcount)));
  //
	// MemoryContextSwitchTo(oldcontext);

	/* empty the result slot */
	// ExecClearTuple(slot);

	/* convert result for RETURNING to arrays of values and null indicators */
	// convertTuple(festate, slot->tts_values, slot->tts_isnull, false);

	/* store the virtual tuple */
	ExecStoreVirtualTuple(slot);

	return slot;
}


static TupleTableSlot *
kafkaExecForeignUpdate(EState *estate,
						   ResultRelInfo *rinfo,
						   TupleTableSlot *slot,
						   TupleTableSlot *planSlot)
{
	/*
	 * Update one tuple in the foreign table. estate is global execution state
	 * for the query. rinfo is the ResultRelInfo struct describing the target
	 * foreign table. slot contains the new data for the tuple; it will match
	 * the rowtype definition of the foreign table. planSlot contains the
	 * tuple that was generated by the ModifyTable plan node's subplan; it
	 * differs from slot in possibly containing additional "junk" columns. In
	 * particular, any junk columns that were requested by
	 * AddForeignUpdateTargets will be available from this slot.
	 *
	 * The return value is either a slot containing the row as it was actually
	 * updated (this might differ from the data supplied, for example as a
	 * result of trigger actions), or NULL if no row was actually updated
	 * (again, typically as a result of triggers). The passed-in slot can be
	 * re-used for this purpose.
	 *
	 * The data in the returned slot is used only if the UPDATE query has a
	 * RETURNING clause. Hence, the FDW could choose to optimize away
	 * returning some or all columns depending on the contents of the
	 * RETURNING clause. However, some slot must be returned to indicate
	 * success, or the query's reported rowcount will be wrong.
	 *
	 * If the ExecForeignUpdate pointer is set to NULL, attempts to update the
	 * foreign table will fail with an error message.
	 *
	 */

	elog(DEBUG1, "entering function %s", __func__);

	return slot;
}


static TupleTableSlot *
kafkaExecForeignDelete(EState *estate,
						   ResultRelInfo *rinfo,
						   TupleTableSlot *slot,
						   TupleTableSlot *planSlot)
{
	/*
	 * Delete one tuple from the foreign table. estate is global execution
	 * state for the query. rinfo is the ResultRelInfo struct describing the
	 * target foreign table. slot contains nothing useful upon call, but can
	 * be used to hold the returned tuple. planSlot contains the tuple that
	 * was generated by the ModifyTable plan node's subplan; in particular, it
	 * will carry any junk columns that were requested by
	 * AddForeignUpdateTargets. The junk column(s) must be used to identify
	 * the tuple to be deleted.
	 *
	 * The return value is either a slot containing the row that was deleted,
	 * or NULL if no row was deleted (typically as a result of triggers). The
	 * passed-in slot can be used to hold the tuple to be returned.
	 *
	 * The data in the returned slot is used only if the DELETE query has a
	 * RETURNING clause. Hence, the FDW could choose to optimize away
	 * returning some or all columns depending on the contents of the
	 * RETURNING clause. However, some slot must be returned to indicate
	 * success, or the query's reported rowcount will be wrong.
	 *
	 * If the ExecForeignDelete pointer is set to NULL, attempts to delete
	 * from the foreign table will fail with an error message.
	 */

	elog(DEBUG1, "entering function %s", __func__);

	return slot;
}


static void
kafkaEndForeignModify(EState *estate,
						  ResultRelInfo *rinfo)
{
	/*
	 * End the table update and release resources. It is normally not
	 * important to release palloc'd memory, but for example open files and
	 * connections to remote servers should be cleaned up.
	 *
	 * If the EndForeignModify pointer is set to NULL, no action is taken
	 * during executor shutdown.
	 */
   	int outq;
  rd_kafka_topic_t *rkt;
  kafkaFdwExecutionState *festate = (kafkaFdwExecutionState *) rinfo->ri_FdwState;
  rkt = festate->context;

 elog(DEBUG1, "%s: entering function ", __func__);

 /* Must poll to handle delivery reports */
 rd_kafka_poll(rkProducer, 100);
 // /* Wait for messages to be delivered */
 // rd_kafka_poll(rk, 1000);
 outq = rd_kafka_outq_len(rkProducer);
 elog(DEBUG2,"%s: %i messages in outq\n", __func__,outq) ;
// rd_kafka_poll(rkProducer, 0);


 /* Destroy topic */
 rd_kafka_topic_destroy(rkt);

 /* Destroy handle */
 // rd_kafka_destroy(rk);

 elog(DEBUG1, "%s: DONE ", __func__);

}

static int
kafkaIsForeignRelUpdatable(Relation rel)
{
	/*
	 * Report which update operations the specified foreign table supports.
	 * The return value should be a bit mask of rule event numbers indicating
	 * which operations are supported by the foreign table, using the CmdType
	 * enumeration; that is, (1 << CMD_UPDATE) = 4 for UPDATE, (1 <<
	 * CMD_INSERT) = 8 for INSERT, and (1 << CMD_DELETE) = 16 for DELETE.
	 *
	 * If the IsForeignRelUpdatable pointer is set to NULL, foreign tables are
	 * assumed to be insertable, updatable, or deletable if the FDW provides
	 * ExecForeignInsert, ExecForeignUpdate, or ExecForeignDelete
	 * respectively. This function is only needed if the FDW supports some
	 * tables that are updatable and some that are not. (Even then, it's
	 * permissible to throw an error in the execution routine instead of
	 * checking in this function. However, this function is used to determine
	 * updatability for display in the information_schema views.)
	 */

	elog(DEBUG1, "entering function %s", __func__);
  return (1 << CMD_INSERT);
	// return (1 << CMD_UPDATE) | (1 << CMD_INSERT) | (1 << CMD_DELETE);
}
#endif


static void
kafkaExplainForeignScan(ForeignScanState *node,
							struct ExplainState * es)
{
	/*
	 * Print additional EXPLAIN output for a foreign table scan. This function
	 * can call ExplainPropertyText and related functions to add fields to the
	 * EXPLAIN output. The flag fields in es can be used to determine what to
	 * print, and the state of the ForeignScanState node can be inspected to
	 * provide run-time statistics in the EXPLAIN ANALYZE case.
	 *
	 * If the ExplainForeignScan pointer is set to NULL, no additional
	 * information is printed during EXPLAIN.
	 */

	elog(DEBUG1, "entering function %s", __func__);

}


#if (PG_VERSION_NUM >= 90300)
static void
kafkaExplainForeignModify(ModifyTableState *mtstate,
							  ResultRelInfo *rinfo,
							  List *fdw_private,
							  int subplan_index,
							  struct ExplainState * es)
{
	/*
	 * Print additional EXPLAIN output for a foreign table update. This
	 * function can call ExplainPropertyText and related functions to add
	 * fields to the EXPLAIN output. The flag fields in es can be used to
	 * determine what to print, and the state of the ModifyTableState node can
	 * be inspected to provide run-time statistics in the EXPLAIN ANALYZE
	 * case. The first four arguments are the same as for BeginForeignModify.
	 *
	 * If the ExplainForeignModify pointer is set to NULL, no additional
	 * information is printed during EXPLAIN.
	 */

	elog(DEBUG1, "entering function %s", __func__);

}
#endif


#if (PG_VERSION_NUM >= 90200)
static bool
kafkaAnalyzeForeignTable(Relation relation,
							 AcquireSampleRowsFunc *func,
							 BlockNumber *totalpages)
{
	/* ----
	 * This function is called when ANALYZE is executed on a foreign table. If
	 * the FDW can collect statistics for this foreign table, it should return
	 * true, and provide a pointer to a function that will collect sample rows
	 * from the table in func, plus the estimated size of the table in pages
	 * in totalpages. Otherwise, return false.
	 *
	 * If the FDW does not support collecting statistics for any tables, the
	 * AnalyzeForeignTable pointer can be set to NULL.
	 *
	 * If provided, the sample collection function must have the signature:
	 *
	 *	  int
	 *	  AcquireSampleRowsFunc (Relation relation, int elevel,
	 *							 HeapTuple *rows, int targrows,
	 *							 double *totalrows,
	 *							 double *totaldeadrows);
	 *
	 * A random sample of up to targrows rows should be collected from the
	 * table and stored into the caller-provided rows array. The actual number
	 * of rows collected must be returned. In addition, store estimates of the
	 * total numbers of live and dead rows in the table into the output
	 * parameters totalrows and totaldeadrows. (Set totaldeadrows to zero if
	 * the FDW does not have any concept of dead rows.)
	 * ----
	 */

	elog(DEBUG1, "entering function %s", __func__);

	return false;
}
#endif


#if (PG_VERSION_NUM >= 90500)
static void
kafkaGetForeignJoinPaths(PlannerInfo *root,
							 RelOptInfo *joinrel,
							 RelOptInfo *outerrel,
							 RelOptInfo *innerrel,
							 JoinType jointype,
							 JoinPathExtraData *extra)
{
	/*
	 * Create possible access paths for a join of two (or more) foreign tables
	 * that all belong to the same foreign server. This optional function is
	 * called during query planning. As with GetForeignPaths, this function
	 * should generate ForeignPath path(s) for the supplied joinrel, and call
	 * add_path to add these paths to the set of paths considered for the
	 * join. But unlike GetForeignPaths, it is not necessary that this
	 * function succeed in creating at least one path, since paths involving
	 * local joining are always possible.
	 *
	 * Note that this function will be invoked repeatedly for the same join
	 * relation, with different combinations of inner and outer relations; it
	 * is the responsibility of the FDW to minimize duplicated work.
	 *
	 * If a ForeignPath path is chosen for the join, it will represent the
	 * entire join process; paths generated for the component tables and
	 * subsidiary joins will not be used. Subsequent processing of the join
	 * path proceeds much as it does for a path scanning a single foreign
	 * table. One difference is that the scanrelid of the resulting
	 * ForeignScan plan node should be set to zero, since there is no single
	 * relation that it represents; instead, the fs_relids field of the
	 * ForeignScan node represents the set of relations that were joined. (The
	 * latter field is set up automatically by the core planner code, and need
	 * not be filled by the FDW.) Another difference is that, because the
	 * column list for a remote join cannot be found from the system catalogs,
	 * the FDW must fill fdw_scan_tlist with an appropriate list of
	 * TargetEntry nodes, representing the set of columns it will supply at
	 * runtime in the tuples it returns.
	 */

	elog(DEBUG1, "entering function %s", __func__);

}


static RowMarkType
kafkaGetForeignRowMarkType(RangeTblEntry *rte,
							   LockClauseStrength strength)
{
	/*
	 * Report which row-marking option to use for a foreign table. rte is the
	 * RangeTblEntry node for the table and strength describes the lock
	 * strength requested by the relevant FOR UPDATE/SHARE clause, if any. The
	 * result must be a member of the RowMarkType enum type.
	 *
	 * This function is called during query planning for each foreign table
	 * that appears in an UPDATE, DELETE, or SELECT FOR UPDATE/SHARE query and
	 * is not the target of UPDATE or DELETE.
	 *
	 * If the GetForeignRowMarkType pointer is set to NULL, the ROW_MARK_COPY
	 * option is always used. (This implies that RefetchForeignRow will never
	 * be called, so it need not be provided either.)
	 */

	elog(DEBUG1, "entering function %s", __func__);

	return ROW_MARK_COPY;

}

static HeapTuple
kafkaRefetchForeignRow(EState *estate,
						   ExecRowMark *erm,
						   Datum rowid,
						   bool *updated)
{
	/*
	 * Re-fetch one tuple from the foreign table, after locking it if
	 * required. estate is global execution state for the query. erm is the
	 * ExecRowMark struct describing the target foreign table and the row lock
	 * type (if any) to acquire. rowid identifies the tuple to be fetched.
	 * updated is an output parameter.
	 *
	 * This function should return a palloc'ed copy of the fetched tuple, or
	 * NULL if the row lock couldn't be obtained. The row lock type to acquire
	 * is defined by erm->markType, which is the value previously returned by
	 * GetForeignRowMarkType. (ROW_MARK_REFERENCE means to just re-fetch the
	 * tuple without acquiring any lock, and ROW_MARK_COPY will never be seen
	 * by this routine.)
	 *
	 * In addition, *updated should be set to true if what was fetched was an
	 * updated version of the tuple rather than the same version previously
	 * obtained. (If the FDW cannot be sure about this, always returning true
	 * is recommended.)
	 *
	 * Note that by default, failure to acquire a row lock should result in
	 * raising an error; a NULL return is only appropriate if the SKIP LOCKED
	 * option is specified by erm->waitPolicy.
	 *
	 * The rowid is the ctid value previously read for the row to be
	 * re-fetched. Although the rowid value is passed as a Datum, it can
	 * currently only be a tid. The function API is chosen in hopes that it
	 * may be possible to allow other datatypes for row IDs in future.
	 *
	 * If the RefetchForeignRow pointer is set to NULL, attempts to re-fetch
	 * rows will fail with an error message.
	 */

	elog(DEBUG1, "entering function %s", __func__);

	return NULL;

}


static List *
kafkaImportForeignSchema(ImportForeignSchemaStmt *stmt,
							 Oid serverOid)
{
	/*
	 * Obtain a list of foreign table creation commands. This function is
	 * called when executing IMPORT FOREIGN SCHEMA, and is passed the parse
	 * tree for that statement, as well as the OID of the foreign server to
	 * use. It should return a list of C strings, each of which must contain a
	 * CREATE FOREIGN TABLE command. These strings will be parsed and executed
	 * by the core server.
	 *
	 * Within the ImportForeignSchemaStmt struct, remote_schema is the name of
	 * the remote schema from which tables are to be imported. list_type
	 * identifies how to filter table names: FDW_IMPORT_SCHEMA_ALL means that
	 * all tables in the remote schema should be imported (in this case
	 * table_list is empty), FDW_IMPORT_SCHEMA_LIMIT_TO means to include only
	 * tables listed in table_list, and FDW_IMPORT_SCHEMA_EXCEPT means to
	 * exclude the tables listed in table_list. options is a list of options
	 * used for the import process. The meanings of the options are up to the
	 * FDW. For example, an FDW could use an option to define whether the NOT
	 * NULL attributes of columns should be imported. These options need not
	 * have anything to do with those supported by the FDW as database object
	 * options.
	 *
	 * The FDW may ignore the local_schema field of the
	 * ImportForeignSchemaStmt, because the core server will automatically
	 * insert that name into the parsed CREATE FOREIGN TABLE commands.
	 *
	 * The FDW does not have to concern itself with implementing the filtering
	 * specified by list_type and table_list, either, as the core server will
	 * automatically skip any returned commands for tables excluded according
	 * to those options. However, it's often useful to avoid the work of
	 * creating commands for excluded tables in the first place. The function
	 * IsImportableForeignTable() may be useful to test whether a given
	 * foreign-table name will pass the filter.
	 */

	elog(DEBUG1, "entering function %s", __func__);
  rd_kafka_topic_t *rkt;
   char *brokers = "localhost:9092";
   char mode = 'C';
   char *topic = "test";
   int partition = 0;// RD_KAFKA_PARTITION_UA;
   int opt;
   rd_kafka_conf_t *conf;
   rd_kafka_topic_conf_t *topic_conf;
   char errstr[512];
   const char *debug = NULL;
   int64_t start_offset = 0;
          int report_offsets = 0;
   int do_conf_dump = 0;
   char tmp[16];
   kafkaTableOptions table_options;
   kafkaFdwExecutionState *festate;
   int run = 1;
   int i;

   elog(DEBUG1, "entering function %s", __func__);


  List       *commands = NIL;
	rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;

  /* Topic configuration */
  topic_conf = rd_kafka_topic_conf_new();

  /* Kafka configuration */
  conf = rd_kafka_conf_new();

		/* Create Kafka handle */
		if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
					errstr, sizeof(errstr)))) {
			fprintf(stderr,
				"%% Failed to create new producer: %s\n",
				errstr);
			exit(1);
		}

		/* Set logger */
		// rd_kafka_set_logger(rk, logger);
		// rd_kafka_set_log_level(rk, LOG_DEBUG);

		/* Add brokers */
		if (rd_kafka_brokers_add(rk, brokers) == 0) {
			fprintf(stderr, "%% No valid brokers specified\n");
			exit(1);
		}

                /* Create topic */
                if (topic)
                        rkt = rd_kafka_topic_new(rk, topic, topic_conf);
                else
                        rkt = NULL;

                while (run) {
                        rd_kafka_metadata_t *metadata;

                        /* Fetch metadata */
                        elog(DEBUG1, "%s: Fetch metadata\n ", __func__);
                        err = rd_kafka_metadata(rk, rkt ? 0 : 1, rkt,
                                                &metadata, 5000);
                        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                                fprintf(stderr,
                                        "%% Failed to acquire metadata: %s\n",
                                        rd_kafka_err2str(err));
                                run = 0;
                                break;
                        }
                        elog(DEBUG1, "%s: creating cmds for  %i topics:\n ", __func__, metadata->topic_cnt);
                        // metadata_print(topic, metadata);

                        //
                        for (i = 0 ; i < metadata->topic_cnt ; i++) {
                                rd_kafka_metadata_topic_t *t = &metadata->topics[i];
                        //         printf("  topic \"%s\" with %i partitions:",
                        //                t->topic,
                        //                t->partition_cnt);
                        //         if (t->err) {
                        //                 printf(" %s", rd_kafka_err2str(t->err));
                        //                 if (t->err == RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE)
                        //                         printf(" (try again)");
                        //         }
                        //         printf("\n");
                        //
                        //         /* Iterate topic's partitions */
                        //         for (j = 0 ; j < t->partition_cnt ; j++) {
                        //                 const struct rd_kafka_metadata_partition *p;
                        //                 p = &t->partitions[j];
                        //                 printf("    partition %"PRId32", "
                        //                        "leader %"PRId32", replicas: ",
                        //                        p->id, p->leader);
                        //
                        //                 /* Iterate partition's replicas */
                        //                 for (k = 0 ; k < p->replica_cnt ; k++)
                        //                         printf("%s%"PRId32,
                        //                                k > 0 ? ",":"", p->replicas[k]);
                        //
                        //                 /* Iterate partition's ISRs */
                        //                 printf(", isrs: ");
                        //                 for (k = 0 ; k < p->isr_cnt ; k++)
                        //                         printf("%s%"PRId32,
                        //                                k > 0 ? ",":"", p->isrs[k]);
                        //                 if (p->err)
                        //                         printf(", %s\n", rd_kafka_err2str(p->err));
                        //                 else
                        //                         printf("\n");
                        //         }
                        // }





                          StringInfoData buf;
                          /* Create workspace for strings */
                          initStringInfo(&buf);
                          /* code */

                          appendStringInfo(&buf, "CREATE FOREIGN TABLE %s (message text, len int, offs int)",
                                          quote_identifier(t->topic));
                          appendStringInfo(&buf, "\nSERVER %s\nOPTIONS (",
                                          quote_identifier("kafka_server"));
                          appendStringInfoString(&buf, "partition \'0\'");
                          // deparseStringLiteral(&buf, "0");
                          appendStringInfoString(&buf, ");");

                          elog(DEBUG1, "%s: adding cmd: %s\n ", __func__,buf.data);
                          commands = lappend(commands, pstrdup(buf.data));
                          resetStringInfo(&buf);
                        }

                        rd_kafka_metadata_destroy(metadata);

                        run = 0;
                }

		/* Destroy topic */
		if (rkt)
			rd_kafka_topic_destroy(rkt);

		/* Destroy the handle */
		rd_kafka_destroy(rk);


  // return NULL;
	return commands; //TODO: uncomment this when we want it to work.
}

#endif

/*
 * Fetch the options for a kafka_fdw foreign table.
 */
static void
kafkaGetOptions(Oid foreigntableid, kafkaTableOptions *table_options)
{
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *mapping;
	List	   *options;
	ListCell   *lc;

#ifdef DEBUG
	elog(NOTICE, "kafkaGetOptions");
#endif

	/*
	 * Extract options from FDW objects. We only need to worry about server
	 * options for kafka
	 *
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	mapping = GetUserMapping(GetUserId(), table->serverid);

	options = NIL;
	options = list_concat(options, table->options);
	options = list_concat(options, server->options);
	options = list_concat(options, mapping->options);

  elog(DEBUG1, "%s: Default",__func__);
  /* Defaults */
  table_options->offset = RD_KAFKA_OFFSET_BEGINNING;

	/* Loop through the options, and get the server/port */
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "address") == 0)
			table_options->address = strVal(def->arg);

		if (strcmp(def->defname, "port") == 0){
			table_options->port = strtol(strVal(def->arg),NULL,10);
    }
    if (strcmp(def->defname, "topic") == 0)
			table_options->topic = strVal(def->arg);

		if (strcmp(def->defname, "partition") == 0){
			table_options->partition = strtol(strVal(def->arg),NULL,10);
    }
    if (strcmp(def->defname, "timeout") == 0){
        table_options->timeout = strtol(strVal(def->arg),NULL,10);
      }
		if (strcmp(def->defname, "tablekeyprefix") == 0)
			table_options->keyprefix = defGetString(def);

		if (strcmp(def->defname, "tablekeyset") == 0)
			table_options->keyset = defGetString(def);

		if (strcmp(def->defname, "offset") == 0)
    {
      char	   *typeval = strVal(def->arg);
      // elog(DEBUG1, "%s: offset: %s",__func__,typeval);
      if (strcmp(typeval, "stored") == 0){
        // elog(DEBUG1, "%s: offset atored",__func__);
				table_options->offset = RD_KAFKA_OFFSET_STORED;
      }
			else if (strcmp(typeval, "beginning") == 0){
        // elog(DEBUG1, "%s: offset beginning",__func__);
				table_options->offset = RD_KAFKA_OFFSET_BEGINNING;
      }
			//table_options->offset = defGetString(def);
    }

		if (strcmp(def->defname, "tabletype") == 0)
		{
			char	   *typeval = defGetString(def);

			if (strcmp(typeval, "hash") == 0)
				table_options->table_type = PG_kafka_HASH_TABLE;
			else if (strcmp(typeval, "list") == 0)
				table_options->table_type = PG_kafka_LIST_TABLE;
			else if (strcmp(typeval, "set") == 0)
				table_options->table_type = PG_kafka_SET_TABLE;
			else if (strcmp(typeval, "zset") == 0)
				table_options->table_type = PG_kafka_ZSET_TABLE;
			/* XXX detect error here */
		}
	}
  #ifdef DEBUG
  	elog(DEBUG1, "Setting default values");
  #endif
	/* Default values, if required */
	if (!table_options->address)
		table_options->address = "127.0.0.1";

	if (!table_options->port)
		table_options->port = 9092;

	if (!table_options->partition)
		table_options->partition = 0;

    #ifdef DEBUG
      elog(DEBUG1, "broker list will be server %s", table_options->address);
      elog(DEBUG1, "broker list will be port %d", table_options->port);
    #endif

}


/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
kafkaIsValidOption(const char *option, Oid context)
{
	struct KafkaFdwOption *opt;

#ifdef DEBUG
	elog(NOTICE, "kafkaIsValidOption");
#endif

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}


// static void metadata_print (const char *topic,
//                             const struct rd_kafka_metadata *metadata) {
//         int i, j, k;
//
//         printf("Metadata for %s (from broker %"PRId32": %s):\n",
//                topic ? : "all topics",
//                metadata->orig_broker_id,
//                metadata->orig_broker_name);
//
//
//         /* Iterate brokers */
//         printf(" %i brokers:\n", metadata->broker_cnt);
//         for (i = 0 ; i < metadata->broker_cnt ; i++)
//                 printf("  broker %"PRId32" at %s:%i\n",
//                        metadata->brokers[i].id,
//                        metadata->brokers[i].host,
//                        metadata->brokers[i].port);
//
//         /* Iterate topics */
//         printf(" %i topics:\n", metadata->topic_cnt);
//         for (i = 0 ; i < metadata->topic_cnt ; i++) {
//                 const struct rd_kafka_metadata_topic *t = &metadata->topics[i];
//                 printf("  topic \"%s\" with %i partitions:",
//                        t->topic,
//                        t->partition_cnt);
//                 if (t->err) {
//                         printf(" %s", rd_kafka_err2str(t->err));
//                         if (t->err == RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE)
//                                 printf(" (try again)");
//                 }
//                 printf("\n");
//
//                 /* Iterate topic's partitions */
//                 for (j = 0 ; j < t->partition_cnt ; j++) {
//                         const struct rd_kafka_metadata_partition *p;
//                         p = &t->partitions[j];
//                         printf("    partition %"PRId32", "
//                                "leader %"PRId32", replicas: ",
//                                p->id, p->leader);
//
//                         /* Iterate partition's replicas */
//                         for (k = 0 ; k < p->replica_cnt ; k++)
//                                 printf("%s%"PRId32,
//                                        k > 0 ? ",":"", p->replicas[k]);
//
//                         /* Iterate partition's ISRs */
//                         printf(", isrs: ");
//                         for (k = 0 ; k < p->isr_cnt ; k++)
//                                 printf("%s%"PRId32,
//                                        k > 0 ? ",":"", p->isrs[k]);
//                         if (p->err)
//                                 printf(", %s\n", rd_kafka_err2str(p->err));
//                         else
//                                 printf("\n");
//                 }
//         }
// }

static char *flatten_tup(TupleTableSlot *slot, int *msglen)
 {
   TupleDesc   typeinfo = slot->tts_tupleDescriptor;
   int         natts = typeinfo->natts;
   int         i;
   Datum       attr;
   char       *value;
   char       *string;
   bool        isnull;
   Oid         typoutput;
   bool        typisvarlena;
   StringInfo  buf;
   /* Create workspace for strings */
   buf = makeStringInfo();

   /* code */
   i=0;
   attr = slot_getattr(slot, i + 1, &isnull);
   if (!isnull){
   getTypeOutputInfo(typeinfo->attrs[i]->atttypid,
   &typoutput, &typisvarlena);
   value = OidOutputFunctionCall(typoutput, attr);
   //printatt((unsigned) i + 1, typeinfo->attrs[i], value);
   appendStringInfo(buf,"%s",value);
    }

   for (i = 1; i < natts; ++i)
   {
     attr = slot_getattr(slot, i + 1, &isnull);
     if (isnull)
       continue;
     getTypeOutputInfo(typeinfo->attrs[i]->atttypid,
     &typoutput, &typisvarlena);
     value = OidOutputFunctionCall(typoutput, attr);
     //printatt((unsigned) i + 1, typeinfo->attrs[i], value);
     appendStringInfo(buf,",%s",value);
   }
   *msglen = buf->len;
   return buf->data;
 }
