#include "postgres.h"

#include "miscadmin.h"

#include "ftcopy.h"

#include "access/hio.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/copy.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/resowner.h"
#include "utils/ps_status.h"

PG_MODULE_MAGIC;

Datum ftcopy(PG_FUNCTION_ARGS);


/*
 * Initialize fault tolerant copy state variables
 */
static void
init_ftCopyState(ftCopyState *ftcstate,
					 bool enforce_length,
					 bool disable_triggers,
					 bool execute_ARtrigger_immediatly,
					 bool update_process_title,
					 int rejectmax,
					 bool multi_insert,
					 bool use_wal)
{
	if (disable_triggers && execute_ARtrigger_immediatly)
		elog(WARNING, "execute_ARtrigger_immediatly is ignored");

	ftcstate->enforce_length = enforce_length;
	ftcstate->disable_triggers = disable_triggers;
	ftcstate->execute_ARtrigger_immediatly = execute_ARtrigger_immediatly;
	ftcstate->rejectmax = rejectmax;
	ftcstate->update_process_title = update_process_title;

	ftcstate->procCtxt = CurrentMemoryContext;
	ftcstate->replayCtxt = AllocSetContextCreate(CurrentMemoryContext,
										"Replay buffer context",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);

	ftcstate->heapMultiInsertCtxt = AllocSetContextCreate(CurrentMemoryContext,
										"Heap Multi Insert context",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);

	ftcstate->procResOwner = CurrentResourceOwner;

	ftcstate->processed = 0;
	ftcstate->errors = 0;

	ftcstate->saved_tuples = 0;
	ftcstate->replayed_tuples = 0;

	ftcstate->replay_is_active = false;
	ftcstate->safeguarded = false;

	ftcstate->bistate = NULL;
	ftcstate->hi_options = use_wal ? 0 : HEAP_INSERT_SKIP_WAL;

	ftcstate->useHeapMultiInsert = multi_insert;
	ftcstate->mycid = GetCurrentCommandId(true);
}

static void
release_ftCopyState(ftCopyState *ftcstate)
{
	MemoryContextDelete(ftcstate->replayCtxt);
	MemoryContextDelete(ftcstate->heapMultiInsertCtxt);
}

/*
 * Subtransaction start
 */
static void
Begin(ftCopyState *ftcstate)
{
	if (!ftcstate->safeguarded)
	{
		BeginInternalSubTransaction(NULL);
		MemoryContextSwitchTo(ftcstate->procCtxt);

		ftcstate->safeguarded = true;

		Assert(ftcstate->bistate == NULL);
		ftcstate->bistate = GetBulkInsertState();
	}
}

/*
 * Subtransaction rollback
 */
static void
Rollback(ftCopyState *ftcstate)
{
	Assert(ftcstate->safeguarded);

	if (ftcstate->bistate != NULL)
	{
		FreeBulkInsertState(ftcstate->bistate);
		ftcstate->bistate = NULL;
	}

	RollbackAndReleaseCurrentSubTransaction();
	MemoryContextSwitchTo(ftcstate->procCtxt);
	CurrentResourceOwner = ftcstate->procResOwner;

	ftcstate->safeguarded = false;
}

/*
 * Subtransaction commit
 */
static void
Commit(ftCopyState *ftcstate)
{
	Assert(ftcstate->safeguarded);

	if (ftcstate->bistate != NULL)
	{
		FreeBulkInsertState(ftcstate->bistate);
		ftcstate->bistate = NULL;
	}

	/* Commit the inner transaction, return to outer xact context */
	ReleaseCurrentSubTransaction();
	MemoryContextSwitchTo(ftcstate->procCtxt);
	CurrentResourceOwner = ftcstate->procResOwner;

	/* after commit we don't need return to buffer */
	ftcstate->saved_tuples = 0;

	/* every commit finish replay if any */
	ftcstate->replay_is_active = false;

	MemoryContextReset(ftcstate->replayCtxt);

	MemoryContextReset(ftcstate->heapMultiInsertCtxt);

	ftcstate->safeguarded = false;
}

/*
 * store valid verified tuple for possible replay to buffer.
 * does commit, when buffer is commit.
 */
static void
saveTuple(ftCopyState *ftcstate, HeapTuple tuple)
{
	ResourceOwner oldres;

	Assert(ftcstate->safeguarded);
	Assert(!ftcstate->replay_is_active);
	Assert(tuple != NULL);

	if (ftcstate->saved_tuples >= REPLAY_BUFFER_SIZE)
		Commit(ftcstate);


	oldres = CurrentResourceOwner;
	MemoryContextSwitchTo(ftcstate->replayCtxt);
	CurrentResourceOwner = ftcstate->procResOwner;

	ftcstate->replay_buffer[ftcstate->saved_tuples++] = heap_copytuple(tuple);

	MemoryContextSwitchTo(ftcstate->procCtxt);

	CurrentResourceOwner = oldres;
}

/*
 * push tuple to replay_buffer - used for heapMultiInsert mode
 */
static void
pushTuple(ftCopyState *ftcstate, HeapTuple tuple)
{
	ResourceOwner oldres = CurrentResourceOwner;

	Assert(!ftcstate->replay_is_active);
	Assert(ftcstate->useHeapMultiInsert);
	Assert(tuple != NULL);

	MemoryContextSwitchTo(ftcstate->replayCtxt);
	CurrentResourceOwner = ftcstate->procResOwner;

	ftcstate->replay_buffer[ftcstate->saved_tuples] = heap_copytuple(tuple);

	/* necessary store original line to show valid error context */
	ftcstate->line_nos[ftcstate->saved_tuples] = ftCurrentLineno(ftcstate->cstate);
	ftcstate->line_buf_data[ftcstate->saved_tuples++] = pstrdup(ftCurrentLineData(ftcstate->cstate));

	MemoryContextSwitchTo(ftcstate->procCtxt);

	CurrentResourceOwner = oldres;
}

static void
clean_buffers(ftCopyState *ftcstate)
{
	/* after commit we don't need return to buffer */
	ftcstate->saved_tuples = 0;

	/* every commit finish replay if any */
	ftcstate->replay_is_active = false;
	MemoryContextReset(ftcstate->replayCtxt);
	MemoryContextReset(ftcstate->heapMultiInsertCtxt);
}

/*
 * Flush replay buffer - it is used for multiHeapInser
 *
 * This routine try to push continuous block of inserts - tuples are verified now,
 * but there can be problems with indexes.
 *
 * This routine is implemented for recursive call - so it can push tuples in 
 * different order than is original based on input file orded. This can be changed
 * in future by implementation second strategy.
 *
 * ToDo:  assign context ---- !
 */
static void
flush_replay_buffer(ftCopyState *ftcstate, int lo, int hi)
{
	MemoryContext	oldctxt = CurrentMemoryContext;
	ResourceOwner	oldresowner = CurrentResourceOwner;
	BulkInsertState bistate;

	/*
	 * try to use heap_multiinsert in one subtransactions. When this fails,
	 * does rollback, divide range and try it again
	 */

	/* fast leave */
	if (lo > hi)
		return;

	bistate = GetBulkInsertState();

	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(ftcstate->heapMultiInsertCtxt);

	PG_TRY();
	{
		ftCopyFromInsertBatch(ftcstate, bistate,
					 hi - lo + 1,
					 ftcstate->replay_buffer + lo);

		FreeBulkInsertState(bistate);

		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldctxt);
		CurrentResourceOwner = oldresowner;

	}
	PG_CATCH();
	{

		ErrorData  *edata;

		MemoryContextSwitchTo(oldctxt);
		edata = CopyErrorData();
		FlushErrorState();

		FreeBulkInsertState(bistate);

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldctxt);
		CurrentResourceOwner = oldresowner;

		if (lo == hi)
		{
			/*
			 * Cannot use original internal error context, because it is
			 * developed for streaming processing, and we are proccess
			 * buffered data now.
			 */
			ftSetErrorContext(ftcstate->cstate,
							 ftcstate->line_nos[lo],
							 ftcstate->line_buf_data[lo]);
			ftcstate->errors++;

			elog(NOTICE, "%s", edata->message);
		}
		else
		{
			int range_size = (hi - lo + 1) / 2;

			flush_replay_buffer(ftcstate, lo, lo + range_size - 1);
			flush_replay_buffer(ftcstate, lo + range_size, hi);
		}
	}
	PG_END_TRY();
}

/*
 * process ftcopy
 *
 *
 * CREATE FUNCTION ftcopy(tablename text, filename text,
 * 						 enforce_length boolean = false,
 * 						 disable_triggers boolean = false,
 * 						 execute_artrigger_immediatly boolean = true,
 * 						 update_process_title boolean = false,
 * 						 merge boolean = false,
 * 						 csv boolean = true,
 * 						 header boolean = false,
 * 						 rejectmax int = 20,
 * 						 exception_log text = NULL,
 * 						 rejected_data_log text = NULL,
 * 						 multi_insert bool = false,
 * 						 use_wal bool = true)
 */
PG_FUNCTION_INFO_V1(ftcopy);
Datum
ftcopy(PG_FUNCTION_ARGS)
{
	text *tablename = PG_GETARG_TEXT_PP(0);
	char *filename = text_to_cstring(PG_GETARG_TEXT_PP(1));
	FILE *fp;
	Relation	rel;
	RangeVar	*rv;
	TupleDesc	tupdesc;
	TupleTableSlot		*myslot;
	EState			*estate = CreateExecutorState();
	ResultRelInfo		*resultRelInfo;

	CopyState	cstate;
	ftCopyState	ftcstate;

	ErrorContextCallback	errcallback;

	Datum		*values;
	bool		*nulls;
	Oid	tupleOid;

	ftcstate.hi_options = HEAP_INSERT_SKIP_WAL;

	if( (fp = fopen(filename, "r")) == NULL)
		elog(ERROR, "cannot to open file: \"%s\"", filename);

	init_ftCopyState(&ftcstate,
				    PG_GETARG_BOOL(2),		/* enforce_length */
				    PG_GETARG_BOOL(3),		/* disable_triggers */
				    PG_GETARG_BOOL(4),		/* execute_ARtrigger_immediatly */
				    PG_GETARG_BOOL(5),		/* update process title */
				    PG_GETARG_INT32(9),		/* rejectmax */
				    PG_GETARG_BOOL(12),		/* multi_insert */
				    PG_GETARG_BOOL(13));	/* use WAL */

	rv = makeRangeVarFromNameList(textToQualifiedNameList(tablename));
	rel = heap_openrv(rv, RowExclusiveLock);

	cstate = BeginCopyFrom(rel, filename, NIL, list_make1(makeDefElem("format", (Node *) makeString("csv"))));

	/* Set up callback to identify error line number */
	errcallback.callback = CopyFromErrorCallback;
	errcallback.arg = (void *) cstate;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	tupdesc = RelationGetDescr(rel);

	resultRelInfo = makeNode(ResultRelInfo);
	resultRelInfo->ri_RangeTableIndex = 1;		/* dummy */
	resultRelInfo->ri_RelationDesc = rel;
	resultRelInfo->ri_TrigDesc = CopyTriggerDesc(rel->trigdesc);
	if (resultRelInfo->ri_TrigDesc)
	{
		resultRelInfo->ri_TrigFunctions = (FmgrInfo *)
			palloc0(resultRelInfo->ri_TrigDesc->numtriggers * sizeof(FmgrInfo));
		resultRelInfo->ri_TrigWhenExprs = (List **)
			palloc0(resultRelInfo->ri_TrigDesc->numtriggers * sizeof(List *));
	}
	resultRelInfo->ri_TrigInstrument = NULL;

	ExecOpenIndices(resultRelInfo);

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;

	ftcstate.estate = estate;
	ftcstate.cstate = cstate;
	ftcstate.resultRelInfo = resultRelInfo;

	/* Set up a tuple slot too */
	myslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(myslot, tupdesc);
	ftcstate.myslot = myslot;

	/* Triggers might need a slot as well */
	estate->es_trig_tuple_slot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(estate->es_trig_tuple_slot, tupdesc);

	AfterTriggerBeginQuery();

	if (!ftcstate.disable_triggers)
		ExecBSInsertTriggers(estate, resultRelInfo);

	values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
	nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));

	PG_TRY();
	{
		bool	valid_row = true;

		while (valid_row || ftcstate.replay_is_active)
		{
			if (ftcstate.processed % 1000 == 0)
			{
				CHECK_FOR_INTERRUPTS();

				if (ftcstate.update_process_title && update_process_title)
				{
					char buffer[100];

					snprintf(buffer, 100, "ftcopy proccessed: %d, errors: %d",
											    ftcstate.processed,
											    ftcstate.errors);

					set_ps_display(buffer, false);
				}
			}

			if (ftcstate.processed % 100 == 0)
				ResetPerTupleExprContext(estate);

			if (ftcstate.rejectmax != 0)
				Begin(&ftcstate);

			PG_TRY();
			{
				bool	tuple_is_valid = true;
				HeapTuple	tuple = NULL;

				if (!ftcstate.replay_is_active)
				{
					ExprContext	*econtext;

					MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

					econtext = GetPerTupleExprContext(estate);
					valid_row = ftNextCopyFrom(cstate, econtext, values, nulls, &tupleOid,
													ftcstate.enforce_length);

					if (valid_row)
					{
						tuple = heap_form_tuple(tupdesc, values, nulls);
						// NENI FUNKCNI - zabiji postgres
						//if (tupleOid != InvalidOid)
						//	HeapTupleSetOid(tuple, tupleOid);
					}
					else
						tuple_is_valid = false;

					MemoryContextSwitchTo(ftcstate.procCtxt);
				}
				else
				{
					if (ftcstate.useHeapMultiInsert)
					{
						if (ftcstate.saved_tuples > 0)
							flush_replay_buffer(&ftcstate, 0, ftcstate.saved_tuples - 1);

						if (ftcstate.rejectmax != 0)
							Commit(&ftcstate);
						else
							clean_buffers(&ftcstate);

						tuple_is_valid = false;
					}
					else
					{
						Assert(ftcstate.rejectmax != 0);

						if (ftcstate.replayed_tuples < ftcstate.saved_tuples)
							tuple = ftcstate.replay_buffer[ftcstate.replayed_tuples++];
						else
						{
							/*
							 * There are no not replayed saved tuples. Call commit
							 * and finish replay.
							 */
							Commit(&ftcstate);
							tuple_is_valid = false;
						}
					}
				}

				if (tuple_is_valid)
				{
					TupleTableSlot *slot = myslot;
					bool skip_tuple = false;

					/* triggers and stuff need to be invoked in query context */
					MemoryContextSwitchTo(ftcstate.procCtxt);
					ExecStoreTuple(tuple, slot, InvalidBuffer, false);

					/* BEFORE ROW INSERT Triggers */
					if (resultRelInfo->ri_TrigDesc &&
							resultRelInfo->ri_TrigDesc->trig_insert_before_row &&
							!ftcstate.replay_is_active && !ftcstate.disable_triggers)
					{
						slot = ExecBRInsertTriggers(estate, resultRelInfo, slot);

						if (slot == NULL)	/* "do nothing" */
						{
							skip_tuple = true;
							heap_freetuple(tuple);
							tuple = NULL;
						}
						else	/* trigger might have changed tuple */
							tuple = ExecMaterializeSlot(slot);
					}

					if (!skip_tuple)
					{
						List	   *recheckIndexes = NIL;

						if (rel->rd_att->constr && !ftcstate.replay_is_active)
							ExecConstraints(resultRelInfo, slot, estate);

						if (!ftcstate.useHeapMultiInsert)
						{
							heap_insert(rel, tuple, ftcstate.mycid, ftcstate.hi_options, ftcstate.bistate);

							if (resultRelInfo->ri_NumIndices > 0)
								recheckIndexes = ExecInsertIndexTuples(slot, &(tuple->t_self),
														   estate);

							/* AFTER ROW INSERT Triggers */
							if (!ftcstate.disable_triggers && !ftcstate.replay_is_active)
							{
								if (!ftcstate.execute_ARtrigger_immediatly)
								{
									ExecARInsertTriggers(estate, resultRelInfo, tuple,
													 recheckIndexes);
								}
								else
								{
									/*
									 * we doesn't support recheck for immediate after row triggers
									 * execution - so don't allow it, be imported data consistent.
									 */
									 if (recheckIndexes != NULL)
										elog(ERROR, "cannot use immediate after row triggers execution on relation with exclusion indexes");

									if (resultRelInfo->ri_TrigDesc != NULL &&
													resultRelInfo->ri_TrigDesc->trig_insert_after_row)
										ImmediatelyExecARInsertTriggers(estate, resultRelInfo, slot);
								}
							}

							list_free(recheckIndexes);

							if (!ftcstate.replay_is_active && ftcstate.rejectmax != 0)
								saveTuple(&ftcstate, tuple);
						}
						else
						{
							pushTuple(&ftcstate, tuple);

							if (ftcstate.useHeapMultiInsert && ftcstate.saved_tuples == REPLAY_BUFFER_SIZE)
							{
								flush_replay_buffer(&ftcstate, 0, ftcstate.saved_tuples - 1);

								if (ftcstate.rejectmax != 0)
									Commit(&ftcstate);
								else
									clean_buffers(&ftcstate);
							}
						}
					}

					if (slot != myslot)
						ExecClearTuple(slot);

					ExecClearTuple(myslot);

					tuple = NULL;
				}
			}
			PG_CATCH();
			{
				ErrorData  *edata;

				/* when we are not safegurded or we are in replay forward exception */
				if (!ftcstate.safeguarded || ftcstate.replay_is_active)
					PG_RE_THROW();

				/* Save error info */
				MemoryContextSwitchTo(ftcstate.procCtxt);
				edata = CopyErrorData();

				if (edata->sqlerrcode == ERRCODE_QUERY_CANCELED)
					PG_RE_THROW();

				FlushErrorState();
				elog(NOTICE, "%s", edata->message);

				Rollback(&ftcstate);

				if (++ftcstate.errors >= ftcstate.rejectmax && ftcstate.rejectmax > 0)
					elog(ERROR, "too much errors");

				/* switch to replay mode, when replay buffer is not empty */
				if (ftcstate.saved_tuples > 0)
				{
					ftcstate.replay_is_active = true;
					ftcstate.replayed_tuples = 0;
				}

				FreeErrorData(edata);
			}
			PG_END_TRY();

			/* don't calculate twice */
			if (!ftcstate.replay_is_active && valid_row)
				ftcstate.processed++;
		}

		if (ftcstate.useHeapMultiInsert && ftcstate.saved_tuples > 0)
			flush_replay_buffer(&ftcstate, 0, ftcstate.saved_tuples - 1);

		if (ftcstate.rejectmax != 0)
			Commit(&ftcstate);
		else
			clean_buffers(&ftcstate);

		/* unlock when end of transaction, updates will be commited before */
		heap_close(rel, NoLock);

		fclose(fp);

		MemoryContextSwitchTo(ftcstate.procCtxt);

		release_ftCopyState(&ftcstate);
	}
	PG_CATCH();
	{
		fclose(fp);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Done, clean up */
	error_context_stack = errcallback.previous;

	if (!ftcstate.disable_triggers)
		ExecASInsertTriggers(estate, resultRelInfo);

	AfterTriggerEndQuery(estate);

	ExecResetTupleTable(estate->es_tupleTable, false);
	ExecCloseIndices(resultRelInfo);
	FreeExecutorState(estate);

	EndCopyFrom(cstate);

	PG_RETURN_INT32(ftcstate.processed - ftcstate.errors);
}

