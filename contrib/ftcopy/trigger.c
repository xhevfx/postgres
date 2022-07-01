/*
 * This file contains a code adopted from builtin copy routines. It suppport file parsing.
 * A reason for duplicating is possible using a enforcing a immediately execution of
 * After Insert triggers.
 */

#include "postgres.h"
#include "pgstat.h"

#include "ftcopy.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/pg_trigger.h"
#include "commands/trigger.h"
#include "nodes/execnodes.h"
#include "optimizer/clauses.h"
#include "rewrite/rewriteManip.h"
#include "utils/rel.h"


/*
 * Is trigger enabled to fire?
 */
static bool
TriggerEnabled(EState *estate, ResultRelInfo *relinfo,
			   Trigger *trigger, TriggerEvent event,
			   Bitmapset *modifiedCols,
			   HeapTuple oldtup, HeapTuple newtup)
{
	/* Check replication-role-dependent enable state */
	if (SessionReplicationRole == SESSION_REPLICATION_ROLE_REPLICA)
	{
		if (trigger->tgenabled == TRIGGER_FIRES_ON_ORIGIN ||
			trigger->tgenabled == TRIGGER_DISABLED)
			return false;
	}
	else	/* ORIGIN or LOCAL role */
	{
		if (trigger->tgenabled == TRIGGER_FIRES_ON_REPLICA ||
			trigger->tgenabled == TRIGGER_DISABLED)
			return false;
	}

	/*
	 * Check for column-specific trigger (only possible for UPDATE, and in
	 * fact we *must* ignore tgattr for other event types)
	 */
	if (trigger->tgnattr > 0 && TRIGGER_FIRED_BY_UPDATE(event))
	{
		int			i;
		bool		modified;

		modified = false;
		for (i = 0; i < trigger->tgnattr; i++)
		{
			if (bms_is_member(trigger->tgattr[i] - FirstLowInvalidHeapAttributeNumber,
							  modifiedCols))
			{
				modified = true;
				break;
			}
		}
		if (!modified)
			return false;
	}

	/* Check for WHEN clause */
	if (trigger->tgqual)
	{
		TupleDesc	tupdesc = RelationGetDescr(relinfo->ri_RelationDesc);
		List	  **predicate;
		ExprContext *econtext;
		TupleTableSlot *oldslot = NULL;
		TupleTableSlot *newslot = NULL;
		MemoryContext oldContext;
		int			i;

		Assert(estate != NULL);

		/*
		 * trigger is an element of relinfo->ri_TrigDesc->triggers[]; find the
		 * matching element of relinfo->ri_TrigWhenExprs[]
		 */
		i = trigger - relinfo->ri_TrigDesc->triggers;
		predicate = &relinfo->ri_TrigWhenExprs[i];

		/*
		 * If first time through for this WHEN expression, build expression
		 * nodetrees for it.  Keep them in the per-query memory context so
		 * they'll survive throughout the query.
		 */
		if (*predicate == NIL)
		{
			Node	   *tgqual;

			oldContext = MemoryContextSwitchTo(estate->es_query_cxt);
			tgqual = stringToNode(trigger->tgqual);
			/* Change references to OLD and NEW to INNER_VAR and OUTER_VAR */
			ChangeVarNodes(tgqual, PRS2_OLD_VARNO, INNER_VAR, 0);
			ChangeVarNodes(tgqual, PRS2_NEW_VARNO, OUTER_VAR, 0);
			/* ExecQual wants implicit-AND form */
			tgqual = (Node *) make_ands_implicit((Expr *) tgqual);
			*predicate = (List *) ExecPrepareExpr((Expr *) tgqual, estate);
			MemoryContextSwitchTo(oldContext);
		}

		/*
		 * We will use the EState's per-tuple context for evaluating WHEN
		 * expressions (creating it if it's not already there).
		 */
		econtext = GetPerTupleExprContext(estate);

		/*
		 * Put OLD and NEW tuples into tupleslots for expression evaluation.
		 * These slots can be shared across the whole estate, but be careful
		 * that they have the current resultrel's tupdesc.
		 */
		if (HeapTupleIsValid(oldtup))
		{
			if (estate->es_trig_oldtup_slot == NULL)
			{
				oldContext = MemoryContextSwitchTo(estate->es_query_cxt);
				estate->es_trig_oldtup_slot = ExecInitExtraTupleSlot(estate);
				MemoryContextSwitchTo(oldContext);
			}
			oldslot = estate->es_trig_oldtup_slot;
			if (oldslot->tts_tupleDescriptor != tupdesc)
				ExecSetSlotDescriptor(oldslot, tupdesc);
			ExecStoreTuple(oldtup, oldslot, InvalidBuffer, false);
		}
		if (HeapTupleIsValid(newtup))
		{
			if (estate->es_trig_newtup_slot == NULL)
			{
				oldContext = MemoryContextSwitchTo(estate->es_query_cxt);
				estate->es_trig_newtup_slot = ExecInitExtraTupleSlot(estate);
				MemoryContextSwitchTo(oldContext);
			}
			newslot = estate->es_trig_newtup_slot;
			if (newslot->tts_tupleDescriptor != tupdesc)
				ExecSetSlotDescriptor(newslot, tupdesc);
			ExecStoreTuple(newtup, newslot, InvalidBuffer, false);
		}

		/*
		 * Finally evaluate the expression, making the old and/or new tuples
		 * available as INNER_VAR/OUTER_VAR respectively.
		 */
		econtext->ecxt_innertuple = oldslot;
		econtext->ecxt_outertuple = newslot;
		if (!ExecQual(*predicate, econtext, false))
			return false;
	}

	return true;
}


/*
 * Call a trigger function.
 *
 *		trigdata: trigger descriptor.
 *		tgindx: trigger's index in finfo and instr arrays.
 *		finfo: array of cached trigger function call information.
 *		instr: optional array of EXPLAIN ANALYZE instrumentation state.
 *		per_tuple_context: memory context to execute the function in.
 *
 * Returns the tuple (or NULL) as returned by the function.
 */
static HeapTuple
ExecCallTriggerFunc(TriggerData *trigdata,
					int tgindx,
					FmgrInfo *finfo,
					Instrumentation *instr,
					MemoryContext per_tuple_context)
{
	FunctionCallInfoData fcinfo;
	PgStat_FunctionCallUsage fcusage;
	Datum		result;
	MemoryContext oldContext;

	finfo += tgindx;

	/*
	 * We cache fmgr lookup info, to avoid making the lookup again on each
	 * call.
	 */
	if (finfo->fn_oid == InvalidOid)
		fmgr_info(trigdata->tg_trigger->tgfoid, finfo);

	Assert(finfo->fn_oid == trigdata->tg_trigger->tgfoid);

	/*
	 * If doing EXPLAIN ANALYZE, start charging time to this trigger.
	 */
	if (instr)
		InstrStartNode(instr + tgindx);

	/*
	 * Do the function evaluation in the per-tuple memory context, so that
	 * leaked memory will be reclaimed once per tuple. Note in particular that
	 * any new tuple created by the trigger function will live till the end of
	 * the tuple cycle.
	 */
	oldContext = MemoryContextSwitchTo(per_tuple_context);

	/*
	 * Call the function, passing no arguments but setting a context.
	 */
	InitFunctionCallInfoData(fcinfo, finfo, 0,
							 InvalidOid, (Node *) trigdata, NULL);

	pgstat_init_function_usage(&fcinfo, &fcusage);

	PG_TRY();
	{
		result = FunctionCallInvoke(&fcinfo);
	}
	PG_CATCH();
	{
		PG_RE_THROW();
	}
	PG_END_TRY();

	pgstat_end_function_usage(&fcusage, true);

	MemoryContextSwitchTo(oldContext);

	/*
	 * Trigger protocol allows function to return a null pointer, but NOT to
	 * set the isnull result flag.
	 */
	if (fcinfo.isnull)
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("trigger function %u returned null value",
						fcinfo.flinfo->fn_oid)));

	/*
	 * If doing EXPLAIN ANALYZE, stop charging time to this trigger, and count
	 * one "tuple returned" (really the number of firings).
	 */
	if (instr)
		InstrStopNode(instr + tgindx, 1);

	return (HeapTuple) DatumGetPointer(result);
}

TupleTableSlot *
ImmediatelyExecARInsertTriggers(EState *estate, ResultRelInfo *relinfo,
					 TupleTableSlot *slot)
{
	TriggerDesc *trigdesc = relinfo->ri_TrigDesc;
	HeapTuple	slottuple = ExecMaterializeSlot(slot);
	HeapTuple	newtuple = slottuple;
	TriggerData LocTriggerData;
	int			i;

	LocTriggerData.type = T_TriggerData;
	LocTriggerData.tg_event = TRIGGER_EVENT_INSERT |
		TRIGGER_EVENT_ROW |
		TRIGGER_EVENT_AFTER;
	LocTriggerData.tg_relation = relinfo->ri_RelationDesc;
	LocTriggerData.tg_newtuple = NULL;
	LocTriggerData.tg_newtuplebuf = InvalidBuffer;
	for (i = 0; i < trigdesc->numtriggers; i++)
	{
		Trigger    *trigger = &trigdesc->triggers[i];

		if (!TRIGGER_TYPE_MATCHES(trigger->tgtype,
								  TRIGGER_TYPE_ROW,
								  TRIGGER_TYPE_AFTER,
								  TRIGGER_TYPE_INSERT))
			continue;
		if (!TriggerEnabled(estate, relinfo, trigger, LocTriggerData.tg_event,
							NULL, NULL, newtuple))
			continue;

		LocTriggerData.tg_trigtuple = newtuple;
		LocTriggerData.tg_trigtuplebuf = InvalidBuffer;
		LocTriggerData.tg_trigger = trigger;
		newtuple = ExecCallTriggerFunc(&LocTriggerData,
									   i,
									   relinfo->ri_TrigFunctions,
									   relinfo->ri_TrigInstrument,
									   GetPerTupleMemoryContext(estate));

		if (newtuple != slottuple && newtuple != NULL)
		{
			heap_freetuple(newtuple);
			newtuple = slottuple;
		}
	}

	return slot;
}
