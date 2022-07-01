#ifndef FTCOPY_H
#define FTCOPY_H

#include "postgres.h"

#include "commands/copy.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "utils/resowner.h"

#define REPLAY_BUFFER_SIZE		1000


typedef struct {
	bool	enforce_length;
	bool	disable_triggers;
	bool	execute_ARtrigger_immediatly;
	int	rejectmax;
	bool	update_process_title;
	bool	useHeapMultiInsert;

	int	processed;
	int	errors;

	int	saved_tuples;
	int	replayed_tuples;

	char		*line_buf_data[REPLAY_BUFFER_SIZE];
	HeapTuple	replay_buffer[REPLAY_BUFFER_SIZE];
	int		line_nos[REPLAY_BUFFER_SIZE];
	MemoryContext	replayCtxt;
	bool		replay_is_active;

	bool		safeguarded;

	MemoryContext	procCtxt;
	MemoryContext	heapMultiInsertCtxt;
	ResourceOwner	procResOwner;

	EState	*estate;
	CopyState	cstate;
	BulkInsertState bistate;
	ResultRelInfo *resultRelInfo;
	TupleTableSlot *myslot;
	int	hi_options;
	CommandId mycid;
} ftCopyState;


extern bool ftNextCopyFrom(CopyState cstate, ExprContext *econtext, Datum *values, bool *nulls, Oid *tupleOid,
												   bool enforce_length);
extern TupleTableSlot *ImmediatelyExecARInsertTriggers(EState *estate, ResultRelInfo *relinfo, TupleTableSlot *slot);

extern void ftCopyFromInsertBatch(ftCopyState *ftcstate, BulkInsertState bistateCopyState,
								int nBufferedTuples, HeapTuple *bufferedTuples);

extern int ftCurrentLineno(CopyState cstate);
extern char *ftCurrentLineData(CopyState cstate);
extern void ftSetErrorContext(CopyState cstate, int lineno, char *data);



#endif	/* FTCOPY_H */
