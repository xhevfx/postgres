/*
 * This file contains a code adopted from builtin copy routines. It suppport file parsing.
 * A reason for duplicating is possible using a enforced explicit casting (enforce_length).
 * We don't support full functionality of internal copy, so adopted code is reduced.
 *
 */
#include "ftcopy.h"

#include "commands/copy.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
#include "utils/rel.h"

/*
 * !Attention: following structures must be compatible with private
 * structures from commands/copy.c
 */

/*
 * Represents the different source/dest cases we need to worry about at
 * the bottom level
 */
typedef enum CopyDest
{
	COPY_FILE,					/* to/from file */
	COPY_OLD_FE,				/* to/from frontend (2.0 protocol) */
	COPY_NEW_FE					/* to/from frontend (3.0 protocol) */
} CopyDest;

/*
 *	Represents the end-of-line terminator type of the input
 */
typedef enum EolType
{
	EOL_UNKNOWN,
	EOL_NL,
	EOL_CR,
	EOL_CRNL
} EolType;

/*
 * This struct contains all the state variables used throughout a COPY
 * operation. For simplicity, we use the same struct for all variants of COPY,
 * even though some fields are used in only some cases.
 *
 * Multi-byte encodings: all supported client-side encodings encode multi-byte
 * characters by having the first byte's high bit set. Subsequent bytes of the
 * character can have the high bit not set. When scanning data in such an
 * encoding to look for a match to a single-byte (ie ASCII) character, we must
 * use the full pg_encoding_mblen() machinery to skip over multibyte
 * characters, else we might find a false match to a trailing byte. In
 * supported server encodings, there is no possibility of a false match, and
 * it's faster to make useless comparisons to trailing bytes than it is to
 * invoke pg_encoding_mblen() to skip over them. encoding_embeds_ascii is TRUE
 * when we have to do it the hard way.
 */
typedef struct CopyStateData
{
	/* low-level state data */
	CopyDest	copy_dest;		/* type of copy source/destination */
	FILE	   *copy_file;		/* used if copy_dest == COPY_FILE */
	StringInfo	fe_msgbuf;		/* used for all dests during COPY TO, only for
								 * dest == COPY_NEW_FE in COPY FROM */
	bool		fe_eof;			/* true if detected end of copy data */
	EolType		eol_type;		/* EOL type of input */
	int			file_encoding;	/* file or remote side's character encoding */
	bool		need_transcoding;		/* file encoding diff from server? */
	bool		encoding_embeds_ascii;	/* ASCII can be non-first byte? */

	/* parameters from the COPY command */
	Relation	rel;			/* relation to copy to or from */
	QueryDesc  *queryDesc;		/* executable query to copy from */
	List	   *attnumlist;		/* integer list of attnums to copy */
	char	   *filename;		/* filename, or NULL for STDIN/STDOUT */
	bool		binary;			/* binary format? */
	bool		oids;			/* include OIDs? */

/* attention ! */
	bool		freeze;			/* freeze rows on loading? */
/* attention */

	bool		csv_mode;		/* Comma Separated Value format? */
	bool		header_line;	/* CSV header line? */
	char	   *null_print;		/* NULL marker string (server encoding!) */
	int			null_print_len; /* length of same */
	char	   *null_print_client;		/* same converted to file encoding */
	char	   *delim;			/* column delimiter (must be 1 byte) */
	char	   *quote;			/* CSV quote char (must be 1 byte) */
	char	   *escape;			/* CSV escape char (must be 1 byte) */
	List	   *force_quote;	/* list of column names */
	bool		force_quote_all;	/* FORCE QUOTE *? */
	bool	   *force_quote_flags;		/* per-column CSV FQ flags */
	List	   *force_notnull;	/* list of column names */
	bool	   *force_notnull_flags;	/* per-column CSV FNN flags */
	bool		convert_selectively;	/* do selective binary conversion? */
	List	   *convert_select;	/* list of column names (can be NIL) */
	bool	   *convert_select_flags;	/* per-column CSV/TEXT CS flags */

	/* these are just for error messages, see CopyFromErrorCallback */
	const char *cur_relname;	/* table name for error messages */
	int			cur_lineno;		/* line number for error messages */
	const char *cur_attname;	/* current att for error messages */
	const char *cur_attval;		/* current att value for error messages */

	/*
	 * Working state for COPY TO/FROM
	 */
	MemoryContext copycontext;	/* per-copy execution context */

	/*
	 * Working state for COPY TO
	 */
	FmgrInfo   *out_functions;	/* lookup info for output functions */
	MemoryContext rowcontext;	/* per-row evaluation context */

	/*
	 * Working state for COPY FROM
	 */
	AttrNumber	num_defaults;
	bool		file_has_oids;
	FmgrInfo	oid_in_function;
	Oid			oid_typioparam;
	FmgrInfo   *in_functions;	/* array of input functions for each attrs */
	Oid		   *typioparams;	/* array of element types for in_functions */
	int		   *defmap;			/* array of default att numbers */
	ExprState **defexprs;		/* array of default att expressions */
	bool		volatile_defexprs;		/* is any of defexprs volatile? */

	/*
	 * These variables are used to reduce overhead in textual COPY FROM.
	 *
	 * attribute_buf holds the separated, de-escaped text for each field of
	 * the current line.  The CopyReadAttributes functions return arrays of
	 * pointers into this buffer.  We avoid palloc/pfree overhead by re-using
	 * the buffer on each cycle.
	 */
	StringInfoData attribute_buf;

	/* field raw data pointers found by COPY FROM */

	int			max_fields;
	char	  **raw_fields;

	/*
	 * Similarly, line_buf holds the whole input line being processed. The
	 * input cycle is first to read the whole line into line_buf, convert it
	 * to server encoding there, and then extract the individual attribute
	 * fields into attribute_buf.  line_buf is preserved unmodified so that we
	 * can display it in error messages if appropriate.
	 */
	StringInfoData line_buf;
	bool		line_buf_converted;		/* converted to server encoding? */

	/*
	 * Finally, raw_buf holds raw data read from the data source (file or
	 * client connection).	CopyReadLine parses this data sufficiently to
	 * locate line boundaries, then transfers the data to line_buf and
	 * converts it.  Note: we guarantee that there is a \0 at
	 * raw_buf[raw_buf_len].
	 */
#define RAW_BUF_SIZE 65536		/* we palloc RAW_BUF_SIZE+1 bytes */
	char	   *raw_buf;
	int			raw_buf_index;	/* next byte to process */
	int			raw_buf_len;	/* total # of bytes stored */
} CopyStateData;

/*
 * trivial methods that enable access to private CopyState properties
 */
int
ftCurrentLineno(CopyState cstate)
{
	return cstate->cur_lineno;
}

char *
ftCurrentLineData(CopyState cstate)
{
	return cstate->line_buf.data;
}

void
ftSetErrorContext(CopyState cstate, int lineno, char *data)
{
	cstate->cur_lineno = lineno;
	cstate->line_buf.data = data;
}

/*
 * Read next tuple from file for COPY FROM. Return false if no more tuples.
 *
 * 'econtext' is used to evaluate default expression for each columns not
 * read from the file. It can be NULL when no default values are used, i.e.
 * when all columns are read from the file.
 *
 * 'values' and 'nulls' arrays must be the same length as columns of the
 * relation passed to BeginCopyFrom. This function fills the arrays.
 * Oid of the tuple is returned with 'tupleOid' separately.
 */
bool
ftNextCopyFrom(CopyState cstate, ExprContext *econtext,
			 Datum *values, bool *nulls, Oid *tupleOid,
			 bool enforce_length)
{
	TupleDesc	tupDesc;
	Form_pg_attribute *attr;
	AttrNumber	num_phys_attrs,
				attr_count,
				num_defaults = cstate->num_defaults;
	FmgrInfo   *in_functions = cstate->in_functions;
	Oid		   *typioparams = cstate->typioparams;
	int			i;
	int			nfields;
	bool		file_has_oids = cstate->file_has_oids;
	int		   *defmap = cstate->defmap;
	ExprState **defexprs = cstate->defexprs;
	char	  **field_strings;
	ListCell   *cur;
	int			fldct;
	int			fieldno;
	char	   *string;

	tupDesc = RelationGetDescr(cstate->rel);
	attr = tupDesc->attrs;
	num_phys_attrs = tupDesc->natts;
	attr_count = list_length(cstate->attnumlist);
	nfields = file_has_oids ? (attr_count + 1) : attr_count;

	/* Initialize all values for row to NULL */
	MemSet(values, 0, num_phys_attrs * sizeof(Datum));
	MemSet(nulls, true, num_phys_attrs * sizeof(bool));

	/* read raw fields in the next line */
	if (!NextCopyFromRawFields(cstate, &field_strings, &fldct))
		return false;

	/* check for overflowing fields */
	if (nfields > 0 && fldct > nfields)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("extra data after last expected column")));

	fieldno = 0;

	/* Read the OID field if present */
	if (file_has_oids)
	{
		if (fieldno >= fldct)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("missing data for OID column")));
		string = field_strings[fieldno++];

		if (string == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("null OID in COPY data")));
		else if (cstate->oids && tupleOid != NULL)
		{
			cstate->cur_attname = "oid";
			cstate->cur_attval = string;
			*tupleOid = DatumGetObjectId(DirectFunctionCall1(oidin,
											   CStringGetDatum(string)));
			if (*tupleOid == InvalidOid)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("invalid OID in COPY data")));
			cstate->cur_attname = NULL;
			cstate->cur_attval = NULL;
		}
	}

	/* Loop to read the user attributes on the line. */
	foreach(cur, cstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		int			m = attnum - 1;

		if (fieldno >= fldct)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("missing data for column \"%s\"",
							NameStr(attr[m]->attname))));
		string = field_strings[fieldno++];

		if (cstate->csv_mode && string == NULL &&
			cstate->force_notnull_flags[m])
		{
			/* Go ahead and read the NULL string */
			string = cstate->null_print;
		}

		cstate->cur_attname = NameStr(attr[m]->attname);
		cstate->cur_attval = string;

		if (!enforce_length || attr[m]->atttypmod == -1)
		{
			values[m] = InputFunctionCall(&in_functions[m],
								  string,
								  typioparams[m],
								  attr[m]->atttypmod);
		}
		else
		{
			Oid	funcid;
			CoercionPathType   coercion_type;

			coercion_type = find_typmod_coercion_function(attr[m]->atttypid, &funcid);

			if (coercion_type == COERCION_PATH_FUNC)
			{
				/*
				 * When we can use function for typmod coercion, then we can enforce
				 * a explicit casting. If we have not this function, then we can do it.
				 * It is not a issue - then typmod is not important or only IO cast 
				 * is available - and it is equalent of original behave.
				 */
				values[m] = InputFunctionCall(&in_functions[m],
										  string,
										  typioparams[m],
										  -1);

				/* call typmod coercion function now */
				values[m] = OidFunctionCall3(funcid,
									values[m],
									Int32GetDatum(attr[m]->atttypmod),
									BoolGetDatum(true));
			}
			else
			{
				/* we have no typmod coercion function, or typmod is not important */
				values[m] = InputFunctionCall(&in_functions[m],
										  string,
										  typioparams[m],
										  attr[m]->atttypmod);
			}
		}

		if (string != NULL)
			nulls[m] = false;
		cstate->cur_attname = NULL;
		cstate->cur_attval = NULL;
	}

	Assert(fieldno == nfields);

	/*
	 * Now compute and insert any defaults available for the columns not
	 * provided by the input data.	Anything not processed here or above will
	 * remain NULL.
	 */
	for (i = 0; i < num_defaults; i++)
	{
		/*
		 * The caller must supply econtext and have switched into the
		 * per-tuple memory context in it.
		 */
		Assert(econtext != NULL);
		Assert(CurrentMemoryContext == econtext->ecxt_per_tuple_memory);

		values[defmap[i]] = ExecEvalExpr(defexprs[i], econtext,
										 &nulls[defmap[i]], NULL);
	}

	return true;
}

void
ftCopyFromInsertBatch(ftCopyState *ftcstate,
						BulkInsertState bistate,
						int nBufferedTuples,
						HeapTuple *bufferedTuples)
{
	MemoryContext oldcontext;
	int			i;
	TupleTableSlot *slot = ftcstate->myslot;

	/*
	 * heap_multi_insert leaks memory, so switch to short-lived memory context
	 * before calling it.
	 */
	oldcontext = MemoryContextSwitchTo(GetPerTupleMemoryContext(ftcstate->estate));
	heap_multi_insert(ftcstate->cstate->rel,
					  bufferedTuples,
					  nBufferedTuples,
					  ftcstate->mycid,
					  ftcstate->hi_options,
					  bistate);
	MemoryContextSwitchTo(oldcontext);

	/*
	 * If there are any indexes, update them for all the inserted tuples, and
	 * run AFTER ROW INSERT triggers.
	 * Attention - After triggers can be executed more times - it has no impact on data, but
	 * it can have a performance impact.
	 */
	if (ftcstate->resultRelInfo->ri_NumIndices > 0)
	{
		for (i = 0; i < nBufferedTuples; i++)
		{
			List	   *recheckIndexes;
			HeapTuple	tuple = bufferedTuples[i];

			ExecStoreTuple(tuple, ftcstate->myslot, InvalidBuffer, false);
			recheckIndexes =
				ExecInsertIndexTuples(ftcstate->myslot, &(tuple->t_self),
									  ftcstate->estate);

			if (!ftcstate->disable_triggers)
			{
				if (!ftcstate->execute_ARtrigger_immediatly)
				{
					ExecARInsertTriggers(ftcstate->estate, ftcstate->resultRelInfo, tuple,
											 recheckIndexes);
				}
				else
				{
					 if (recheckIndexes != NULL)
						elog(ERROR, "cannot use immediate after row triggers execution on relation with exclusion indexes");

					ExecStoreTuple(tuple, slot, InvalidBuffer, false);

					if (ftcstate->resultRelInfo->ri_TrigDesc != NULL &&
									ftcstate->resultRelInfo->ri_TrigDesc->trig_insert_after_row)
						ImmediatelyExecARInsertTriggers(ftcstate->estate, ftcstate->resultRelInfo, slot);
				}
			}

			ExecClearTuple(slot);

			list_free(recheckIndexes);
		}
	}

	/*
	 * There's no indexes, but see if we need to run AFTER ROW INSERT triggers
	 * anyway.
	 */

	else if (ftcstate->resultRelInfo->ri_TrigDesc != NULL &&
			 ftcstate->resultRelInfo->ri_TrigDesc->trig_insert_after_row)
	{
		for (i = 0; i < nBufferedTuples; i++)
		{
			HeapTuple tuple = bufferedTuples[i];

			if (!ftcstate->disable_triggers)
			{
				if (!ftcstate->execute_ARtrigger_immediatly)
				{
					ExecARInsertTriggers(ftcstate->estate, ftcstate->resultRelInfo, tuple,
											 NULL);
				}
				else
				{
					if (ftcstate->resultRelInfo->ri_TrigDesc != NULL &&
									ftcstate->resultRelInfo->ri_TrigDesc->trig_insert_after_row)
					{
						ExecStoreTuple(tuple, slot, InvalidBuffer, false);
						ImmediatelyExecARInsertTriggers(ftcstate->estate, ftcstate->resultRelInfo, slot);
					}
				}
			}

			ExecClearTuple(slot);
		}
	}
}
