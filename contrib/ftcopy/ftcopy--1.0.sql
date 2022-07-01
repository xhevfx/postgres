-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION ftcopy" to load this file. \quit

CREATE FUNCTION ftcopy(tablename text, filename text,
						 enforce_length boolean = false,
						 disable_triggers boolean = false,
						 execute_artrigger_immediatly boolean = true,
						 update_process_title boolean = true,
						 merge boolean = false,
						 csv boolean = true,
						 header boolean = false,
						 rejectmax int = 20,
						 exception_log text = NULL,
						 rejected_data_log text = NULL,
						 multi_insert boolean = false,
						 use_wal boolean = true)
RETURNS int
AS 'MODULE_PATHNAME', 'ftcopy'
LANGUAGE C;
