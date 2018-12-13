&SCOPED-DEFINE Sequel PRESENT
{Sequel.i}
{constants.i}


/* Define how to connect to SQL server */
&SCOPED-DEFINE sqlserver 192.168.1.240
&SCOPED-DEFINE sqluser username
&SCOPED-DEFINE sqlpass passw0rd

/* Set these to the SQL database name of your live and test environments */
&SCOPED-DEFINE sqlLiveDB live
&SCOPED-DEFINE sqlTestDB test



&SCOPED-DEFINE batchcount 100


/*******************************************************************************
	SQL SERVER CONNECTIONS
*******************************************************************************/

CREATE "ADODB.Connection":U SequelCon.
CREATE "ADODB.Command":U SequelCmd.
CREATE "ADODB.RecordSet":U SequelRes.


PROCEDURE ConnectLive:
	SequelCon:OPEN("Driver=~{SQL Server};Server={&sqlserver};Database={&sqlLiveDB};UID={&sqluser};PWD={&sqlpass}":U, , , 0).
END PROCEDURE.


PROCEDURE ConnectTest:
	SequelCon:OPEN("Driver=~{SQL Server};Server={&sqlserver};Database={&sqlTestDB};UID={&sqluser};PWD={&sqlpass}":U, , , 0).
END PROCEDURE.


PROCEDURE CloseConnect:
	SequelCon:CLOSE NO-ERROR.
END PROCEDURE.


/*******************************************************************************
	LOGGING
*******************************************************************************/

DEF STREAM oLog.

PROCEDURE openStream:
	OUTPUT STREAM oLog TO VALUE("c:\temp\sequel_log.txt":U) APPEND.
END PROCEDURE.

PROCEDURE closeStream:
	OUTPUT STREAM oLog CLOSE.
END PROCEDURE.

PROCEDURE writeStream:
	DEF INPUT PARAMETER s AS CHAR NO-UNDO.
	PUT STREAM oLog UNFORMATTED s SKIP.
END PROCEDURE.

PROCEDURE LogEntry:
	DEF INPUT PARAMETER s AS CHAR NO-UNDO.
	IF NOT SequelLog THEN RETURN.
	
	RUN openStream NO-ERROR.
	RUN writeStream(INPUT s) NO-ERROR.
		
	DEF VAR tmpdt AS CHAR NO-UNDO. 
	ASSIGN tmpdt = 'Error Date = ' + string(YEAR(today),"9999") + string(MONTH(today),"99") + string(DAY(today),"99"). 
	RUN writeStream(INPUT tmpdt) NO-ERROR.
	
	RUN closeStream NO-ERROR.
	
END PROCEDURE.



/*******************************************************************************
	DATA TRANSFER
*******************************************************************************/

DEF VAR hBuffer AS HANDLE NO-UNDO.
DEF VAR hQuery AS HANDLE NO-UNDO.
DEF VAR hBuffld AS HANDLE NO-UNDO.


PROCEDURE MergeTable:
	DEFINE INPUT PARAMETER tableName AS CHAR NO-UNDO.
	DEFINE INPUT PARAMETER tableKey AS CHAR NO-UNDO.
	DEFINE INPUT PARAMETER tableData AS HANDLE NO-UNDO.
	DEFINE INPUT PARAMETER deleteOthers AS LOGICAL NO-UNDO.
	
	DEF VAR i AS INT NO-UNDO.
	DEF VAR rownum AS INT NO-UNDO.
	DEF VAR colnames AS CHAR EXTENT NO-UNDO.
	DEF VAR insertStart AS CHAR NO-UNDO.
	DEF VAR sqlText AS CHAR FORMAT "X(80)" NO-UNDO.
	
	IF tableData = ? THEN RETURN.
	
	ASSIGN hBuffer = tableData:DEFAULT-BUFFER-HANDLE.
	CREATE QUERY hQuery.
	hQuery:SET-BUFFERS(hBuffer).
	hQuery:QUERY-PREPARE("FOR EACH " + hBuffer:NAME + " NO-LOCK").
	hQuery:QUERY-OPEN().
	
	
	
	/* first, need to create sql temp table */
	
	/* SequelCon:EXECUTE("CREATE TABLE #" + tableName + " AS (SELECT * FROM " + tableName + " WHERE 1=2)", , ). */
	SequelCon:EXECUTE("DROP TABLE #" + tableName, , ) NO-ERROR.
	SequelCon:EXECUTE("SELECT * INTO #" + tableName + " FROM [" + tableName + "] WHERE 1 = 2", , ).
	
	insertStart = "INSERT INTO #" + tableName + "(".
	REPEAT i = 1 TO hBuffer:NUM-FIELDS:
		IF i NE 1 THEN insertStart = insertStart + ", ".
		
		hBuffld = hBuffer:BUFFER-FIELD(i).
		insertStart = insertStart + "[" + hBuffld:NAME + "]".
	END.
	insertStart = insertStart + ") VALUES ".
	
	REPEAT:
		hQuery:GET-NEXT.
		IF hQuery:QUERY-OFF-END THEN LEAVE.

		IF rownum = 0 OR rownum MOD {&batchcount} = 0 THEN DO:
			/* MESSAGE sqlText. */
			IF rownum > 0 THEN DO:
				SequelCon:EXECUTE(sqlText, , ) NO-ERROR.
				IF ERROR-STATUS:NUM-MESSAGES > 0 THEN DO:
					RUN LogEntry( ERROR-STATUS:GET-MESSAGE(1) + CHR(10) + sqlText ).
				END.
				
				/* RUN LogEntry( sqlText ). */
			END.
			
			
			ASSIGN sqlText = insertStart.
		END.
		
		IF rownum > 0 AND rownum MOD {&batchcount} > 0 THEN ASSIGN sqlText = sqlText + ", (".
		ELSE ASSIGN sqlText = sqlText + "(".
		
		
		REPEAT i = 1 TO hBuffer:NUM-FIELDS:
			hBufFld = hBuffer:BUFFER-FIELD(i).
			
			IF i NE 1 THEN sqlText = sqlText + ", ".

			IF hBuffld:BUFFER-VALUE EQ ? THEN ASSIGN
				sqlText = sqlText + "NULL".
		
			ELSE IF hBuffld:DATA-TYPE = "DATE" OR hBuffld:DATA-TYPE = "CHARACTER" THEN ASSIGN
				sqlText = sqlText + "'" + REPLACE(STRING(hBuffld:BUFFER-VALUE), "'", "''") + "'".
			ELSE IF hBuffld:DATA-TYPE = "LOGICAL" THEN
				DO:
					IF LOGICAL(hBuffld:BUFFER-VALUE) THEN ASSIGN sqlText = sqlText + "1".
					ELSE ASSIGN sqlText = sqlText + "0".
				END.
			ELSE ASSIGN sqlText = sqlText + STRING(hBuffld:BUFFER-VALUE).
			
		END.
		
		ASSIGN	sqlText = sqlText + ")"
			rownum = rownum + 1.
	END.
	
	/* do not run if there are no records */
	IF rownum > 0 THEN DO:
		SequelCon:EXECUTE(sqlText, , ) NO-ERROR.
		IF ERROR-STATUS:NUM-MESSAGES > 0 THEN DO:
			RUN LogEntry( ERROR-STATUS:GET-MESSAGE(1) + CHR(10) + sqlText ).
		END.
		
		/* RUN LogEntry( sqlText ). */
		
		/* merge... */
		sqlText = "MERGE [" + tableName + "] WITH (HOLDLOCK) AS t_old USING (SELECT ".
		REPEAT i = 1 TO hBuffer:NUM-FIELDS:
			ASSIGN hBufFld = hBuffer:BUFFER-FIELD(i).
		
			IF i NE 1 THEN ASSIGN sqlText = sqlText + ", [" + hBuffld:NAME + "]".
			ELSE ASSIGN sqlText = sqlText + "[" + hBuffld:NAME + "]". 
		END.
		
		sqlText = sqlText + " FROM #" + tableName + ") AS t_new ON ".
		
		REPEAT i = 1 TO NUM-ENTRIES(tableKey):
			IF i NE 1 THEN ASSIGN sqlText = sqlText + " AND ".
			
			sqlText = sqlText + "t_new." + ENTRY(i,tableKey) + " = t_old." + ENTRY(i,tableKey).
		END.
		
		sqlText = sqlText + " WHEN MATCHED THEN UPDATE SET ".
		
		REPEAT i = 1 TO hBuffer:NUM-FIELDS:
			ASSIGN hBufFld = hBuffer:BUFFER-FIELD(i).
		
			IF i NE 1 THEN ASSIGN sqlText = sqlText + ", ".
			ASSIGN sqlText = sqlText + "t_old." + hBuffld:NAME + " = t_new." + hBuffld:NAME. 
		END.
		ASSIGN sqlText = sqlText + " WHEN NOT MATCHED THEN INSERT (".
		REPEAT i = 1 TO hBuffer:NUM-FIELDS:
			ASSIGN hBuffld = hBuffer:BUFFER-FIELD(i).
			IF i NE 1 THEN ASSIGN sqlText = sqlText + ", ".
					
			ASSIGN sqlText = sqlText + hBuffld:NAME.
		END.
		ASSIGN sqlText = sqlText + ") VALUES (".
		REPEAT i = 1 TO hBuffer:NUM-FIELDS:
			ASSIGN hBuffld = hBuffer:BUFFER-FIELD(i).
			IF i NE 1 THEN ASSIGN sqlText = sqlText + ", ".
					
			ASSIGN sqlText = sqlText + "t_new." + hBuffld:NAME.
		END.
		ASSIGN sqlText = sqlText + ");".
		
		SequelCon:EXECUTE(sqlText, , ) NO-ERROR.
		IF ERROR-STATUS:NUM-MESSAGES > 0 THEN DO:
			RUN LogEntry( ERROR-STATUS:GET-MESSAGE(1) + CHR(10) + sqlText ).
		END.
	END.
	
	IF deleteOthers THEN DO:
		ASSIGN sqlText = "DELETE [" + tableName + "] FROM [" + tableName + "] LEFT JOIN #" + tableName + " ON ".
	
		REPEAT i = 1 TO NUM-ENTRIES(tableKey):
			IF i NE 1 THEN ASSIGN sqlText = sqlText + " AND ".
			
			sqlText = sqlText + tableName + "." + ENTRY(i,tableKey) + " = #" + tableName + "." + ENTRY(i,tableKey).
		END.
		
		ASSIGN sqlText = sqlText + " WHERE #" + tableName + "." + ENTRY(1,tableKey) + " IS NULL".
		SequelCon:EXECUTE(sqlText, , ) NO-ERROR.
		
		IF ERROR-STATUS:NUM-MESSAGES > 0 THEN DO:
			RUN LogEntry( ERROR-STATUS:GET-MESSAGE(1) + CHR(10) + sqlText ).
		END.
		
		/*SequelCon:EXECUTE("DELETE " + tableName + " FROM " + tableName + " LEFT JOIN #" + tableName + " ON " + tableName + "." + tableKey + " = #" + tableName + "." + tableKey + " WHERE #" + tableName + "." + tableKey + " IS NULL", , ).*/		
	END.
	
	SequelCon:EXECUTE("DROP TABLE #" + tableName, , ) NO-ERROR.

	
END PROCEDURE.




PROCEDURE UpdateTable:
	DEFINE INPUT PARAMETER tableName AS CHAR NO-UNDO.
	DEFINE INPUT PARAMETER tableKey AS CHAR NO-UNDO.
	DEFINE INPUT PARAMETER tableData AS HANDLE NO-UNDO.
	
	DEF VAR i AS INT NO-UNDO.
	DEF VAR rownum AS INT NO-UNDO.
	DEF VAR colnames AS CHAR EXTENT NO-UNDO.
	DEF VAR insertStart AS CHAR NO-UNDO.
	DEF VAR sqlText AS CHAR FORMAT "X(80)" NO-UNDO.
	
	IF tableData = ? THEN RETURN.
	
	ASSIGN hBuffer = tableData:DEFAULT-BUFFER-HANDLE.
	CREATE QUERY hQuery.
	hQuery:SET-BUFFERS(hBuffer).
	hQuery:QUERY-PREPARE("FOR EACH " + hBuffer:NAME + " NO-LOCK").
	hQuery:QUERY-OPEN().


	/* -- Create SQL Temp Table -- */
	SequelCon:EXECUTE("DROP TABLE #" + tableName, , ) NO-ERROR.
	SequelCon:EXECUTE("SELECT * INTO #" + tableName + " FROM [" + tableName + "] WHERE 1 = 2", , ).
	insertStart = "INSERT INTO #" + tableName + "(".
	REPEAT i = 1 TO hBuffer:NUM-FIELDS:
		IF i NE 1 THEN insertStart = insertStart + ", ".
		
		hBuffld = hBuffer:BUFFER-FIELD(i).
		insertStart = insertStart + "[" + hBuffld:NAME + "]".
	END.
	insertStart = insertStart + ") VALUES ".
	
	REPEAT:
		hQuery:GET-NEXT.
		IF hQuery:QUERY-OFF-END THEN LEAVE.
		
		IF rownum = 0 OR rownum MOD 20 = 0 THEN DO:
			/* MESSAGE sqlText. */
			IF rownum > 0 THEN DO:
				SequelCon:EXECUTE(sqlText, , ).
				IF ERROR-STATUS:NUM-MESSAGES > 0 THEN DO:
					RUN LogEntry( ERROR-STATUS:GET-MESSAGE(1) + CHR(10) + sqlText ).
				END.
			END.
			
			ASSIGN sqlText = insertStart.
		END.
		
		IF rownum > 0 AND rownum MOD 20 > 0 THEN ASSIGN sqlText = sqlText + ", (".
		ELSE ASSIGN sqlText = sqlText + "(".
		
		
		REPEAT i = 1 TO hBuffer:NUM-FIELDS:
			hBufFld = hBuffer:BUFFER-FIELD(i).
			
			IF i NE 1 THEN sqlText = sqlText + ", ".

			IF hBuffld:BUFFER-VALUE EQ ? THEN ASSIGN
				sqlText = sqlText + "NULL".
				
			ELSE IF hBuffld:DATA-TYPE = "DATE" OR hBuffld:DATA-TYPE = "CHARACTER" THEN ASSIGN
				sqlText = sqlText + "'" + REPLACE(STRING(hBuffld:BUFFER-VALUE), "'", "''") + "'".
			ELSE IF hBuffld:DATA-TYPE = "LOGICAL" THEN
				DO:
					IF LOGICAL(hBuffld:BUFFER-VALUE) THEN ASSIGN sqlText = sqlText + "1".
					ELSE ASSIGN sqlText = sqlText + "0".
				END.
			ELSE ASSIGN sqlText = sqlText + STRING(hBuffld:BUFFER-VALUE).
			
		END.
		
		ASSIGN	sqlText = sqlText + ")"
			rownum = rownum + 1.
	END.
	
	IF rownum = 0 THEN LEAVE. /* do not continue if there are no records */
	
	SequelCon:EXECUTE(sqlText, , ) NO-ERROR.
	IF ERROR-STATUS:NUM-MESSAGES > 0 THEN DO:
		RUN LogEntry( ERROR-STATUS:GET-MESSAGE(1) + CHR(10) + sqlText ).
	END.

	/* -- Update Table -- */
	ASSIGN sqlText = "UPDATE t_old".
	
	ASSIGN sqlText = sqlText + " SET ".

	REPEAT i = 1 TO hBuffer:NUM-FIELDS:
		ASSIGN hBufFld = hBuffer:BUFFER-FIELD(i).
	
		IF i NE 1 THEN ASSIGN sqlText = sqlText + ", ".
		ASSIGN sqlText = sqlText + "t_old." + hBuffld:NAME + " = t_new." + hBuffld:NAME. 
	END.

	ASSIGN sqlText = sqlText + " FROM [" + tableName + "] AS t_old, #" + tableName + " AS t_new".

	ASSIGN sqlText = sqlText + " WHERE ".
	
	REPEAT i = 1 TO NUM-ENTRIES(tableKey):
		IF i NE 1 THEN ASSIGN sqlText = sqlText + " AND ".
		
		sqlText = sqlText + "t_new." + ENTRY(i,tableKey) + " = t_old." + ENTRY(i,tableKey).
	END.	

	ASSIGN sqlText = sqlText + ";".
	
	SequelCon:EXECUTE(sqlText, , ) NO-ERROR.
	IF ERROR-STATUS:NUM-MESSAGES > 0 THEN DO:
		RUN LogEntry( ERROR-STATUS:GET-MESSAGE(1) + CHR(10) + sqlText ).
	END.
	
END PROCEDURE.