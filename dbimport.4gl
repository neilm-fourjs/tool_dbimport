IMPORT os

-- Do a commit every C_COMMIT rows
CONSTANT C_COMMIT = 1000

DEFINE m_dbn STRING
DEFINE m_sql STRING
DEFINE m_creTab BOOLEAN = FALSE
DEFINE m_creIdx BOOLEAN = FALSE
DEFINE m_tabs DYNAMIC ARRAY OF RECORD
	tabName STRING,
	cols SMALLINT,
	dataFile STRING,
	rowsize INTEGER,
	rows INTEGER,
	size INTEGER
END RECORD
DEFINE m_tabName STRING
DEFINE m_idxName STRING
DEFINE m_file STRING
DEFINE m_rows, m_cols, m_size INTEGER

DEFINE m_drop BOOLEAN = FALSE
DEFINE m_create BOOLEAN = FALSE
DEFINE m_load BOOLEAN = TRUE
DEFINE m_dropIdx BOOLEAN = FALSE
DEFINE m_indexes BOOLEAN = FALSE

MAIN
	DEFINE x SMALLINT

	LET m_dbn = ARG_VAL(1)

	IF NOT os.path.exists(m_dbn || ".exp") THEN
		DISPLAY "No " || m_dbn || ".exp here!"
		EXIT PROGRAM
	END IF

	TRY
		CALL disp(SFMT("Database: %1",m_dbn))
		DATABASE m_dbn
	CATCH
		CALL disp(SQLERRMESSAGE)
		EXIT PROGRAM
	END TRY

	CALL procSQL(SFMT("%1.exp/%2.sql", m_dbn, m_dbn))

	CALL m_tabs.sort("size", FALSE)
	FOR x = 1 TO m_tabs.getLength()
		CALL disp(
				SFMT("%1 of %2 - %3 Columns: %4 File: %5 Rows: %6 RowSize: %7 DataSize: %8",
						x, m_tabs.getLength(), m_tabs[x].tabName, m_tabs[x].cols, m_tabs[x].dataFile, 
						(m_tabs[x].rows USING "###,###,###"), 
						(m_tabs[x].rowsize USING "##,###,###,###"),
						(m_tabs[x].size USING "###,###,###M")))
		IF m_tabs[x].rows > 0 THEN
			CALL load(x)
		END IF
	END FOR

END MAIN
--------------------------------------------------------------------------------
FUNCTION disp(l_line STRING)
	DISPLAY CURRENT, ":", l_line
END FUNCTION
--------------------------------------------------------------------------------
FUNCTION procSQL(l_file STRING)
	DEFINE c base.Channel
	LET c = base.Channel.create()
	CALL c.openFile(l_file, "r")
	WHILE NOT c.isEof()
		CALL procSQLLine(c.readLine().trim())
	END WHILE
	CALL c.close()
END FUNCTION
--------------------------------------------------------------------------------
FUNCTION procSQLLine(l_line STRING)
	DEFINE x, y SMALLINT

	IF l_line.getLength() < 1 THEN
		RETURN
	END IF
	IF l_line MATCHES "revoke all *" THEN
		RETURN
	END IF
	IF l_line MATCHES "grant *" THEN
		RETURN
	END IF
	IF l_line MATCHES "update statistics *" THEN
		RETURN
	END IF

-- 1234567890123456789012
-- { TABLE "neilm".testtab1 row size = 1156 number of columns = 37 index size = 54 }
-- Get the table name without the 'owner' and get the number of columns
	IF l_line MATCHES "{ TABLE *" THEN
		IF l_line.getCharAt(9) = "\"" THEN
			LET x = l_line.getIndexOf(".", 10)
			LET y = l_line.getIndexOf(" ", x + 1)
			LET m_tabName = l_line.subString(x + 1, y - 1)
		ELSE
			LET y = l_line.getIndexOf(" ", 9)
			LET m_tabName = l_line.subString(9, y - 1)
		END IF
		LET x = l_line.getIndexOf("=", 10)
		LET y = l_line.getIndexOf(" ", x + 2)
		LET m_size = l_line.subString(x + 2, y - 1)

		LET x = l_line.getIndexOf("=", x + 1)
		LET y = l_line.getIndexOf(" ", x + 2)
		LET m_cols = l_line.subString(x + 2, y - 1)

		LET m_tabs[m_tabs.getLength() + 1].tabName = m_tabName
		LET m_tabs[m_tabs.getLength()].cols = m_cols
		LET m_tabs[m_tabs.getLength()].rowsize = m_size
	END IF

-- Get the unload file name and the number of rows.
	IF l_line MATCHES "{ unload file name = *" THEN
		LET x = l_line.getIndexOf(" ", 22)
		LET m_file = l_line.subString(22, x - 1)
		LET x = l_line.getIndexOf("=", x)
		LET y = l_line.getIndexOf(" ", x + 2)
		LET m_rows = l_line.subString(x + 2, y - 1)
		LET m_tabs[m_tabs.getLength()].rows = m_rows
		LET m_tabs[m_tabs.getLength()].size = ((m_size * m_rows) / 1024) / 1000 -- calc MB
		LET m_tabs[m_tabs.getLength()].dataFile = SFMT("%1.exp/%2", m_dbn, m_file)
	END IF

	IF l_line.getCharAt(1) = "{" THEN
		RETURN
	END IF

-- Setup the m_tabs array for the table - do the DROP
	IF l_line.subString(1, 12) = "create table" THEN
		LET m_sql = SFMT("drop table %1;", m_tabName)
		CALL disp(m_sql)
		CALL doSQL(m_drop)
		LET m_creTab = TRUE
		LET m_creIdx = FALSE
		LET m_sql = l_line
		RETURN
	END IF

	IF l_line MATCHES "create*index*" THEN
		LET x = l_line.getIndexOf("\"",1)
		IF x > 1 THEN
			LET x = l_line.getIndexOf(".",x)
			LET y = l_line.getIndexOf(" ",x)
			LET m_idxName = l_line.subString(x+1,y-1)
		END IF
		LET m_sql = SFMT("drop index %1;", m_idxName)
		CALL disp(m_sql)
		CALL doSQL(m_dropIdx)
		LET m_sql = stripOwner(l_line)
		LET m_creTab = FALSE
		LET m_creIdx = TRUE
		RETURN
	END IF

	LET m_sql = m_sql.append(l_line)

	LET x = l_line.getIndexOf(";", 1)
	IF x > 0 THEN
		IF m_creTab THEN
			CALL doSQL(m_create)
			LET m_creTab = FALSE
		END IF
		IF m_creIdx THEN
			CALL doSQL(m_indexes)
			LET m_creIdx = FALSE
		END IF
	END IF
END FUNCTION
--------------------------------------------------------------------------------
-- String the owner from the idx name and table name in a create index statement
-- create index "fred".idx1 on "fred".mytab (key);
FUNCTION stripOwner(l_line STRING)
	DEFINE l_newLine STRING
	DEFINE x,y SMALLINT
	LET x = l_line.getIndexOf("\"",1)
	LET l_newLine = l_line.subString(1,x-1) -- up to idx name
	LET x = l_line.getIndexOf(".",x)
	LET y = l_line.getIndexOf(" ",x)
	LET l_newLine = l_newLine.append( l_line.subString(x+1, y-1) )
	LET x = l_line.getIndexOf(".",y)
	LET l_newLine = l_newLine.append( " ON "||l_line.subString(x+1, l_line.getLength()) ) -- the rest
	RETURN l_newLine
END FUNCTION
--------------------------------------------------------------------------------
FUNCTION doSQL(doIt BOOLEAN)
	IF doIt THEN
		CALL disp("Execute:" || m_sql)
		TRY
			EXECUTE IMMEDIATE m_sql
			CALL disp("Okay")
		CATCH
			CALL disp(SQLERRMESSAGE)
		END TRY
		CALL disp("")
	ELSE
		CALL disp("Skipping: " || m_sql)
	END IF
END FUNCTION
--------------------------------------------------------------------------------
FUNCTION load(x SMALLINT)
	DEFINE l_cmd STRING
	DEFINE l_file, l_logFile, l_outFile, l_line STRING
	DEFINE c base.Channel
	DEFINE i INTEGER
	LET l_line = "SELECT COUNT(*) FROM " || m_tabs[x].tabName
	PREPARE pre FROM l_line
	EXECUTE pre INTO i
	IF i > 0 THEN
		CALL disp(m_tabs[x].tabName || " - Already has data - " || i)
		RETURN
	END IF
	LET l_file = SFMT("%1.exp/%2.load", m_dbn, m_tabs[x].tabName)
	LET l_logFile = SFMT("%1.exp/%2.log", m_dbn, m_tabs[x].tabName)
	LET l_outFile = SFMT("%1.exp/%2.out", m_dbn, m_tabs[x].tabName)
	LET l_cmd = SFMT("dbload -d %1 -k -c %2 -n %3 -l %4 > %5", m_dbn, l_file, C_COMMIT, l_logFile, l_outFile)
	LET c = base.Channel.create()
	CALL c.openFile(l_file, "w")
	LET l_line =
			SFMT("FILE \"%1\" DELIMITER '|' %2;\n\tINSERT INTO %3;", m_tabs[x].dataFile, m_tabs[x].cols, m_tabs[x].tabName)
	CALL c.writeLine(l_line)
	CALL c.close()
	IF m_load THEN
		CALL disp(l_cmd)
		RUN l_cmd
		LET l_cmd = "tail -1 ", l_outFile
		RUN l_cmd
		DISPLAY ""
	END IF
END FUNCTION
--------------------------------------------------------------------------------
