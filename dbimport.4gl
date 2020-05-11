IMPORT os
IMPORT util

-- Do a commit every m_cfg.commit rows
DEFINE m_dbsource STRING
DEFINE m_sql STRING
DEFINE m_creTab BOOLEAN = FALSE
DEFINE m_creIdx BOOLEAN = FALSE
DEFINE m_tabs DYNAMIC ARRAY OF RECORD
	tabName STRING,
	cols SMALLINT,
	dataFile STRING,
	rowsize INTEGER,
	rows INTEGER,
	act_rows INTEGER,
	size DECIMAL(20,2)
END RECORD
DEFINE m_tabName STRING
DEFINE m_idxName STRING
DEFINE m_file STRING
DEFINE m_rows, m_cols, m_size INTEGER
DEFINE m_logFile STRING
DEFINE m_idxs DYNAMIC ARRAY OF STRING

DEFINE m_cfg RECORD
	targetdbn STRING,
	targettyp CHAR(3),
	commit SMALLINT,
	drop BOOLEAN,
	create BOOLEAN,
	load BOOLEAN,
	dropIdx BOOLEAN,
	indexes BOOLEAN,
	updstats BOOLEAN
END RECORD
DEFINE l_dbdriver STRING
DEFINE m_start, m_end DATETIME YEAR TO SECOND

MAIN
	DEFINE x SMALLINT
	DEFINE l_dbdriver STRING
	DEFINE l_imported INTEGER = 0

	LET m_dbsource = ARG_VAL(1)

	IF NOT os.path.exists(m_dbsource || ".exp") THEN
		DISPLAY "No " || m_dbsource || ".exp here!"
		EXIT PROGRAM
	END IF

	LET m_logFile = m_dbsource||"_"||(util.Datetime.format( CURRENT, "%Y%m%d%H%M" ))||".log"

	LET m_cfg.targetdbn = m_dbsource
	LET m_cfg.targettyp = "ifx"
	LET m_cfg.commit = 1000
	LET m_cfg.drop = FALSE
	LET m_cfg.create = FALSE
	LET m_cfg.load = FALSE
	LET m_cfg.dropIdx = FALSE
	LET m_cfg.indexes = FALSE
	LET m_cfg.updstats = FALSE
	CALL loadCfg()

	LET l_dbdriver = fgl_getResource("dbi.default.driver")
	LET l_dbdriver = l_dbdriver.subString(4,6)
	
	IF l_dbdriver != m_cfg.targettyp THEN
		DISPLAY SFMT("DB Driver %1 not equal to targettyp %2 !", l_dbdriver, m_cfg.targettyp)
		EXIT PROGRAM
	END IF

	CALL disp(SFMT("DB Driver: %1",l_dbdriver))
	IF m_cfg.targetTyp = "sqt" THEN
		IF NOT os.path.exists( m_cfg.targetdbn ) THEN
			CALL disp(SFMT("Create Database: %1",m_cfg.targetdbn))
			TRY
				CREATE DATABASE m_cfg.targetdbn
			CATCH
				CALL disp(SQLERRMESSAGE)
				EXIT PROGRAM
			END TRY
		END IF
	END IF

	TRY
		CALL disp(SFMT("Database: %1",m_cfg.targetdbn))
		DATABASE m_cfg.targetdbn
	CATCH
		CALL disp(SFMT("Connect failed:%1: %2",m_cfg.targettyp,SQLERRMESSAGE))
		EXIT PROGRAM
	END TRY

	LET m_start = CURRENT
	CALL disp("Commit: "||m_cfg.commit)
	CALL disp("Target DB Name: "||m_cfg.targetdbn)
	CALL disp("Target DB Type: "||m_cfg.targettyp)
	CALL disp("Drop Tables: "||IIF(m_cfg.drop,"Yes","No"))
	CALL disp("Create Tables: "||IIF(m_cfg.create,"Yes","No"))
	CALL disp("Load data: "||IIF(m_cfg.load,"Yes","No"))
	CALL disp("Drop Indexes: "||IIF(m_cfg.dropIdx,"Yes","No"))
	CALL disp("Create Indexes: "||IIF(m_cfg.indexes,"Yes","No"))

	CALL procSQL(SFMT("%1.exp/%2.sql", m_dbsource, m_dbsource))

	CALL m_tabs.sort("size", FALSE)
	FOR x = 1 TO m_tabs.getLength()
		CALL disp(
				SFMT("%1 of %2 - %3 Columns: %4 File: %5 Rows: %6 Act Rows: %7 RowSize: %8 DataSize: %9",
						x, m_tabs.getLength(), m_tabs[x].tabName, m_tabs[x].cols, m_tabs[x].dataFile, 
						(m_tabs[x].rows USING "<<<,<<<,<<&"), 
						(m_tabs[x].act_rows USING "<<<,<<<,<<&"), 
						(m_tabs[x].rowsize USING "<<,<<<,<<<,<<&"),
						(m_tabs[x].size USING "<<<,<<<,<<&.&&M")))
		IF m_tabs[x].act_rows > 0 THEN
			CASE m_cfg.targettyp
				WHEN "ifx" CALL loadIFX(x)
				WHEN "sqt" CALL loadSQT(x)
				WHEN "pgs" CALL loadPGS(x)
			END CASE
			IF m_cfg.load THEN
				LET l_imported = l_imported + m_tabs[x].size
			END IF
		END IF
		IF m_cfg.targettyp = "ifx" AND l_imported > 200 THEN -- force a checkpoint every 200MB to try and stop DB from stalling 
			CALL disp("Doing check point")
			RUN "onmode -c"
			LET l_imported = 0
		END IF
	END FOR

	IF m_cfg.indexes AND m_idxs.getLength() > 0 THEN
		FOR x = 1 TO m_idxs.getLength()
			LET m_sql = m_idxs[x]
			CALL doSQL( TRUE, 0 )
		END FOR
	END IF

	IF m_cfg.updstats THEN
		CALL updateStats()
	END IF

	LET m_end = CURRENT
	CALL disp(SFMT("Finished - %1", m_end - m_start))
END MAIN
--------------------------------------------------------------------------------
FUNCTION disp(l_line STRING)
	DEFINE l_ts DATETIME YEAR TO SECOND
	DEFINE l_log STRING
	DEFINE c base.Channel
	LET l_ts = CURRENT
	LET l_log = l_ts, ") ", l_line
	DISPLAY l_log
	LET c = base.Channel.create()
	CALL c.openFile(m_logFile, "a+")
	CALL c.writeLine( l_log )
	CALL c.close()
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
		LET m_tabs[m_tabs.getLength()].rowsize = m_size
		LET m_tabs[m_tabs.getLength()].cols = m_cols
	END IF

-- Get the unload file name and the number of rows.
	IF l_line MATCHES "{ unload file name = *" THEN
		LET x = l_line.getIndexOf(" ", 22)
		LET m_file = l_line.subString(22, x - 1)
		LET x = l_line.getIndexOf("=", x)
		LET y = l_line.getIndexOf(" ", x + 2)
		LET m_rows = l_line.subString(x + 2, y - 1)
		LET m_tabs[m_tabs.getLength()].dataFile = SFMT("%1.exp/%2", m_dbsource, m_file)
		LET m_tabs[m_tabs.getLength()].rows = m_rows
		LET m_rows = countRows( m_tabs[m_tabs.getLength()].dataFile )
		LET m_tabs[m_tabs.getLength()].act_rows = m_rows
		LET m_tabs[m_tabs.getLength()].size = ((m_size * m_rows) / 1024) / 1000 -- calc MB
	END IF

	IF l_line.getCharAt(1) = "{" THEN
		RETURN
	END IF

-- Setup the m_tabs array for the table - do the DROP
	IF l_line.subString(1, 12) = "create table" THEN
		LET m_sql = SFMT("drop table %1;", m_tabName)
		CALL disp(m_sql)
		CALL doSQL(m_cfg.drop, 0)
		LET m_creTab = TRUE
		LET m_creIdx = FALSE
		LET m_sql = "CREATE TABLE ",m_tabName
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
		CALL doSQL(m_cfg.dropIdx, 0)
		LET m_sql = stripOwner(l_line)
		LET m_creTab = FALSE
		LET m_creIdx = TRUE
		RETURN
	END IF

	LET m_sql = m_sql.append(" "||l_line)

	LET x = l_line.getIndexOf(";", 1)
	IF x > 0 THEN
		IF m_creTab THEN
			CALL doSQL(m_cfg.create, 0)
			LET m_creTab = FALSE
		END IF
		IF m_creIdx THEN
			CALL removeBtree()
			LET m_idxs[ m_idxs.getLength() + 1 ] = m_sql
			LET m_creIdx = FALSE
		END IF
	END IF
END FUNCTION
--------------------------------------------------------------------------------
FUNCTION removeBtree()
	DEFINE x SMALLINT
	LET x = m_sql.getIndexOf("using btree",1) 
	IF x > 0 THEN
		LET m_sql = m_sql.subString(1,x-1)||";"
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
FUNCTION doSQL(doIt BOOLEAN, l_cnt SMALLINT)
	DEFINE l_start DATETIME YEAR TO SECOND
	DEFINE l_line STRING
	IF doIt THEN
		LET l_start = CURRENT
		IF l_cnt > 0 THEN
			LET l_line = SFMT("%1 of %2 - Execute: %3",l_cnt, m_tabs.getLength(), m_sql)
		ELSE
			LET l_line = SFMT("Execute: %1",m_sql)
		END IF
		CALL disp(l_line)
		TRY
			EXECUTE IMMEDIATE m_sql
			CALL disp(SFMT("Okay: %1", (CURRENT - l_start)))
		CATCH
			CALL disp(SFMT("Failed: %1",SQLERRMESSAGE))
		END TRY
	ELSE
		CALL disp("Skipping: " || m_sql)
	END IF
END FUNCTION
--------------------------------------------------------------------------------
FUNCTION chkData(x SMALLINT)
	DEFINE l_line STRING
	DEFINE i INTEGER
	LET l_line = "SELECT COUNT(*) FROM " || m_tabs[x].tabName
	PREPARE pre FROM l_line
	EXECUTE pre INTO i
	RETURN i
END FUNCTION
--------------------------------------------------------------------------------
FUNCTION updateStats()
	DEFINE x SMALLINT
	FOR x = 1 TO m_tabs.getLength()
		CASE m_cfg.targettyp
			WHEN "ifx" LET m_sql = "UPDATE STATISTICS MEDIUM FOR TABLE "||m_tabs[x].tabName
			OTHERWISE
				LET m_sql = "ANALYZE "||m_tabs[x].tabName
		END CASE
		CALL doSQL(TRUE,x)
	END FOR
END FUNCTION
--------------------------------------------------------------------------------
FUNCTION loadPGS(x SMALLINT)
	DEFINE l_file, l_outFile, l_line, l_cmd, l_cmd2 STRING
	DEFINE i INTEGER
	DEFINE c base.Channel
	LET i = chkData(x)
	IF i > 0 THEN
		CALL disp(m_tabs[x].tabName || " - Already has data - " || i)
		RETURN
	END IF
	LET l_file = SFMT("%1.exp/%2.load", m_dbsource, m_tabs[x].tabName)
	LET l_outFile = SFMT("%1.exp/%2.out", m_dbsource, m_tabs[x].tabName)
	LET c = base.Channel.create()
	CALL c.openFile(l_file, "w")
	LET l_line = SFMT("COPY %1\nFROM '%2' DELIMITER '|'", m_tabs[x].tabName, os.path.join(os.path.pwd(),m_tabs[x].dataFile))
	CALL c.writeLine(l_line)
	CALL c.close()
	LET l_cmd = SFMT("psql -d %1 < %2 > %3 2>&1", m_cfg.targetdbn, l_file, l_outFile)
	CALL disp(l_cmd)
	IF m_cfg.load THEN
		-- have to remove the last | for the load to work.
		LET l_cmd2 = SFMT("sed -i 's/|$//g' %1", m_tabs[x].dataFile)
		RUN l_cmd2

		RUN l_cmd
	END IF
	LET i = chkData(x)
	CALL disp(SFMT("%1 has %2 rows.",m_tabs[x].tabName, i))
END FUNCTION
--------------------------------------------------------------------------------
FUNCTION loadSQT(x SMALLINT)
	DEFINE l_file, l_outFile, l_line, l_cmd, l_cmd2 STRING
	DEFINE i INTEGER
	DEFINE c base.Channel
	LET i = chkData(x)
	IF i > 0 THEN
		CALL disp(m_tabs[x].tabName || " - Already has data - " || i)
		RETURN
	END IF
	LET l_file = SFMT("%1.exp/%2.load", m_dbsource, m_tabs[x].tabName)
	LET l_outFile = SFMT("%1.exp/%2.out", m_dbsource, m_tabs[x].tabName)
	LET c = base.Channel.create()
	CALL c.openFile(l_file, "w")
	LET l_line = SFMT(".separator '|'\n.import '%1' %2", m_tabs[x].dataFile, m_tabs[x].tabName)
	CALL c.writeLine(l_line)
	CALL c.close()
	LET l_cmd = SFMT("cat %1 | sqlite3 %2 > %3 2>&1", l_file, m_cfg.targetdbn, l_outFile)
	IF m_cfg.load THEN
		-- remove the last | for the load not to produce an error line for every row
		LET l_cmd2 = SFMT("sed -i 's/|$//g' %1", m_tabs[x].dataFile)
		RUN l_cmd2

		RUN l_cmd
	END IF
	LET i = chkData(x)
	CALL disp(SFMT("%1 has %2 rows.",m_tabs[x].tabName, i))
END FUNCTION
--------------------------------------------------------------------------------
FUNCTION loadIFX(x SMALLINT)
	DEFINE l_cmd STRING
	DEFINE l_file, l_logFile, l_outFile, l_line STRING
	DEFINE c base.Channel
	DEFINE i INTEGER
	LET i = chkData(x)
	IF i > 0 THEN
		CALL disp(m_tabs[x].tabName || " - Already has data - " || i)
		RETURN
	END IF
	LET l_file = SFMT("%1.exp/%2.load", m_dbsource, m_tabs[x].tabName)
	LET l_logFile = SFMT("%1.exp/%2.log", m_dbsource, m_tabs[x].tabName)
	LET l_outFile = SFMT("%1.exp/%2.out", m_dbsource, m_tabs[x].tabName)
	LET c = base.Channel.create()
	CALL c.openFile(l_file, "w")
	LET l_line =
			SFMT("FILE \"%1\" DELIMITER '|' %2;\n\tINSERT INTO %3;", m_tabs[x].dataFile, m_tabs[x].cols, m_tabs[x].tabName)
	CALL c.writeLine(l_line)
	CALL c.close()
	LET l_cmd = SFMT("dbload -d %1 -k -c %2 -n %3 -l %4 > %5", m_dbsource, l_file, m_cfg.commit, l_logFile, l_outFile)
	IF m_cfg.load THEN
--		CALL disp(l_cmd)
		RUN l_cmd
		LET l_cmd = "tail -1 ", l_outFile
		RUN l_cmd
	END IF
-- we tail the output file to show the number of rows loaded.
--	LET i = chkData(x)
--	CALL disp(SFMT("%1 has %2 rows.",m_tabs[x].tabName, i))
END FUNCTION
--------------------------------------------------------------------------------
FUNCTION countRows( l_file STRING ) RETURNS INT
	DEFINE c base.channel
	DEFINE l_line STRING
	DEFINE l_ret INT
	LET c = base.channel.create()
	CALL c.openPipe(SFMT("cat %1 | wc -l", l_file),"r")
	LET l_line = c.readLine()	
	CALL c.close()
	LET l_ret = l_line
	RETURN l_ret
END FUNCTION
--------------------------------------------------------------------------------
FUNCTION loadCfg()
	DEFINE l_file STRING
	DEFINE l_data TEXT
	LET l_file = m_dbsource||".cfg"
	LOCATE l_data IN FILE l_file
	IF NOT os.path.exists(l_file) THEN
		CALL disp(SFMT("Creating %1 ...", l_file))
		LET l_data = util.JSON.stringify( m_cfg )
		EXIT PROGRAM
	ELSE
		CALL disp(SFMT("Loading %1 ...", l_file))
	END IF
	CALL util.JSON.parse( l_data, m_cfg )
END FUNCTION
--------------------------------------------------------------------------------
