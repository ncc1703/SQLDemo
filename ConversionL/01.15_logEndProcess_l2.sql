create or replace PROCEDURE logEndNJSProcess_Log2
  (
    pISOasOfDate    VARCHAR,
    processID       NUMBER,
    AFFECTEDRECORDS NUMBER,
    CUSTOMMESSAGE NVARCHAR2,
    PTARGETTABLE     NVARCHAR2:='NO',
    POPERATION       NVARCHAR2:='NO'
  )
AS
  LAST_SQL_ID VARCHAR2(13) ;
  LAST_SQL_TEXT njexternalprocess.SQL_TEXT%type ;
  L NJEXTERNALPROCESS.XPLAIN_PLAN%TYPE;
  BEGIN
     LAST_SQL_TEXT := '';
     LAST_SQL_ID := '';
  -- Getting last SQL ID, based on executed params
  -- Full operations name and target table must be provided
  IF (LENGTH(POPERATION)>3 AND LENGTH(PTARGETTABLE)>3) THEN
  
    select NVL(V1.SQL_ID,'')
    ,NVL(V1.SQL_TEXT,'')
    INTO LAST_SQL_ID, LAST_SQL_TEXT
    FROM V$SQL V1 
    WHERE V1.SQL_TEXT LIKE POPERATION||'%'||PTARGETTABLE||'%'
    AND V1.FIRST_LOAD_TIME=
    (SELECT MAX(V2.FIRST_LOAD_TIME) 
    FROM V$SQL V2
     WHERE UPPER(V2.SQL_TEXT) LIKE POPERATION||'%'||PTARGETTABLE||'%');
     
     -- second stage - process L object , if query found
     if (length(LAST_SQL_TEXT) > 0 ) then 
     
        for X in ( select PLAN_TABLE_OUTPUT from  table(DBMS_XPLAN.DISPLAY_CURSOR(LAST_SQL_ID,0)))
        -- adding each line with custom line separator
        LOOP
          L:=L||to_nchar(X.PLAN_TABLE_OUTPUT)||'><';
        END LOOP;
      end if;
  ELSE
     LAST_SQL_TEXT := '';
     LAST_SQL_ID := '';
  END IF;
  -- Step 1 Update status to complete   
   UPDATE  njExternalprocess P
   set P.STATUSCODE ='D', P.PROCESSEDCOUNT = AFFECTEDRECORDS
   ,P.MESSAGE = TO_CHAR(SYS_EXTRACT_UTC(SYSTIMESTAMP),'YYYY-MM-DD HH24:MI:SS.FF')||'->'||CUSTOMMESSAGE||'->'||P.MESSAGE 
   , P.SQL_TEXT = LAST_SQL_TEXT
   , XPLAIN_PLAN = L
   where processID = P.STEP
   and P.name = pISOasOfDate
   and statusCode in ('R', 'F');
END logEndNJSProcess_Log2;
