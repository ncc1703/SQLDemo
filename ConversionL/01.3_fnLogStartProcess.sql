create or replace
function fnLogStartNJSProcess
  (
    pISOasOfDate    nvarchar2,
    processID       NUMBER,
    PAFFECTEDRECORDS number,
    PTARGETTABLE     nvarchar2,
    POPERATION       nvarchar2,
    PCUSTOMMESSAGE nvarchar2,
    EXECUTIONOVERRIDEFAILED nvarchar2,
    EXECUTIONOVERRIDECOMPLETED nvarchar2)
    return nvarchar2
as
  LPARTITIONID NJPARTITION.id%type;
  RETURNCODE nvarchar2(40);
  CNTPREVRECORDS NUMBER;
  LCUSTOMMESSAGE nvarchar2(2048);
  
  owa_owner_name    VARCHAR2 (100);
  owa_caller_name   VARCHAR2 (100);
  owa_line_number   NUMBER;
  owa_CALLER_TYPE   VARCHAR2 (100);

begin
  --EXECUTIONOVERRIDEFAILED, EXECUTIONOVERRIDECOMPLETED = 'Y', 'N'
  -- Y will attepmt to re-run for statuses D and F
   OWA_UTIL.WHO_CALLED_ME (owa_owner_name,owa_caller_name,owa_line_number,owa_caller_type);
  -- Never Override R status
  -- Never pass to override initila step , only substeps 
  -- Initial step should be controlled by orchestration
  -- PASS PAFFECTEDRECORDS (-1), if unknown
  
  
  --step 1 - detect partionID
   SELECT id
     INTO LPARTITIONID
     FROM njpartition
    where CLASSCODE = 'S';
  
    -- N - First Start
    -- R - currently running, never stopped
    -- D - completed before - don't executed again
    -- F - falied before
       -- using pseudogrouping to avoid No data exception
   
        select COUNT(1) 
        , NVL(max(NJEP.STATUSCODE),'N') 
        into CNTPREVRECORDS
        ,RETURNCODE
        from NJEXTERNALPROCESS NJEP 
        where NJEP.name = PISOASOFDATE
        and NJEP.STEP = TO_CHAR(PROCESSID)
        and NJEP.PARTITIONID = LPARTITIONID;
   
    LCUSTOMMESSAGE:='Line number: ' || $$plsql_line || ' Unit: ' || $$plsql_unit||'<-->Called by:'||owa_caller_type||'.'||owa_owner_name||'.'||owa_caller_name||'-LINE-'||owa_line_number||' MSG-'||PCUSTOMMESSAGE;
    
  -- Step 2 log custom message show err
  -- R status will be replaced later by a correct process END statement to D
  -- Or to F in case of failure
  if RETURNCODE = 'N' then
  -- Inserting New recors, first count
   INSERT
     INTO njExternalProcess
    (
      NAME ,
      STEP     ,
      STATUSCODE    ,
      MESSAGE       ,
      CREATETIME    ,
      PROCESSEDCOUNT,
      TOTALCOUNT    ,
      attempt,
      OPERATION,
      TARGETTABLE,
      PARTITIONID
    )
    VALUES
    (
      PISOASOFDATE                   ,
      TO_CHAR(processID) ,
      'R'                     ,
      to_char(sys_extract_utc(systimestamp),'YYYY-MM-DD HH24:MI:SS.FF')||'--'||LCUSTOMMESSAGE,
      sys_extract_utc(systimestamp) ,
      0,
      PAFFECTEDRECORDS ,
      1,
      POPERATION,
      PTARGETTABLE,
      LPARTITIONID
    );
    commit;
    end if;


   -- Rerun  if directed
   
  if ((EXECUTIONOVERRIDEFAILED = 'Y' and  RETURNCODE = 'F')
      or ( EXECUTIONOVERRIDECOMPLETED='Y' and  RETURNCODE = 'D')) then
 
    update NJEXTERNALPROCESS NJEP
    set (NJEP.STATUSCODE,
    NJEP.MESSAGE,
    NJEP.ATTEMPT,
    NJEP.OLDMESSAGE,
    TOTALCOUNT,
    OLDTOTALCOUNT) = 
    (select
    'R',
    TO_CHAR(SYS_EXTRACT_UTC(systimestamp),'YYYY-MM-DD HH24:MI:SS.FF')||'--'||LCUSTOMMESSAGE,
    NJEPOLD.ATTEMPT+1,
    NJEPOLD.MESSAGE ||' -- '|| substr(NJEP.OLDMESSAGE,0,60),
    PAFFECTEDRECORDS,
    NJEPOLD.TOTALCOUNT
    from NJEXTERNALPROCESS NJEPOLD
     where NJEPOLD.name = PISOASOFDATE
    and NJEPOLD.STEP = TO_CHAR(processID))
    where NJEP.name = PISOASOFDATE
    and NJEP.STEP = TO_CHAR(PROCESSID);
    
    -- set return code to N ( New)
    RETURNCODE:='N';
  commit;
  end if;
    
    return (RETURNCODE);
END fnLogStartNJSProcess;