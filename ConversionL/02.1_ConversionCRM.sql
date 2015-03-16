CREATE OR REPLACE
PACKAGE BODY njsconversioncrm
AS
PROCEDURE conv_contact
  (
    pisoasofdate VARCHAR,
    step         NUMBER    :=257,
    dataSource   VARCHAR   :=' ')
                           IS
  u_rows_processed INTEGER := 0;
  u_rows_total     INTEGER := 0;
  i_rows_processed INTEGER := 0;
  i_rows_total     INTEGER := 0;
  e_rows_processed INTEGER := 0;
  nexjsauserid njuser.loginname%type;
  viewprincipalid njentity.id%type;
  lpartitionid njpartition.id%type;
  nexjsaentityid njentity.id%type;
  contacttypeid njentitytype.id%type;
  companytypeid njentitytype.id%type;
  pTimestamp TIMESTAMP(3);
  
  cursor cur is 
  SELECT njsm.rowid,
  njse.updatedDate
  FROM S_njsentity njse
  INNER join S_njsentitymapper njsm
  on njse.sourceid = njsm.sourceid
  AND njse.externalid   = njsm.externalid
  AND njse.isoasofdate  = njsm.isoasofdate
  AND njsm.isoasofdate = pisoasofdate
  inner join njentity njentity
  on njsm.njid = njentity.id
  inner join njentityxA01 njentityxA01
  on njentity.id = njentityxA01.id
  where
  njse.sin <> njentity.ssn
  and njse.dateOfBirth <> njentity.birthTime
  and njse.gender <> njentity.gendercode
  and njse.isClient <> njentity.isclient
  and njse.isProspect <> njentity.isprospect
  and njse.isFormerClient <> njentity.isformerclient
  and njse.isOther <> njentity.isother
  and njse.active <> njentityxA01.activeStatus
  and njse.title <> njentity.title
  and njse.lastName <> njentity.lastname
  and njse.firstName <> njentity.firstname
  and njse.initials <> njentity.initials
  and njse.affix <> njentity.affix
  and njse.dealerCode <> njsm.dealerCode
  and njse.repCode <> njsm.repCode
  and njse.cif <> njentityxA01.cif
  and njse.isProfessional <> njentityxA01.isProfessional;  
BEGIN
  --Step 1.1 assign global properties variables for the major insert
   SELECT id
     INTO nexjsauserid
     FROM njuser
    WHERE loginname LIKE TO_CHAR('nexjsa');
   SELECT id INTO viewprincipalid FROM njprincipal WHERE classcode = 'X';
   SELECT id INTO lpartitionid FROM njpartition WHERE classcode = 'S';
   SELECT id INTO nexjsaentityid FROM njentity WHERE lastname$ ='NEXJSA';
   SELECT id
     INTO contacttypeid
     FROM njentitytype
    WHERE UPPER("NAME") LIKE 'CONTACT';
   SELECT id
     INTO companytypeid
     FROM njentitytype
    WHERE UPPER("NAME") LIKE 'COMPANY';
   SELECT sys_extract_utc(systimestamp) INTO pTimestamp FROM DUAL;
  
  --Step 2.0 Reporting 
   select count(1) into i_rows_total
    FROM S_njsentity e
      WHERE pisoasofdate = e.isoasofdate
      AND (e.sourceId      = dataSource
      OR dataSource        = ' ');
   logStartNJSProcess( pisoasofdate,step,i_rows_total,'Entity insert candidates' );
   commit;
  --Step 2.1 Create Mapper for insert and update show err
  if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => STEP*100+1,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJSENTITYMAPPER',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'ALL TOTAL ENTITIES ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = glResponseContinue )then
   INSERT
     INTO S_njsentitymapper
    (
      sourceid             ,
      isoasofdate          ,
      externalid           ,
      externalupddate      ,
      entitytype           ,
      njid                 ,
      NJAGGRPRFLIOID       ,
      NJAGGRVIRTUALPRFLIOID,
      updcandidate         ,
      inscandidate         ,
      processStatusBF      ,
      dealerCode,
      repCode,
      errorcode
    )
    (SELECT e.sourceid      ,
        e.isoasofdate       ,
        e.externalid        ,
        e.updateddate        ,
        e.entitytype        ,
        hextoraw(sys_guid()),
        hextoraw(sys_guid()),
        hextoraw(sys_guid()),
        1                   ,
        1                   ,
        1                   ,
        E.dealerCode,
        E.repCode,
        0
         FROM S_njsentity e
        WHERE pisoasofdate = e.isoasofdate
      AND (e.sourceId      = dataSource
      OR dataSource        = ' ')
      AND NOT EXISTS
        ( -- The update will be handled through the sync engine.
         SELECT 'I'
           FROM S_njsentitymapper emapper
          WHERE e.sourceid = emapper.sourceid
        AND e.externalid   = emapper.externalid
        )
    ) ;
  --    Log inserting rows candidate show err
  I_ROWS_TOTAL := SQL % ROWCOUNT;
  I_ROWS_PROCESSED:= I_ROWS_TOTAL ;
  LOGENDNJSPROCESS_LOG2( PISOASOFDATE,STEP*100+1,I_ROWS_TOTAL,'Entity insert - New Entities ',PTARGETTABLE => 'NJSENTITYMAPPER',
    POPERATION => 'INSERT' );
  commit;
  end if;
  --Step 2.2 Mark Mapper for update
  -- Marking as last update date
  --logStartNJSProcess(pisoasofdate,step*100+2,u_rows_processed,'Entity Update');
    if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => STEP*100+2,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJSENTITYMAPPER',
    POPERATION => 'UPDATE',
    PCUSTOMMESSAGE => 'ALL TOTAL ENTITIES - UPDATE CANDIDATES',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'Y'
  ) = GLRESPONSECONTINUE )THEN
  
  IF (DATASOURCE = 'RPM' OR DATASOURCE = ' ' ) THEN

   UPDATE S_njsentitymapper M
  SET
    (
      M.updcandidate   ,
      M.externalupddate,
      M.isoasofdate
    )
    =
    (SELECT 1      ,
      e.updateddate,
      pisoasofdate
       FROM S_njsentity e
      WHERE e.isoasofdate = pisoasofdate
    AND e.sourceid        = m.sourceid
    AND e.externalid      = m.externalid
    )
    WHERE 
    (m.sourceId = dataSource
      OR dataSource       = ' ')
    AND (
      (EXISTS
        (SELECT 'X'
        FROM S_njsentity njse
          WHERE njse.sourceid = m.sourceid
          AND njse.externalid   = m.externalid
          AND njse.isoasofdate  = pisoasofdate
          AND (njse.updateddate  > m.externalupddate))
    ));
  u_rows_processed := SQL % rowcount;
  end if;

  if (dataSource = 'AAA' or dataSource = ' ' ) then
  u_rows_processed := 0;
  for rec in cur
  loop
        UPDATE S_njsentitymapper M
        SET    
        (      
          M.updcandidate,
          M.externalupddate,
          M.isoasofdate  
        )    
        =    
        (SELECT 1,     
        rec.updateddate,
        pisoasofdate    
        FROM dual)
        where M.rowId = rec.rowId;
        u_rows_processed:=u_rows_processed+1;
  end loop;
  end if;    
  u_rows_processed := SQL % rowcount;
  --REPORT UPDATE Candidates
  
    LOGENDNJSPROCESS_LOG2( PISOASOFDATE,STEP*100+2,u_rows_processed,'Entity Update ',PTARGETTABLE => 'NJSENTITYMAPPER',
    POPERATION => 'INSERT' );
    COMMIT;
  END IF;
  -- step 3.1 do the insert - NJEntity show err
    if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => STEP*100+3,
    PAFFECTEDRECORDS => I_ROWS_TOTAL,
    PTARGETTABLE => 'NJENTITY',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'ALL TOTAL ENTITIES - MAIN ENTITY ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = glResponseContinue )then
   INSERT
     INTO njentity
    (
      id                ,
      classcode         ,
      typeid            ,
      birthtime         ,
      gendercode        ,
      lastname          ,
      lastname$         ,
      firstname         ,
      firstname$        ,
      lastupdated       ,
      title             ,
      title$            ,
      affix             ,
      affix$            ,
      initials          ,
      initials$         ,
      valid             ,
      isclient          ,
      isprospect        ,
      isformerclient    ,
      isother           ,
      syscreateuserid   ,
      syscreatetime     ,
      sysedituserid     ,
      sysedittime       ,
      createtime        ,
      createuseralias   ,
      createauthoralias ,
      edittime          ,
      edituseralias     ,
      editauthoralias   ,
      viewprincipalid   ,
      editprincipalid   ,
      deletedflag       ,
      readonlyflag      ,
      coverageparentid  ,
      defaultlang       ,
      dear              ,
      dear$             ,
      tier              ,
      ssn               ,
      ssn$              ,
      isinvestingcompany,
      locking           ,
      "SOURCE"          ,
      SOURCEIDENTIFIER  ,
      SOURCEIDENTIFIER$ ,
      branchprincipalid ,
      partitionid
    )
   SELECT mapper.njid,
    CASE e.entitytype
      WHEN N'P'
      THEN 'PSN'
      WHEN N'C'
      THEN 'CO'
    END --classcode
    ,
    CASE e.entitytype
      WHEN N'P'
      THEN contacttypeid
      WHEN N'C'
      THEN companytypeid
    END                                    ,--typeid                               ,
    e.dateofbirth                          ,
    e.gender                               ,
    e.lastname                             ,
    UPPER(e.lastname)                      ,
    SUBSTR(NVL(firstname,' '),0,25)        ,
    UPPER(SUBSTR(NVL(firstname,' '),0,25)) ,
    pTimestamp                             ,
    e.title                                ,
    UPPER(e.title)                         ,
    e.affix                                ,
    UPPER(e.affix)                         ,
    e.initials                             ,
    UPPER(e.initials)                      ,
    1                                      ,
    e.isclient                             ,
    e.isprospect                           ,
    e.isformerclient                       ,
    e.isother                              ,
    NVL(BU.NJUSERID ,nexjsauserid)         ,
    pTimestamp                             ,
    NVL(BU.NJUSERID ,nexjsauserid)         ,
    pTimestamp                             ,
    NVL(e.syscreatedate,pTimestamp)        ,
    NVL(BU.NJALIAS ,'nexjsa')              ,
    NVL(BU.NJALIAS ,'nexjsa')              ,
    NVL(e.updateddate,pTimestamp)          ,
    NVL(BU.NJALIAS ,'nexjsa')              ,
    NVL(BU.NJALIAS ,'nexjsa')              ,
    viewprincipalid                        ,
    viewprincipalid                        ,
    0                                      ,
    0                                      ,
    mapper.njid                            ,
    e.entitylanguage                       ,
    e.firstname                            ,
    UPPER(e.firstname)                     ,
    'UNKNOWN'                              ,
    e.SIN                                  ,
    UPPER(e.SIN)                           ,
    0                                      ,
    0                                      ,
    e.sourceid                             ,
    e.externalid                           ,
    upper(e.externalid)                    ,
    viewprincipalid,
    lpartitionid
     FROM S_njsentity e
  INNER JOIN S_njsentitymapper mapper
       ON e.sourceid      = mapper.sourceid
  AND e.externalid        = mapper.externalid
  AND mapper.inscandidate = 1
  AND pisoasofdate        = mapper.isoasofdate
  AND pisoasofdate        = e.isoasofdate
  LEFT OUTER JOIN S_BUSINESSUSER BU
       ON mapper.sourceid = BU.NJALIAS;
  -- ready to reporting value
  
  I_ROWS_PROCESSED := sql % ROWCOUNT;
    LOGENDNJSPROCESS_LOG2( PISOASOFDATE,STEP*100+3,I_ROWS_PROCESSED,'Entity completed ',PTARGETTABLE => 'NJENTITY',
    POPERATION => 'INSERT' );
  COMMIT;
  end if;
  
  -- Step 2.4 NJEntityXA01 -  show err
  -- Includes DealerRepCode data
    if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => STEP*100+4,
    PAFFECTEDRECORDS => I_ROWS_TOTAL,
    PTARGETTABLE => 'NJENTITYXA01',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'ALL TOTAL ENTITIES - TRADING INFORMATION ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = glResponseContinue )then
  INSERT
     INTO NJEntityXA01
    (
      id               ,
      activeStatus     ,
      isNewEntity      ,
      reportingCurrency,
      CIF              ,
      CIF$             ,
      isProfessional   ,
      dealerRepCodeId  ,
      partitionId
    )
   SELECT mapper.njid     ,
    e.active              ,
    0                     ,
    'CAD'                 ,
    e.cif                 ,
    upper(e.cif)          ,
    e.isProfessional      ,
    drc.NJDEALEREREPCODEID,
    lpartitionid
     FROM S_njsentity e
  INNER JOIN S_njsentitymapper mapper
       ON e.sourceid      = mapper.sourceid
  AND e.externalid        = mapper.externalid
  AND mapper.inscandidate = 1
  AND pisoasofdate       = mapper.isoasofdate
  AND pisoasofdate       = e.isoasofdate
  left outer join S_NJSDEALERREPCODEMAPPER drc
       ON drc.extRepCode = e.repCode
  AND drc.extDealerCode  = e.dealerCode
  AND pisoasofdate = drc.isoasofdate
  AND drc.sourceid       ='AADB';
  
  
  I_ROWS_PROCESSED := SQL % ROWCOUNT;
   LOGENDNJSPROCESS_LOG2( PISOASOFDATE,STEP*100+4,i_rows_processed,'Entity Insert  - XA01 ',PTARGETTABLE => 'NJENTITYXA01',
    POPERATION => 'INSERT' );
  commit;
  end if;
  -- step 3.2 do the insert - NJENTITYDATE - BIRTHTIME show err
    if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => STEP*100+5,
    PAFFECTEDRECORDS => I_ROWS_TOTAL,
    PTARGETTABLE => 'NJENTITYDATE',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'ALL TOTAL ENTITIES - DATE BIRTHDAY ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = GLRESPONSECONTINUE )then
  
   INSERT
     INTO njentitydate
    (
      id               ,
      syscreatetime    ,
      syscreateuserid  ,
      sysedittime      ,
      sysedituserid    ,
      deletedflag      ,
      readonlyflag     ,
      entityid         ,
      datetype         ,
      fulldate         ,
      YEAR             ,
      MONTH            ,
      DAY              ,
      monthday         ,
      locking          ,
      partitionid
    )
   SELECT sys_guid()                                                      ,
    pTimestamp                                                            ,
    a.syscreateuserid                                                     ,
    pTimestamp                                                            ,
    a.sysedituserid                                                       ,
    0                                                                     ,
    0                                                                     ,
    a.id                                                                  ,
    'BIRTHTIME'                                                           ,
    TRUNC(a.birthtime)                                                    ,
    EXTRACT(YEAR FROM a.birthtime)                                        ,
    EXTRACT(MONTH FROM a.birthtime)                                       ,
    EXTRACT(DAY FROM a.birthtime)                                         ,
    (EXTRACT(MONTH FROM a.birthtime) *100 + EXTRACT(DAY FROM a.birthtime)),
    0                                                                     ,
    lpartitionid
     FROM njentity a
  INNER JOIN S_njsentitymapper mapper
       ON a.id            = mapper.njid
  AND mapper.inscandidate = 1
  AND a.birthtime        IS NOT NULL
  LEFT OUTER JOIN S_BUSINESSUSER BU
       ON mapper.sourceid = BU.NJALIAS;
       
  I_ROWS_PROCESSED := SQL % ROWCOUNT;
   LOGENDNJSPROCESS_LOG2( PISOASOFDATE,STEP*100+5,I_ROWS_PROCESSED,'Entity Insert  - BirthDate ',PTARGETTABLE => 'NJENTITYDATE',
    POPERATION => 'INSERT' );
  COMMIT;
  end if;
  -- step 3.3 do the insert - NJENTITYDATE - EDITTIME show err
    if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => STEP*100+6,
    PAFFECTEDRECORDS => I_ROWS_TOTAL,
    PTARGETTABLE => 'NJENTITYDATE',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'ALL TOTAL ENTITIES - DATE - EDIT TIME',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = glResponseContinue )then
  -- Same Entity DAte for the EDITTIME
   INSERT
     INTO njentitydate
    (
      id               ,
      syscreatetime    ,
      syscreateuserid  ,
      sysedittime      ,
      sysedituserid    ,
      deletedflag      ,
      readonlyflag     ,
      entityid         ,
      datetype         ,
      fulldate         ,
      YEAR             ,
      MONTH            ,
      DAY              ,
      monthday         ,
      locking          ,
      partitionid
    )
   SELECT sys_guid()                                                    ,
    pTimestamp                                                          ,
    a.syscreateuserid                                                   ,
    pTimestamp                                                          ,
    a.sysedituserid                                                     ,
    0                                                                   ,
    0                                                                   ,
    a.id                                                                ,
    'EDITTIME'                                                          ,
    TRUNC(a.edittime)                                                   ,
    EXTRACT(YEAR FROM a.edittime)                                       ,
    EXTRACT(MONTH FROM a.edittime)                                      ,
    EXTRACT(DAY FROM a.edittime)                                        ,
    (EXTRACT(MONTH FROM a.edittime) *100 + EXTRACT(DAY FROM a.edittime)),
    0                                                                   ,
    lpartitionid
     FROM njentity a
  INNER JOIN S_njsentitymapper mapper
       ON a.id            = mapper.njid
  AND mapper.inscandidate = 1
  LEFT OUTER JOIN S_BUSINESSUSER BU
       ON MAPPER.SOURCEID = BU.NJALIAS;
  
  I_ROWS_PROCESSED := sql % ROWCOUNT;
   LOGENDNJSPROCESS_LOG2( PISOASOFDATE,STEP*100+6,I_ROWS_PROCESSED,'ALL TOTAL ENTITIES - DATE - EDIT TIME ',PTARGETTABLE => 'NJENTITYDATE',
    POPERATION => 'INSERT' );
  COMMIT;
  END IF;
  
  --Adding Self Hierarchies
  
   if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => STEP*100+7,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJENTITYHIERARCHY',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'ALL TOTAL ENTITIES - ENTITYHIERARCHY',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = glResponseContinue )then
  
   INSERT
     INTO NJEntityHierarchy
    (
      id       ,
      parentId ,
      childId  ,
      type     ,
      contextId,
      locking  ,
      partitionId
    )
   SELECT sys_guid() ,
    a.id             ,
    a.id             ,
    'S'              ,
    hcontext.id      ,
    0                ,
    lpartitionid
     FROM njentity a
  INNER JOIN S_njsentitymapper mapper
       ON a.id            = mapper.njid
  AND mapper.inscandidate = 1
  AND mapper.isoasofdate  = pisoasofdate
  INNER JOIN njhierarchycontext hcontext
       ON 1=1;
  -- set insert flag down
   UPDATE S_njsentitymapper mapper
  SET mapper.inscandidate     = 0
    WHERE mapper.inscandidate = 1
  AND pisoasofdate            = mapper.isoasofdate;
  I_ROWS_PROCESSED := sql % ROWCOUNT;
   LOGENDNJSPROCESS_LOG2( PISOASOFDATE,STEP*100+7,i_rows_processed,'Entity completed- EntityHierarchy ',PTARGETTABLE => 'NJENTITYHIERARCHY',
    POPERATION => 'INSERT' );
  COMMIT;
  end if;
  --Reporting
  logEndNJSProcess ( pisoasofdate,step,i_rows_processed,'Entity Insert completed' ) ;
  commit;
EXCEPTION
WHEN OTHERS THEN
  ROLLBACK;
  --Update njProcess with an error show err
  logFailNJSProcess(pisoasofdate,step,0,'An error was encountered converting entity- ' || SQLCODE ||' -ERROR- ' ||SQLERRM ||'-full-'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
  commit;
END conv_contact;
PROCEDURE conv_telephone
  (
    pisoasofdate VARCHAR,
    step         NUMBER    :=263,
    dataSource   VARCHAR   :=' ')
                           IS
  u_rows_processed INTEGER := 0;
  u_rows_total     INTEGER := 0;
  i_rows_processed INTEGER := 0;
  i_rows_total     INTEGER := 0;
  e_rows_processed INTEGER := 0;
  nexjsauserid njuser.loginname%type;
  viewprincipalid njentity.id%type;
  lpartitionid njpartition.id%type;
  nexjsaentityid njentity.id%type;
  homePhoneTypeid njtelcomtype.id%type;
  businessPhoneTypeid njtelcomtype.id%type;
  pTimestamp TIMESTAMP(3);
BEGIN
  --Step 1.1 assign global properties variables for the major insert show err
   SELECT id
     INTO nexjsauserid
     FROM njuser
    WHERE loginname LIKE TO_CHAR('nexjsa');
   SELECT id INTO viewprincipalid FROM njprincipal WHERE classcode = 'X';
   SELECT id INTO lpartitionid FROM njpartition WHERE classcode = 'S';
   SELECT id INTO nexjsaentityid FROM njentity WHERE lastname$ ='NEXJSA';
   SELECT id INTO homePhoneTypeid FROM njtelcomtype WHERE upper(NAME) = 'HOME';
   SELECT id
     INTO businessPhoneTypeid
     FROM njtelcomtype
    WHERE upper(NAME) = 'BUSINESS';
   SELECT sys_extract_utc(systimestamp) INTO pTimestamp FROM DUAL;
  --step 2.0 reporting
   SELECT COUNT(1)
     INTO i_rows_processed
     FROM S_njstelephone NJTS
    WHERE NJTS.ISOASOFDATE = pisoasofdate
    AND (NJTS.sourceId      = dataSource
      OR dataSource        = ' '); 
  logStartNJSProcess ( pisoasofdate,step,i_rows_processed,'Insert Telephone' ) ;
  commit;
  
  --Step 2.1 Inserting Work Phone mapping
  if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => STEP*100+1,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJTELCOM',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'ALL TOTAL NJTELCOM - TELEPHONE',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'Y'
  ) = GLRESPONSECONTINUE )THEN
  
   INSERT
    /*+ append */
     INTO NJTELCOM
    (
      ID                ,
      CLASSCODE         ,
      SYSCREATEUSERID   ,
      SYSCREATETIME     ,
      SYSEDITUSERID     ,
      SYSEDITTIME       ,
      CREATETIME        ,
      CREATEUSERALIAS   ,
      CREATEAUTHORALIAS ,
      EDITTIME          ,
      EDITUSERALIAS     ,
      EDITAUTHORALIAS   ,
      VIEWPRINCIPALID   ,
      EDITPRINCIPALID   ,
      DELETEDFLAG       ,
      READONLYFLAG      ,
      ENTITYID          ,
      NAME              ,
      NAME$             ,
      ADDRESS           ,
      ADDRESS$          ,
      USABLEPERIOD      ,
      TELCOMTYPEID      ,
      ISEXTERNAL        ,
      LOCKING           ,
      PARTITIONID
    )
   SELECT sys_guid()                  ,
    'tel'                             ,
    NVL(BU.NJUSERID ,nexjsauserid)    ,
    pTimestamp                        ,
    NVL(BU.NJUSERID ,nexjsauserid)    ,
    pTimestamp                        ,
    NVL(ts.SysCreateDate, pTimestamp) ,
    NVL(BU.NJALIAS ,'nexjsa')         ,
    NVL(BU.NJALIAS ,'nexjsa')         ,
    NVL(ts.SysUpdateDate, pTimestamp) ,
    NVL(BU.NJALIAS ,'nexjsa')         ,
    NVL(BU.NJALIAS ,'nexjsa')         ,
    viewprincipalid                   ,
    viewprincipalid                   ,
    0                                 ,
    1                                ,
    mapper.njid                       ,
    telMapper.NJNAME                  ,
    telMapper.NJNAME                  ,
    ts.phoneNumber                    ,
    upper(ts.phoneNumber)             ,
    NULL                              ,
    telMapper.NJTYPEID                ,
    1                                ,
    0                                 ,
    lpartitionid
     FROM S_njstelephone TS
  INNER JOIN S_njsentitymapper mapper
       ON TS.sourceid = mapper.sourceid
  AND TS.EntityID     = mapper.externalid
  INNER JOIN S_BUSINESSDOMAIN telmapper
       ON TS.sourceid            =telmapper.externalsourceID
  AND upper(TS.telephoneType)    = upper(telmapper.externalName)
  AND telmapper.CONVERTIONDOMAIN = 'NJSTelephone'
  LEFT OUTER JOIN S_BUSINESSUSER BU
       ON mapper.sourceid = BU.NJALIAS
    WHERE TS.ISOASOFDATE  = pisoasofdate
     AND (TS.sourceId      = dataSource
      OR dataSource        = ' ');
   u_rows_processed := SQL % rowcount;
  --REPORT telephone records inserts
  logEndNJSProcess(pisoasofdate,step  *100+1,u_rows_processed,'telephone records inserts');
  COMMIT;
  END IF;
  
  --Step 3.0 Update Entity - Home (default)
    if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => STEP*100+2,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJENTITY',
    POPERATION => 'UPDATE',
    PCUSTOMMESSAGE => 'ALL TOTAL NJTELCOM - UPDATE DEFAULT HOME ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'Y'
  ) = GLRESPONSECONTINUE )THEN
  -- Update Entity Work - Default, if no default selected show err
   UPDATE NJEntity E
  SET
    (
      E.lastUpdated   ,
      E.HOMEPHONE     ,
      E.DEFAULTTELCOM ,
      E.sysEditTime   ,
      E.editTime
    )
    =
    (SELECT telcom.editTime ,
      telcom.id             ,
      telcom.id             ,
      telcom.editTime       ,
      telcom.editTime
       FROM NJTELCOM telcom
      WHERE telcom.entityId = e.id
    AND telcom.TELCOMTYPEID =homePhoneTypeid
    AND rownum              = 1
    )
    WHERE e.id IN
    (SELECT DISTINCT(a.entityId)
       FROM NJTELCOM a
      WHERE a.TELCOMTYPEID =homePhoneTypeid
    );
    
     u_rows_processed := SQL % rowcount;
  --REPORT HomePhone Entity Update
  LOGENDNJSPROCESS(PISOASOFDATE,STEP  *100+2,U_ROWS_PROCESSED,'HomePhone Entity Update');
  END IF;
  commit;
  --Step 3.1 Update Entity Work - Default, if no default selected
   if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => STEP*100+3,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJENTITY',
    POPERATION => 'UPDATE',
    PCUSTOMMESSAGE => 'ALL TOTAL NJTELCOM -  UPDATE DEFAULT BUSINESS',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'Y'
  ) = GLRESPONSECONTINUE )THEN
   UPDATE NJEntity E
  SET
    (
      E.lastUpdated   ,
      E.WORKPHONE     ,
      E.DEFAULTTELCOM ,
      E.sysEditTime   ,
      E.editTime
    )
    =
    (SELECT telcom.editTime          ,
      telcom.id                      ,
      NVL(E.DEFAULTTELCOM,telcom.id) ,
      telcom.editTime                ,
      telcom.editTime
       FROM NJTELCOM telcom
      WHERE telcom.entityId = e.id
    AND telcom.TELCOMTYPEID =businessPhoneTypeid
    AND rownum              = 1
    )
    WHERE e.id IN
    (SELECT DISTINCT(a.entityId)
       FROM NJTELCOM a
      WHERE a.TELCOMTYPEID =businessPhoneTypeid
    );
       u_rows_processed := SQL % rowcount;
  --REPORT WorkPhone Entity Update
  logEndNJSProcess(pisoasofdate,step  *100+3,u_rows_processed,'WorkPhone Entity Update');
  commit;
  END IF;
  --NJENITY.HOMEPHONE --> NJENTITY.DEFAULTTELCOM
  --NJENTITY.WORKPHONE
  --WORK -->'BUSINESS'
  --HOME--> 'HOME'
  logEndNJSProcess ( pisoasofdate,step,i_rows_processed,'Insert Telephone' ) ;
  -- show err
  commit;
EXCEPTION
WHEN OTHERS THEN
  ROLLBACK;
  --Update njProcess with an error
  logFailNJSProcess(pisoasofdate,step,0,'An error was encountered Converting ['||step||']Phones- ' || SQLCODE ||' -ERROR- ' ||SQLERRM ||'-full-'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
  commit;
END conv_telephone;
PROCEDURE conv_email
  (
    pisoasofdate VARCHAR,
    step         NUMBER    :=262,
    dataSource   VARCHAR   :=' ')
                           IS
  u_rows_processed INTEGER := 0;
  u_rows_total     INTEGER := 0;
  i_rows_processed INTEGER := 0;
  i_rows_total     INTEGER := 0;
  e_rows_processed INTEGER := 0;
  nexjsauserid njuser.loginname%type;
  viewprincipalid njentity.id%type;
  lpartitionid njpartition.id%type;
  nexjsaentityid njentity.id%type;
  emailTypeid njtelcomtype.id%type;
  webpageTypeid njtelcomtype.id%type;
  pTimestamp TIMESTAMP (3);
BEGIN
  --Step 1.1 assign global properties variables for the major insert show err
   SELECT id
     INTO nexjsauserid
     FROM njuser
    WHERE loginname LIKE TO_CHAR('nexjsa');
   SELECT id INTO viewprincipalid FROM njprincipal WHERE classcode = 'X';
   SELECT id INTO lpartitionid FROM njpartition WHERE classcode = 'S';
   SELECT id INTO nexjsaentityid FROM njentity WHERE lastname$ ='NEXJSA';
   SELECT id
     INTO emailTypeid
     FROM njtelcomtype
    WHERE upper(NAME) = 'PERSONAL EMAIL ADDRESS';
   SELECT id INTO webpageTypeid FROM njtelcomtype WHERE upper(NAME) = 'WEB SITE';
   SELECT sys_extract_utc(systimestamp) INTO pTimestamp FROM DUAL;
   -- step 2.0 - estimate emails to process
      select count(1) into i_rows_total
    FROM S_NJSELECTRONICADDRESS E
      WHERE pisoasofdate = E.isoasofdate
      AND (E.sourceId      = dataSource
      OR dataSource        = ' ');
   logStartNJSProcess( pisoasofdate,step,i_rows_total,'Electronic Address candidates' );
   commit;
   i_rows_total:=0;
  --step 2.1 Inserting Work email mapping
   if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => STEP*100+1,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJTELCOM',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'ALL TOTAL NJTELCOM -  WEBSITE / EMAIL',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'Y'
  ) = GLRESPONSECONTINUE )THEN
   INSERT
     INTO NJTELCOM
    (
      "ID"              ,
      CLASSCODE         ,
      SYSCREATEUSERID   ,
      SYSCREATETIME     ,
      SYSEDITUSERID     ,
      SYSEDITTIME       ,
      CREATETIME        ,
      CREATEUSERALIAS   ,
      CREATEAUTHORALIAS ,
      EDITTIME          ,
      EDITUSERALIAS     ,
      EDITAUTHORALIAS   ,
      VIEWPRINCIPALID   ,
      EDITPRINCIPALID   ,
      DELETEDFLAG       ,
      READONLYFLAG      ,
      ENTITYID          ,
      "NAME"            ,
      NAME$             ,
      ADDRESS           ,
      ADDRESS$          ,
      USABLEPERIOD      ,
      TELCOMTYPEID      ,
      ISEXTERNAL        ,
      LOCKING           ,
      PARTITIONID
    )
   SELECT sys_guid() ,
    CASE
      WHEN njsea.electronicType='Website'
      THEN 'http'
      WHEN njsea.electronicType='Email'
      THEN 'mailto'
    END                                  ,--TYPEID                       ,
    NVL(BU.NJUSERID ,nexjsauserid)       ,
    pTimestamp                           ,
    NVL(BU.NJUSERID ,nexjsauserid)       ,
    pTimestamp                           ,
    NVL(njsea.SysCreateDate ,pTimeStamp) ,
    NVL(BU.NJALIAS ,'nexjsa')            ,
    NVL(BU.NJALIAS ,'nexjsa')            ,
    NVL(njsea.SysUpdateDate ,pTimeStamp) ,
    NVL(BU.NJALIAS ,'nexjsa')            ,
    NVL(BU.NJALIAS ,'nexjsa')            ,
    viewprincipalid                      ,
    viewprincipalid                      ,
    0                                    ,
    1                                    ,
    mapper.njid                          ,
    telMapper.NJNAME                     ,
    telMapper.NJNAME                     ,
    njsea.electronicAddress              ,
    upper(njsea.electronicAddress)       ,
    NULL                                 ,
    telMapper.NJTYPEID                   ,
    1                                   ,
    0                                    ,
    lpartitionid
     FROM S_NJSELECTRONICADDRESS njsea
  INNER JOIN S_njsentitymapper mapper
       ON njsea.sourceid = mapper.sourceid
  AND njsea.EntityID     = mapper.externalid
  INNER JOIN S_BUSINESSDOMAIN telmapper
       ON njsea.sourceid          =telmapper.externalsourceID
  AND upper(njsea.electronicType) = upper(telmapper.externalName)
  AND telmapper.CONVERTIONDOMAIN  = 'NJSElectronicAddress'
  LEFT OUTER JOIN S_BUSINESSUSER BU
       ON mapper.sourceid   = BU.NJALIAS
    WHERE njsea.ISOASOFDATE = pisoasofdate
    AND (njsea.sourceId      = dataSource
      OR dataSource        = ' ');
  
   u_rows_processed := SQL % rowcount;
  --REPORT Insert new Telcoms
  logEndNJSProcess(pisoasofdate,step  *100+1,u_rows_processed,'Telcom Insert');
  commit; 
  end if;
  
  -- Step 3.1 Update Entity EMAIL show err
     if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => STEP*100+2,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJENTITY',
    POPERATION => 'UPDATE',
    PCUSTOMMESSAGE => 'UPDATE NJENTITY - DEFAULT NJTELCOM -  WEBSITE / EMAIL',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'Y'
  ) = GLRESPONSECONTINUE )THEN
   UPDATE NJEntity E
  SET
    (
      E.lastUpdated ,
      E.EMAIL       ,
      E.sysEditTime ,
      E.editTime
    )
    =
    (SELECT telcom.editTime ,
      telcom.id             ,
      telcom.editTime       ,
      pTimestamp
       FROM NJTELCOM telcom
      WHERE telcom.entityId = e.id
    AND telcom.TELCOMTYPEID =emailTypeid
    AND rownum              = 1
    )
    WHERE e.id IN
    (SELECT DISTINCT(a.entityId)
       FROM NJTELCOM a
      WHERE a.TELCOMTYPEID =emailTypeid
    );
  -- REporting
  i_rows_processed := SQL%rowcount;
  logEndNJSProcess ( pisoasofdate,step  *100+2,i_rows_processed,'Update Entity Electronic address' ) ;
  commit;
  end if;
  
  logEndNJSProcess ( pisoasofdate,step,i_rows_total,'Insert Electronic address' ) ;
  commit;
EXCEPTION
WHEN OTHERS THEN
  ROLLBACK;
  --Update njProcess with an error show err
  logFailNJSProcess(pisoasofdate,step,0,'An error was encountered ['||262||']Converting Electronic telcoms- ' || SQLCODE ||' -ERROR- ' ||SQLERRM ||'-full-'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
  commit;
END conv_email;
PROCEDURE conv_address
  (
    pisoasofdate VARCHAR,
    step         NUMBER    :=261,
    dataSource   VARCHAR   :=' ')
                           IS
  u_rows_processed INTEGER := 0;
  u_rows_total     INTEGER := 0;
  i_rows_processed INTEGER := 0;
  i_rows_total     INTEGER := 0;
  e_rows_processed INTEGER := 0;
  nexjsauserid njuser.loginname%type;
  viewprincipalid njentity.id%type;
  lpartitionid njpartition.id%type;
  nexjsaentityid njentity.id%type;
  homeaddressID njaddresstype.id%type;
  pTimeStamp TIMESTAMP(3);
BEGIN
  --Step 1.1 assign global properties variables for the major insert
   SELECT id
     INTO nexjsauserid
     FROM njuser
    WHERE loginname LIKE TO_CHAR('nexjsa');
   SELECT id INTO viewprincipalid FROM njprincipal WHERE classcode = 'X';
   SELECT id INTO lpartitionid FROM njpartition WHERE classcode = 'S';
   SELECT id INTO nexjsaentityid FROM njentity WHERE lastname$ ='NEXJSA';
   SELECT id
     INTO homeaddressID
     FROM njaddresstype
    WHERE upper(NAME) = 'HOME ADDRESS';
   SELECT sys_extract_utc(systimestamp) INTO pTimeStamp FROM DUAL;
   
   -- step 2.0 - select address insert candidates
    select count(1) into i_rows_total
    FROM S_njsaddress E
      WHERE pisoasofdate = E.isoasofdate
      AND (E.sourceId      = dataSource
      OR dataSource        = ' ');
   logStartNJSProcess( pisoasofdate,step,i_rows_total,'address insert candidates' );
   commit;
   
  -- step 2.1 Insert NJADDRESS
     if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => STEP*100+1,
    PAFFECTEDRECORDS => i_rows_total,
    PTARGETTABLE => 'NJADDRESS',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'ALL TOTAL NJADDRESS ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'Y'
  ) = GLRESPONSECONTINUE )THEN
   INSERT
    /*+ append */
     INTO NJAddress
    (
      id               ,
      sysCreateUserId  ,
      sysCreateTime    ,
      sysEditUserId    ,
      sysEditTime      ,
      createTime       ,
      createUserAlias  ,
      createAuthorAlias,
      editTime         ,
      editUserAlias    ,
      editAuthorAlias  ,
      viewPrincipalId  ,
      editPrincipalId  ,
      deletedFlag      ,
      readOnlyFlag     ,
      entityId         ,
      addressTypeId    ,
      name             ,
      name$            ,
      address1         ,
      address1$        ,
      address2         ,
      address2$        ,
      address3         ,
      address3$        ,
      address4         ,
      address4$        ,
      city             ,
      city$            ,
      state            ,
      state$           ,
      country          ,
      country$         ,
      zip              ,
      zip$             ,
      POBox            ,
      POBox$           ,
      locking          ,
      partitionId
    )
   SELECT sys_guid()                     ,
    NVL(BU.NJUSERID ,nexjsauserid)       ,
    pTimeStamp                           ,
    NVL(BU.NJUSERID ,nexjsauserid)       ,
    pTimeStamp                           ,
    pTimeStamp                           ,
    NVL(BU.NJALIAS ,'nexjsa')            ,
    NVL(BU.NJALIAS ,'nexjsa')            ,
    pTimeStamp                           ,
    NVL(BU.NJALIAS ,'nexjsa')            ,
    NVL(BU.NJALIAS ,'nexjsa')            ,
    viewprincipalid                      ,
    viewprincipalid                      ,
    0                                    ,
    1                                    ,
    mapper.njid                          ,
    NVL(atype.id,homeaddressID)          ,
    NVL(atype.name,'Home Address')       ,
    upper(NVL(atype.name,'Home Address')),
    address.addressLine1                 ,
    upper(address.addressLine1)          ,
    address.addressLine2                 ,
    upper(address.addressLine2)          ,
    address.addressLine3                 ,
    upper(address.addressLine3)          ,
    address.addressLine4                 ,
    upper(address.addressLine4)          ,
    address.city                         ,
    upper(address.city)                  ,
    address.provinceState                ,
    upper(address.provinceState)         ,
    address.country                      ,
    upper(address.country)               ,
    address.zipPostalCode                ,
    upper(address.zipPostalCode)         ,
    NULL                                 ,
    NULL                                 ,
    0                                    ,
    lpartitionid
     FROM S_njsaddress address
  INNER JOIN S_njsentitymapper mapper
       ON address.sourceid = mapper.sourceid
  AND address.EntityID     = mapper.externalid
  LEFT OUTER JOIN njaddresstype atype
       ON REPLACE(upper(address.addresstype),'WORK','BUSINESS')
    || ' ADDRESS' = upper(atype.NAME)
  LEFT OUTER JOIN S_BUSINESSUSER BU
       ON mapper.sourceid     = BU.NJALIAS
    WHERE address.ISOASOFDATE = pisoasofdate
    AND (address.sourceId      = dataSource
      OR dataSource        = ' ');
  --UPDATE BACK njentity with fresh created addresses IDs
  -- Reporting
  u_rows_processed := SQL % rowcount;
  --REPORT UPDATE Candidates
  logEndNJSProcess(pisoasofdate,step  *100+1,u_rows_processed,'Entity Update');
  COMMIT;
  end if;
  -- Step 3.1 Update entity - Home address
       if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => STEP*100+2,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJENTITY',
    POPERATION => 'UPDATE',
    PCUSTOMMESSAGE => 'UPDATE NJENTITY - DEFAULT BUSINESS ADDRESS',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'Y'
  ) = GLRESPONSECONTINUE )THEN
   UPDATE NJEntity E
  SET
    (
      E.lastUpdated     ,
      E.businessAddress ,
      E.defaultaddress  ,
      E.sysEditTime     ,
      E.editTime
    )
    =
    (SELECT address.editTime ,
      address.id             ,
      address.id             ,
      address.editTime       ,
      address.editTime
       FROM NJADDRESS address
      WHERE address.entityId   = e.id
    AND rownum                 =1
    AND address.addressTypeId IN
      (SELECT id FROM njaddresstype WHERE NAME$ = 'BUSINESS ADDRESS' AND rownum=1
      )
    )
    WHERE EXISTS
    (SELECT 'X'
       FROM NJADDRESS a
      WHERE a.entityId   = e.id
    AND a.addressTypeId IN
      (SELECT id FROM njaddresstype WHERE NAME$ = 'BUSINESS ADDRESS' AND rownum=1
      )
    AND rownum=1
    ) ;
     u_rows_processed := SQL % rowcount;
  --REPORT Bus Addr UPDATE 
  logEndNJSProcess(pisoasofdate,step  *100+2,u_rows_processed,'Entity Update - Bus Addr');
  COMMIT;
  end if;
  
  -- step 3.2 Home addr Update
         if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => STEP*100+3,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJENTITY',
    POPERATION => 'UPDATE',
    PCUSTOMMESSAGE => 'UPDATE NJENTITY - DEFAULT HOME ADDRESS',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'Y'
  ) = GLRESPONSECONTINUE )THEN
   UPDATE NJEntity E
  SET
    (
      E.lastUpdated    ,
      E.homeAddress    ,
      E.defaultaddress ,
      E.sysEditTime    ,
      E.editTime
    )
    =
    (SELECT address.editTime ,
      address.id             ,
      address.id             ,
      address.editTime       ,
      address.editTime
       FROM NJADDRESS address
      WHERE address.entityId   = e.id
    AND rownum                 =1
    AND address.addressTypeId IN
      (SELECT id FROM njaddresstype WHERE NAME$ = 'HOME ADDRESS'
      )
    AND rownum=1
    )
    WHERE EXISTS
    (SELECT 'X'
       FROM NJADDRESS a
      WHERE a.entityId   = e.id
    AND a.addressTypeId IN
      (SELECT id FROM njaddresstype WHERE NAME$ = 'HOME ADDRESS' AND rownum=1
      )
    AND rownum=1
    );
  u_rows_processed := SQL % rowcount;
  --REPORT Home Addr UPDATE 
  logEndNJSProcess(pisoasofdate,step  *100+3,u_rows_processed,'Entity Update - Home Addr');
  COMMIT;
  end if;
  
  --Step 4.0 update NJEntityDAte show err
         if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => STEP*100+4,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJENTITYDATE',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'Insert Entity Date - ADDRESS Entry DAte',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'Y'
  ) = GLRESPONSECONTINUE )THEN
   INSERT
     INTO NJEntityDate
    (
      id               ,
      sysCreateTime    ,
      sysCreateUserId  ,
      sysEditTime      ,
      sysEditUserId    ,
      deletedFlag      ,
      readOnlyFlag     ,
      entityId         ,
      dateType         ,
      fullDate         ,
      YEAR             ,
      MONTH            ,
      DAY              ,
      monthDay         ,
      locking          ,
      partitionId
    )
   SELECT sys_guid()                                                    ,
    pTimeStamp                                                          ,
    a.syscreateuserid                                                   ,
    pTimeStamp                                                          ,
    a.sysedituserid                                                     ,
    0                                                                   ,
    0                                                                   ,
    a.entityid                                                          ,
    'EDITTIME'                                                          ,
    TRUNC(a.edittime)                                                   ,
    EXTRACT(YEAR FROM a.edittime)                                       ,
    EXTRACT(MONTH FROM a.edittime)                                      ,
    EXTRACT(DAY FROM a.edittime)                                        ,
    (EXTRACT(MONTH FROM a.edittime) *100 + EXTRACT(DAY FROM a.edittime)),
    0                                                                   ,
    a.partitionid
     FROM njaddress a;
    u_rows_processed := SQL % rowcount;
  --Address Entity Date Insert
  logEndNJSProcess(pisoasofdate,step  *100+4,u_rows_processed,'Address Entity Date Insert');
  commit;
  end if;
  
  -- report end of the step
  logEndNJSProcess ( pisoasofdate,step,i_rows_total,'Insert Address' ) ;
  commit;
EXCEPTION
WHEN OTHERS THEN
  ROLLBACK;
  --Update njProcess with an error
  logFailNJSProcess ( pisoasofdate,step,0,'An error was encountered ['||step||'] Inserting  Address- ' || SQLCODE ||' -ERROR- ' ||SQLERRM ||'-full-' ||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE ) ;
  commit;
END conv_address;
PROCEDURE Conv_ClientRelationship
  (
    pisoasofdate VARCHAR,
    step         NUMBER    :=264,
    dataSource   VARCHAR   :=' ')
                           IS
  u_rows_processed INTEGER := 0;
  u_rows_total     INTEGER := 0;
  i_rows_processed INTEGER := 0;
  i_rows_total     INTEGER := 0;
  e_rows_processed INTEGER := 0;
  nexjsauserid njuser.loginname%type;
  lviewprincipalid njentity.id%type;
  lpartitionid njpartition.id%type;
  nexjsaentityid njentity.id%type;
  ConnectedWithCustomfieldCode njcustomfieldtype.id%type;
  currentTimeStamp TIMESTAMP(3);
BEGIN
  --Step 1.1 assign global properties variables for the major insert
   SELECT id
     INTO nexjsauserid
     FROM njuser
    WHERE loginname LIKE TO_CHAR('nexjsa');
   SELECT id INTO lviewprincipalid FROM njprincipal WHERE classcode = 'X';
   SELECT id INTO lpartitionid FROM njpartition WHERE classcode = 'S';
   SELECT id INTO nexjsaentityid FROM njentity WHERE lastname$ ='NEXJSA';
   SELECT sys_extract_utc(systimestamp) INTO currentTimeStamp FROM dual;
   SELECT id
     INTO ConnectedWithCustomfieldCode
     FROM NJCustomFieldType A
    WHERE A.name$   = nls_upper('CONNECTED WITH')
  AND A.partitionId = lpartitionid
  AND A.deletedFlag = 0;
  -- reporting
   SELECT COUNT(1)
     INTO i_rows_processed
     FROM S_NJSClientRelationship CR
    WHERE CR.isoasofdate = pisoasofdate;
   
   
        if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => STEP*100+4,
    PAFFECTEDRECORDS => i_rows_processed,
    PTARGETTABLE => 'NJCUSTOMFIELD',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'Insert Entity relationship  - custom field',
    EXECUTIONOVERRIDEFAILED => 'N',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = GLRESPONSECONTINUE )THEN
   INSERT  INTO NJCustomField
    (
      id                ,
      classCode         ,
      sysCreateUserId   ,
      sysCreateTime     ,
      sysEditUserId     ,
      sysEditTime       ,
      createTime        ,
      createUserAlias   ,
      createAuthorAlias ,
      editTime          ,
      editUserAlias     ,
      editAuthorAlias   ,
      viewPrincipalId   ,
      editPrincipalId   ,
      deletedFlag       ,
      readOnlyFlag      ,
      entityId          ,
      customFieldTypeId ,
      entityValueId     ,
      locking           ,
      partitionId
    )
   SELECT sys_guid()                       ,
    'ent'                                  ,
    NVL(BU.NJUSERID ,nexjsauserid)         ,
    currentTimeStamp                       ,
    NVL(BU.NJUSERID ,nexjsauserid)         ,
    currentTimeStamp                       ,
    NVL(cr.SysCreateDate,currentTimeStamp) ,
    NVL(BU.NJALIAS ,'nexjsa')              ,
    NVL(BU.NJALIAS ,'nexjsa')              ,
    NVL(cr.SysUpdateDate,currentTimeStamp) ,
    NVL(BU.NJALIAS ,'nexjsa')              ,
    NVL(BU.NJALIAS ,'nexjsa')              ,
    lviewprincipalid                       ,
    lviewprincipalid                       ,
    0                                      ,
    1                                      ,
    fromId.njid                            ,
    NVL(a.id,ConnectedWithCustomfieldCode) ,
    toid.njid                              ,
    0                                      ,
    lpartitionID
     FROM S_NJSClientRelationship cr
  INNER JOIN S_NJSENTITYMAPPER fromId
       ON fromid.externalid = cr.entityFromID
  AND fromid.sourceid       = cr.sourceid
  INNER JOIN S_NJSENTITYMAPPER toId
       ON toId.externalid = cr.entityToID
  AND toId.sourceid       = cr.sourceid
  LEFT OUTER JOIN NJCustomFieldType A
       ON A.name$   = nls_upper(cr.relationshipNAME)
  AND a.deletedflag = 0
  AND A.partitionId = lpartitionid
  LEFT OUTER JOIN S_BUSINESSUSER BU
       ON cr.sourceid    = BU.NJALIAS
    WHERE cr.isoasofdate = pisoasofdate;
  -- REporting
  i_rows_processed:= SQL% rowcount;
  logEndNJSProcess(pisoasofdate,step,i_rows_processed,'ClientRelationship   Insert Complete');
  COMMIT;
  end if;
EXCEPTION
WHEN OTHERS THEN
  ROLLBACK;
  --Update njProcess with an error show err
  logFailNJSProcess(pisoasofdate,step,0,'An error was encountered Converting ['||step||'] Custom entity relationships- ' || SQLCODE ||' -ERROR- ' ||SQLERRM ||'-full-'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
  commit;
END Conv_ClientRelationship;
END njsconversioncrm;