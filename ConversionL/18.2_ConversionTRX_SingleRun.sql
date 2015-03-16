CREATE OR REPLACE
PACKAGE BODY NJSConversionTransaction
AS
PROCEDURE Conv_TransactionPreProcess
  (
    pISOasOfDate VARCHAR, step NUMBER:=524, dataSource VARCHAR:=' ')
AS
  i_rows_processed INTEGER := 0;
  i_rows_total     INTEGER := 0;
  nexjsauserid njuser.id%type;
  viewprincipalid njentity.id%type;
  lpartitionid njpartition.id%type;
  nexjsaentityid njentity.id%type;
  contacttypeid njentitytype.id%type;
  companytypeid njentitytype.id%type;
  currentTimeStamp TIMESTAMP(3);
BEGIN
  --Step 1.1 assign global properties variables for the major insert show err
   SELECT id
     INTO nexjsauserid
     FROM njuser
    WHERE loginname LIKE 'nexjsa';
   SELECT id INTO viewprincipalid FROM njprincipal WHERE classcode = 'X';
   SELECT id INTO lpartitionid FROM njpartition WHERE classcode = 'S';
   SELECT id INTO nexjsaentityid FROM njentity  WHERE lastname$ ='NEXJSA';
   SELECT sys_extract_utc(systimestamp) INTO currentTimeStamp FROM dual;
   
   -- step 2.1 - count candidates 
   SELECT COUNT(trx.sourceid) INTO i_rows_total FROM S_NJSTRANSACTION trx
     WHERE pisoasofdate  = trx.isoasofdate
          AND (trx.sourceId = dataSource or dataSource = ' ');
    logStartNJSProcess ( pisoasofdate,step,i_rows_total,'Possible Insert Transactions' ) ;
    commit;
    
    
     if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => (step)*100+1,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJSTRANSACTION',
    POPERATION => 'SELECT COUNT',
    PCUSTOMMESSAGE => 'Possible Insert Reversal Transactions ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'Y'
  ) = glResponseContinue )then
    SELECT COUNT(trx.sourceid) INTO i_rows_processed FROM S_NJSTRANSACTION trx
     WHERE pisoasofdate  = trx.isoasofdate
          AND (trx.sourceId = dataSource or dataSource = ' ')
          and trx.reversalflag = 1;
    LOGENDNJSPROCESS_LOG2( PISOASOFDATE,(STEP)*100+1,I_ROWS_PROCESSED,'Possible Insert Reversal Transactions ',PTARGETTABLE => 'NJSTRANSACTION',
    POPERATION => 'SELECT COUNT' );
    commit;
    end if;
    -- step 2.2 
    i_rows_processed:=0;
      if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => (step)*100+2,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJSTRANSACTIONMAPPER',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'Insert Transactions mapper ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = glResponseContinue )then
   INSERT /*+ APPEND*/
     INTO S_NJSTRANSACTIONMAPPER
    (
      sourceid        ,
      isoasofdate     ,
      externalid      ,
      externalupddate ,
      NJTRANSACTIONID ,
      NJHOLDINGID     ,
      processStatusBF ,
      updcandidate    ,
      inscandidate    ,
      extrenalHoldingID,
      reversalFlag,
      sourceHeadId,
      headTradeDate,
      SEQUENCENUM,
      errorcode
    )
     (SELECT trx.sourceid         ,
        trx.isoasofdate          ,
        trx.externalid           ,
        trx.sysupdatedate        ,
        hextoraw(sys_guid())     ,
        holdingmapper.NJHOLDINGID,
        -- Not reversal - can be used as HEAD data source immedeate
        DECODE(TRX.REVERSALFLAG,  	0,  	BITOR(1,2),
                                    1,    1,
                                    2,    1),
        0                        ,
        1                        ,
        trx.HOLDINGID,
        trx.reversalFlag,
        trx.ORIGINALTRANSACTIONID,
        trx.tradedate,
        nvl(trx.SEQUENCENUM,0),
        0
         FROM S_NJSTRANSACTION trx
      inner join S_NJSPOSITIONHOLDINGMAPPER HOLDINGMAPPER
           on TRX.HOLDINGID = HOLDINGMAPPER.EXTERNALID
           and TRX.SOURCEID      = HOLDINGMAPPER.SOURCEID
      -- verify currency , Transaction Type
      -- hodalble
      inner join NJHOLDABLE HOLDABLE
          on HOLDINGMAPPER.NJHOLDABLEID = HOLDABLE.id
      inner join NJTRANSACTIONTYPE TRXTYPE
        on TRX.TRANSACTIONTYPE= TRXTYPE.SYMBOL
      inner join NJCURRENCY CRNCY
         on (TRX.CURRENCY) = (CRNCY.SYMBOL)
      inner join NJCURRENCY CRNCY4TRX
          on (TRX.TRANSACTIONCURRENCY) = (CRNCY4TRX.SYMBOL)
      where PISOASOFDATE  = TRX.ISOASOFDATE
           and (TRX.SOURCEID = DATASOURCE or DATASOURCE = ' ')   
    ) ;
  I_ROWS_PROCESSED :=sql%ROWCOUNT;
  LOGENDNJSPROCESS_LOG2( PISOASOFDATE,(STEP)*100+2,I_ROWS_PROCESSED,'Insert Transactions mapper ',PTARGETTABLE => 'NJSTRANSACTIONMAPPER',
    POPERATION => 'INSERT' );
  commit;
  end if;
  -- headsourceid -- must be correct or pointing to the transaction with the correcton

  -- Update head of the chain of high transaction
  -- assumption: all transaction in the mapper - sourceHeadId is correct
  -- We will detect those by checking BF on the loaded transaction mapper transactions
  -- Order for Sequence number order of for a mapper tables
  -- show err
  i_rows_processed:=0;
      if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => (step)*100+3,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJSTRANSACTIONMAPPER',
    POPERATION => 'UPDATE',
    PCUSTOMMESSAGE => 'Update HEAD SOURCEID, based on existing data in the mapper ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'Y'
  ) = glResponseContinue )then
  update S_NJSTRANSACTIONMAPPER TRXMAPPER
  set (TRXMAPPER.sourceHeadId, TRXMAPPER.processStatusBF, TRXMAPPER.headTradeDate)= 
      (select TRX2.sourceHeadId, bitor(TRXMAPPER.processStatusBF,2), TRX2.headTradeDate
        from S_NJSTRANSACTIONMAPPER TRX2
        where TRXMAPPER.sourceId = TRX2.sourceid
        and TRXMAPPER.sourceHeadId = TRX2.externalid
        -- current transaction is reversal
        and TRXMAPPER.reversalFlag = 1
        -- Pervious transactiuon must point to update head
        and (bitand(TRX2.processStatusBF,2) = 2 or TRX2.reversalFlag = 0)
        -- current HeadSourceid is not updated
        and bitand(TRXMAPPER.processStatusBF,2) = 0
        -- only current load
        and TRXMAPPER.isoasofdate = pisoasofdate
        AND (TRXMAPPER.sourceId = dataSource or dataSource = ' ')
        -- Only first row, in case we have both - original and previous reversal 
        AND rownum = 1
        ) 
        where TRXMAPPER.NJTRANSACTIONID 
        in (select SUBMAPPER.NJTRANSACTIONID 
        from S_NJSTRANSACTIONMAPPER SUBMAPPER
        inner join S_NJSTRANSACTIONMAPPER TRX3
        on SUBMAPPER.sourceid = TRX3.sourceid
        and SUBMAPPER.sourceHeadId = TRX3.externalid
        -- current transaction is reversal
        and SUBMAPPER.reversalFlag = 1
        -- previous HEADSOURCEID is converted
        and bitand(TRX3.processStatusBF,2) = 2
        -- CURRENT is NOT CONVERTED
        and bitand(SUBMAPPER.processStatusBF,2) = 0
        -- LOADED current load
         where SUBMAPPER.isoasofdate = pisoasofdate
        AND (SUBMAPPER.sourceId = dataSource or dataSource = ' '));
   
   I_ROWS_PROCESSED:=sql%ROWCOUNT;
   
  LOGENDNJSPROCESS_LOG2( PISOASOFDATE,(STEP)*100+3,I_ROWS_PROCESSED,'Insert Transactions mapper ',PTARGETTABLE => 'NJSTRANSACTIONMAPPER',
    POPERATION => 'UPDATE' );
  commit;
  end if;
  
  -- update mapper for the data, where previous HEAD sourceid is not among converted before.
  -- bitand(SUBMAPPER.processStatusBF,2) =0
   if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => (step)*100+4,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJSTRANSACTIONMAPPER',
    POPERATION => 'UPDATE',
    PCUSTOMMESSAGE => 'Update HEAD SOURCEID, based on staging TRX DATA  ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'Y'
  ) = glResponseContinue )then
  update S_NJSTRANSACTIONMAPPER TRXMAPPER
  set (TRXMAPPER.sourceHeadId, TRXMAPPER.processStatusBF,TRXMAPPER.headTradeDate)= 
      (select STTRX.ORIGINALTRANSACTIONID, bitor(TRXMAPPER.processStatusBF,2), STTRX.TRADEDATE
        from S_NJSTRANSACTION STTRX
        where TRXMAPPER.sourceId = STTRX.sourceid
        and TRXMAPPER.sourceHeadId = STTRX.externalid
        -- current transaction is reversal
        and STTRX.reversalFlag = 0
        and TRXMAPPER.reversalFlag = 1
        -- current HeadSourceid is not updated
        and bitand(TRXMAPPER.processStatusBF,2) = 0
        -- only current load
        and TRXMAPPER.isoasofdate = pisoasofdate
        AND (TRXMAPPER.sourceId = dataSource or dataSource = ' ')
        -- only reference in the current load
        and STTRX.isoasofdate = pisoasofdate
        AND (STTRX.sourceId = dataSource or dataSource = ' ')
        ) 
        where TRXMAPPER.NJTRANSACTIONID 
        in (select SUBMAPPER.NJTRANSACTIONID 
        from S_NJSTRANSACTION  STTRX3
        inner join S_NJSTRANSACTIONMAPPER SUBMAPPER
        on SUBMAPPER.sourceid = STTRX3.sourceid
        and SUBMAPPER.sourceHeadId = STTRX3.externalid
        -- current transaction is reversal
         and STTRX3.reversalFlag = 0
        and SUBMAPPER.reversalFlag = 1
        -- CURRENT is NOT CONVERTED
        and bitand(SUBMAPPER.processStatusBF,2) = 0
        -- LOADED current load
        where SUBMAPPER.isoasofdate = pisoasofdate
        AND (SUBMAPPER.sourceId = dataSource or dataSource = ' ')
        -- original records is from the current set too
        and STTRX3.isoasofdate = pisoasofdate
        AND (STTRX3.sourceId = dataSource or dataSource = ' '));
  
   i_rows_processed:=sql%rowcount;
  LOGENDNJSPROCESS_LOG2( PISOASOFDATE,(STEP)*100+4,I_ROWS_PROCESSED,'Update HEAD SOURCEID, based on staging TRX DATA',PTARGETTABLE => 'NJSTRANSACTIONMAPPER',
    POPERATION => 'UPDATE' );
  commit;
  end if;
  
  



  logEndNJSProcess(pisoasofdate,step,i_rows_total,'Insert Transactions into mapper:'|| datasource );
  commit;
EXCEPTION
WHEN OTHERS THEN
  --Update njProcess with an error show err
  ROLLBACK;
  logFailNJSProcess(pisoasofdate,step,0,'An error was encountered  CONV Transactions- '|| SQLCODE||' -ERROR- '||SQLERRM||' -full-'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
  commit;
END Conv_TransactionPreProcess;

PROCEDURE Conv_TransactionFinal
(
    pISOasOfDate VARCHAR,
    step NUMBER:=300, dataSource VARCHAR:=' '
  )
AS
i_rows_processed INTEGER := 0;
  i_rows_total     INTEGER := 0;
  nexjsauserid njuser.id%type;
  viewprincipalid njentity.id%type;
  lpartitionid njpartition.id%type;
  nexjsaentityid njentity.id%type;
  contacttypeid njentitytype.id%type;
  companytypeid njentitytype.id%type;
  currentTimeStamp TIMESTAMP ( 3);
BEGIN
 -- This implementation will ignore the nAttempt
  --Step 1.1 assign global properties variables for the major insert show err
   SELECT id
     INTO nexjsauserid
     FROM njuser
    WHERE loginname LIKE 'nexjsa';
   SELECT id INTO viewprincipalid FROM njprincipal WHERE classcode = 'X';
   SELECT id INTO lpartitionid FROM njpartition WHERE classcode = 'S';
   SELECT id INTO nexjsaentityid FROM njentity WHERE lastname$ ='NEXJSA';
   SELECT sys_extract_utc(systimestamp) INTO currentTimeStamp FROM dual;
  -- show err
  --step 1.1
 
  select count(1) 
  into i_rows_total from S_NJSTRANSACTION TRX
  where TRX.isoasofdate = pisoasofdate
  and (TRX.sourceId = dataSource or dataSource = ' ');
  
  i_rows_total :=sql%rowcount;
  logStartNJSProcess ( pisoasofdate,(step),i_rows_total,'Mark   transactions to process: '||datasource ) ;
  commit;
  
  --step 2.1
  -- get the date from headsourceid / externalid
  -- From S_NJSTRANSACTION - staging
  -- Bring trx2 transaction id for 
  -- show err
  i_rows_processed:=0;
  
  if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => (step)*100+1,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJTRANSACTION',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'Insert Transactions Main Insert  ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = GLRESPONSECONTINUE )then
  
   INSERT /*+ APPEND */
     INTO NJTransaction
    (
       id                 ,
      classCode          ,
      transactionTypeId  ,
      transactionNumber  ,
      transactionNumber$,
      holdingId          ,
      status             ,
      adjustedAmount     ,
      accountid          ,
      visible            ,
      entryDate          ,
      tradeDate          ,
      settlementDate     ,
      CURRENCYID         ,
      PRICECURRENCYID    ,
      settlementsource   ,
      TRANSFERTYPE       ,
      price              ,
      quantity           ,
      closingBalance     ,
      exchangeRate       ,
      grossTradeAmount   ,
      netTradeAmount     ,
      bookValue          ,     
      sysCreateTime      ,
      sysCreateUserId    ,
      sysEditTime        ,
      sysEditUserId      ,
      source             ,
      sourceIdentifier   ,
      sourceIdentifier$  ,
      sourceRevIdentifier,
      sourceheadid ,
      isreversal,
      sequencenumber,
      locking            ,
      partitionId
    )
   SELECT /*+ ORDERED*/
  TRXMAPPER.njtransactionid          ,
    holdable.classcode                       ,
    trxType.id                               ,
    trx.transactionNumber                    ,
    upper(trx.transactionNumber)            ,
    holdingmapper.NJHOLDINGID                ,
    'SETTLED'                                ,
    nvl(trx.adjustedAmount,0)                ,
    holdingmapper.njaccountid                ,
    trx.visible                              ,
    trx.entryDate                            ,
    nvl(trx.tradeDate,  TRXMAPPER.headTradeDate)    ,
    trx.settlementDate                       ,
    crncy.id                                 ,
    crncy4trx.id                             ,
    upper(trx.settlementsource )             ,
    trx.TRANSFERTYPE                         ,
    trx.transactionPrice                     ,
    trx.transactionQuantity                  ,
    NULL                                     ,
    trx.exchangeRate                         ,
    trx.grossTradeAmount                     ,
    trx.netTRadeAmount                       ,
    trx.bookValue                            ,
    currentTimeStamp                         ,
    nexjsauserid                             ,
    currentTimeStamp                         ,
    nexjsauserid                             ,
    trx.SOURCEID                             ,
    trx.EXTERNALID                           ,
    upper(trx.EXTERNALID)                    ,
    trx.originalTransactionId                ,
    trxmapper.sourceHeadId         ,
    trxmapper.reversalFlag   ,    
    trx.SEQUENCENUM,
    0                                        ,
    lpartitionid
  FROM S_NJSPOSITIONHOLDINGMAPPER holdingmapper
   INNER JOIN njholdable holdable
       ON holdingmapper.NJHOLDABLEID = holdable.id
  inner join S_NJSTRANSACTIONMAPPER TRXMAPPER
  on trxmapper.extrenalHoldingID = holdingmapper.externalid
  and trxmapper.sourceid = holdingmapper.sourceid
  and bitand (trxmapper.ProcessStatusBF,1)=1
  and (trxmapper.Sourceid = dataSource or datasource=' ')
  and trxmapper.isoasofdate = pisoasofdate
  INNER JOIN S_NJSTRANSACTION trx
  on trx.sourceid = trxmapper.sourceid
  AND trx.externalid   = trxmapper.externalid
  AND trx.reversalflag = trxmapper.reversalflag
  AND trx.isoasofdate = pisoasofdate
  INNER JOIN njTransactionType trxType
       ON trx.transactionType= trxType.Symbol
  INNER JOIN njcurrency crncy
       ON (trx.currency) = (crncy.symbol)
  INNER JOIN njcurrency crncy4trx
       ON (trx.transactioncurrency) = (crncy4trx.symbol);
  
  I_ROWS_PROCESSED :=SQL%ROWCOUNT;
  
  LOGENDNJSPROCESS_LOG2( PISOASOFDATE,(STEP)*100+1,I_ROWS_PROCESSED,'Insert Transactions Main Insert',PTARGETTABLE => 'NJTRANSACTION',
  POPERATION => 'INSERT' );

  COMMIT;
  end if;
  
  if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => (step)*100+2,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJTRANSACTIONXPOST',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'Insert Transactions XPOST  ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = glResponseContinue )then
     INSERT /*+ APPEND */
     INTO NJTransactionxpost
    (
       id,
      poststatus,
      source
    )
   SELECT /*+ ORDERED*/
    TRXMAPPER.njtransactionid,
    'N',
    TRXMAPPER.SOURCEID
  FROM S_NJSPOSITIONHOLDINGMAPPER holdingmapper
  inner join S_NJSTRANSACTIONMAPPER TRXMAPPER
  on trxmapper.extrenalHoldingID = holdingmapper.externalid
  and trxmapper.sourceid = holdingmapper.sourceid
  and bitand (trxmapper.ProcessStatusBF,1)=1
  and (trxmapper.Sourceid = dataSource or datasource=' ')
  and trxmapper.isoasofdate = pisoasofdate;

  I_ROWS_PROCESSED :=sql%ROWCOUNT;
  LOGENDNJSPROCESS_LOG2( PISOASOFDATE,(STEP)*100+2,I_ROWS_PROCESSED,'Insert Transactions XPOST',PTARGETTABLE => 'NJTRANSACTIONXPOST',
  POPERATION => 'INSERT' );

  COMMIT;
  END IF;
  
  
if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => (step)*100+3,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJTRANSACTIONXPROCESS',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'Insert Transactions XPROCESS  ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = glResponseContinue )then
  INSERT /*+ APPEND */
     INTO NJTransactionxprocess
    (
       id,
      PROCESSEDSTATUS,
      source      
    )
   SELECT /*+ ORDERED*/
    trxMapper.njtransactionid,
    DECODE(trxMapper.sourceid,'RPM','N','AAA','D'),
    trxMapper.SOURCEID
  FROM S_NJSPOSITIONHOLDINGMAPPER holdingmapper
  inner join S_NJSTRANSACTIONMAPPER TRXMAPPER
  on trxmapper.extrenalHoldingID = holdingmapper.externalid
  and trxmapper.sourceid = holdingmapper.sourceid
  and bitand(trxmapper.ProcessStatusBF,1)=1
  AND (TRXMAPPER.SOURCEID = DATASOURCE OR DATASOURCE=' ')
  and trxmapper.isoasofdate = pisoasofdate;
  
   I_ROWS_PROCESSED :=SQL%ROWCOUNT;
  
    LOGENDNJSPROCESS_LOG2( PISOASOFDATE,(STEP)*100+3,I_ROWS_PROCESSED,'Insert Transactions XPROCESS',PTARGETTABLE => 'NJTRANSACTIONXPROCESS',
  POPERATION => 'INSERT' );
  COMMIT;
  END IF;
  
  -- Step 4.0 show err
  -- Step 4.1
  -- NJTRANSACTIONXCASH
  i_rows_processed:=0;
  if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => (step)*100+4,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJTRANSACTIONXCASH',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'Insert Transactions Final - CASH  ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = glResponseContinue )then
   INSERT
    /*+ APPEND */
     INTO NJTRANSACTIONXCASH
    (
      ID            ,
      depositsource ,
      efttype       ,
      PARTITIONID
    )
   SELECT
    trxmapper.NJTRANSACTIONID ,
    trx.depositsource      ,
    trx.efttype            ,
    lpartitionid
    FROM S_NJSPOSITIONHOLDINGMAPPER holdingmapper
  inner join S_NJSTRANSACTIONMAPPER trxmapper
  on trxmapper.extrenalHoldingID = holdingmapper.externalid
  and trxmapper.sourceid = holdingmapper.sourceid
  AND holdingmapper.HOLDABLETYPE               ='CASH'
  and bitand (trxmapper.ProcessStatusBF,1)=1
  and (trxmapper.Sourceid = dataSource or datasource=' ')
  INNER JOIN S_NJSTRANSACTION trx
  on trx.sourceid = trxmapper.sourceid
  AND trx.externalid   = trxmapper.externalid
  AND trx.reversalflag = trxmapper.reversalflag
  AND TRX.isoasofdate = pisoasofdate
  WHERE trxmapper.ISOAsOfDate                   = pisoasofdate
  AND trxmapper.inscandidate                      = 1;
  
   I_ROWS_PROCESSED :=SQL%ROWCOUNT;
  
   LOGENDNJSPROCESS_LOG2( PISOASOFDATE,(STEP)*100+4,I_ROWS_PROCESSED,'Insert Transactions Final - CASH',PTARGETTABLE => 'NJTRANSACTIONXCASH',
  POPERATION => 'INSERT' );
  COMMIT;
  END IF;
  
  
  --step 4.1.1
  I_ROWS_PROCESSED:=0;
  if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => (step)*100+5,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJTRANSACTIONX04',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'Insert Transactions Final - Taxes  ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = glResponseContinue )then
   INSERT
    /*+ APPEND */
     INTO NJTRANSACTIONX04
    (
      ID            ,
      GST           ,
      QST           ,
      HST           ,
      PST           ,
      fedtax        ,
      PROVTAX       ,
      NONRESIDENTTAX,
      FOREIGNTAX    ,
      LOCKING,
      PARTITIONID
    )
   SELECT
    trxmapper.NJTRANSACTIONID ,
    trx.GST                ,
    trx.QST                ,
    trx.HST                ,
    trx.PST                ,
    trx.fedtax             ,
    trx.PROVTAX            ,
    trx.nonResTaX          ,
    trx.FOREIGNTAX         ,
    0,
    lpartitionid
    FROM S_NJSPOSITIONHOLDINGMAPPER holdingmapper
  inner join S_NJSTRANSACTIONMAPPER trxmapper
  on trxmapper.extrenalHoldingID = holdingmapper.externalid
  and trxmapper.sourceid = holdingmapper.sourceid
  and bitand (trxmapper.ProcessStatusBF,1)=1
  and (trxmapper.Sourceid = dataSource or datasource=' ')
  INNER JOIN S_NJSTRANSACTION trx
  on trx.sourceid = trxmapper.sourceid
  AND trx.externalid   = trxmapper.externalid
  AND trx.reversalflag = trxmapper.reversalflag
  AND TRX.isoasofdate = pisoasofdate
  WHERE trxmapper.ISOAsOfDate                   = pisoasofdate
  AND trxmapper.inscandidate                      = 1;
  
   I_ROWS_PROCESSED :=SQL%ROWCOUNT;
   
    LOGENDNJSPROCESS_LOG2( PISOASOFDATE,(STEP)*100+5,I_ROWS_PROCESSED,'Insert Transactions Final - Taxes',PTARGETTABLE => 'NJTRANSACTIONX04',
  POPERATION => 'INSERT' );
  COMMIT;
  END IF;
  
  
  
    --step 4.1.2
  i_rows_processed:=0;
   if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => (step)*100+6,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJTRANSACTIONX03',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'Insert Transactions Final - Wiring Order  ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = glResponseContinue )then
   INSERT
    /*+ APPEND */
     INTO NJTRANSACTIONX03
    (
      ID            ,
      COMMISSIONAMOUNT,
      COMMISSIONPERCENT ,
      REDEMPTIONFEES ,
      TRANSFERCOMPANY ,
      TRANSFERSOURCE ,
      WIREORDERNUMBER ,
      WIREORDERNUMBER$,
      LOCKING,
      PARTITIONID
    )
   SELECT
    trxmapper.NJTRANSACTIONID ,
    TRX.COMMISSIONAMOUNT                ,
    TRX.COMMISSIONPERCENTAGE                 ,
    TRX.REDEMPTIONFEES                ,
    TRX.TRANSFERCOMPANY                ,
    TRX.TRANSFERSOURCE             ,
    TRX.WIREORDERNUMBER            ,
    upper(trx.WIREORDERNUMBER),
    0,
    lpartitionid
    FROM S_NJSPOSITIONHOLDINGMAPPER holdingmapper
  inner join S_NJSTRANSACTIONMAPPER trxmapper
  on trxmapper.extrenalHoldingID = holdingmapper.externalid
  and trxmapper.sourceid = holdingmapper.sourceid
  and bitand (trxmapper.ProcessStatusBF,1)=1
  and (trxmapper.Sourceid = dataSource or datasource=' ')
  INNER JOIN S_NJSTRANSACTION TRX
  on TRX.sourceid = trxmapper.sourceid
  AND TRX.externalid   = trxmapper.externalid
  AND TRX.reversalflag = trxmapper.reversalflag
  AND TRX.isoasofdate = pisoasofdate
  WHERE trxmapper.ISOAsOfDate                   = pisoasofdate
  and (TRX.COMMISSIONAMOUNT is not null 
  or TRX.COMMISSIONPERCENTAGE  is not null
  or TRX.REDEMPTIONFEES is not null
  or TRX.TRANSFERCOMPANY is not null
  or TRX.TRANSFERSOURCE is not null
  or TRX.WIREORDERNUMBER is not null)
  AND trxmapper.inscandidate                      = 1;
  
   I_ROWS_PROCESSED :=SQL%ROWCOUNT;
   
      LOGENDNJSPROCESS_LOG2( PISOASOFDATE,(STEP)*100+6,I_ROWS_PROCESSED,'Insert Transactions Final - Wiring order',PTARGETTABLE => 'NJTRANSACTIONX03',
  POPERATION => 'INSERT' );
  COMMIT;
  END IF;
 
  
  -- Step 4.2
  -- TransactionXEquity show err
  I_ROWS_PROCESSED:=0;
   if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => (step)*100+7,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJTRANSACTIONXEQUITY',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'Insert Transactions Final - Equity  ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = glResponseContinue )then
   INSERT
    /*+ APPEND */
     INTO NJTransactionXEquity
    (
      ID           ,
      STOCKEXCHANGE,
      SECFEE       ,
      DEALERPRICE  ,
      PARTITIONID
    )
   SELECT 
   trxmapper.NJTRANSACTIONID ,
    trx.stockExchange            ,
    trx.secFee                   ,
    trx.dealerprice              ,
    lpartitionid
  FROM S_NJSPOSITIONHOLDINGMAPPER holdingmapper
  inner join S_NJSTRANSACTIONMAPPER trxmapper
  on trxmapper.extrenalHoldingID = holdingmapper.externalid
  and trxmapper.sourceid = holdingmapper.sourceid
  AND holdingmapper.HOLDABLETYPE               = 'EQ'
   and bitand (trxmapper.ProcessStatusBF,1)=1
  and (trxmapper.Sourceid = dataSource or datasource=' ')
  INNER JOIN S_NJSTRANSACTION trx
  on trx.sourceid = trxmapper.sourceid
  AND trx.externalid   = trxmapper.externalid
  AND trx.reversalflag = trxmapper.reversalflag
  AND TRX.isoasofdate = pisoasofdate
  WHERE trxmapper.ISOAsOfDate                   = pisoasofdate
  AND trxmapper.inscandidate                      = 1;
  i_rows_processed :=sql%rowcount;
      LOGENDNJSPROCESS_LOG2( PISOASOFDATE,(STEP)*100+7,I_ROWS_PROCESSED,'Insert Transactions Final - Equity',PTARGETTABLE => 'NJTRANSACTIONXEQUITY',
  POPERATION => 'INSERT' );
  commit;
  END IF;
  -- Step 4.3
  --TRANSACTIONXFIXED
  I_ROWS_PROCESSED:=0;
   if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => (step)*100+8,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJTRANSACTIONXFIXED',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'Insert Transactions Final - FIXED  ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = glResponseContinue )then
   INSERT
    /*+ APPEND */
     INTO NJTRANSACTIONXFIXED
    (
      ID             ,
      ACCRUEDINTEREST,
      CLIENTPRICE    ,
      DEALERPRICE    ,
      PARTITIONID
    )
   SELECT 
   trxmapper.NJTRANSACTIONID ,
    trx.ACCRUEDINTEREST          ,
    trx.CLIENTPRICE              ,
    trx.dealerprice              ,
    lpartitionid
    FROM S_NJSPOSITIONHOLDINGMAPPER holdingmapper
  inner join S_NJSTRANSACTIONMAPPER trxmapper
  on trxmapper.extrenalHoldingID = holdingmapper.externalid
  and trxmapper.sourceid = holdingmapper.sourceid
  AND holdingmapper.HOLDABLETYPE                    ='FIX'
   and bitand (trxmapper.ProcessStatusBF,1)=1
  and (trxmapper.Sourceid = dataSource or datasource=' ')
  INNER JOIN S_NJSTRANSACTION trx
  on trx.sourceid = trxmapper.sourceid
  AND trx.externalid   = trxmapper.externalid
  AND trx.reversalflag = trxmapper.reversalflag
  AND TRX.isoasofdate = pisoasofdate
  WHERE trxmapper.ISOAsOfDate                   = pisoasofdate
  AND TRXMAPPER.INSCANDIDATE                      = 1 ;
   
  I_ROWS_PROCESSED :=SQL%ROWCOUNT;
  
  LOGENDNJSPROCESS_LOG2( PISOASOFDATE,(STEP)*100+8,I_ROWS_PROCESSED,'Insert Transactions Final - FIXED',PTARGETTABLE => 'NJTRANSACTIONXFIXED',
  POPERATION => 'INSERT' );
  COMMIT;
  END IF;
  -- Step 4.4
  --TransactionXFund
  -- show err
  I_ROWS_PROCESSED:=0;
   if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => (step)*100+9,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJTRANSACTIONXFUND',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'Insert Transactions Final - Funds  ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = glResponseContinue )then
   INSERT
    /*+ APPEND */
     INTO NJTransactionXFund
    (
      ID          ,
      TAXABLEEVENT,
      PARTITIONID
    )
   SELECT    
   trxmapper.NJTRANSACTIONID  ,
    trx.TAXABLEEVENT ,
    lpartitionid
  FROM S_NJSPOSITIONHOLDINGMAPPER holdingmapper
  inner join S_NJSTRANSACTIONMAPPER trxmapper
  on trxmapper.extrenalHoldingID = holdingmapper.externalid
  and trxmapper.sourceid = holdingmapper.sourceid
  AND holdingmapper.HOLDABLETYPE                   = 'FUND' 
   and bitand (trxmapper.ProcessStatusBF,1)=1
  and (trxmapper.Sourceid = dataSource or datasource=' ')
  INNER JOIN S_NJSTRANSACTION trx
  on trx.sourceid = trxmapper.sourceid
  AND trx.externalid   = trxmapper.externalid
  AND trx.reversalflag = trxmapper.reversalflag
  AND TRX.isoasofdate = pisoasofdate
  WHERE trxmapper.ISOAsOfDate                   = pisoasofdate
  AND TRXMAPPER.INSCANDIDATE                      = 1;
  
    I_ROWS_PROCESSED :=SQL%ROWCOUNT;
      LOGENDNJSPROCESS_LOG2( PISOASOFDATE,(STEP)*100+9,I_ROWS_PROCESSED,'Insert Transactions Final - Fund',PTARGETTABLE => 'NJTRANSACTIONXFUND',
  POPERATION => 'INSERT' );
  COMMIT;
  END IF;
  
  -- step 4.5
  -- TransactionXTerm
  I_ROWS_PROCESSED:=0;
   if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => (step)*100+10,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJTRANSACTIONXTERM',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'Insert Transactions Final - Term  ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = glResponseContinue )then
   INSERT
    /*+ APPEND */
     INTO NJTransactionXTerm
    (
      ID             ,
      ACCRUEDINTEREST,
      CLIENTPRICE    ,
      PARTITIONID
    )
   SELECT 
   trxmapper.NJTRANSACTIONID ,
    trx.ACCRUEDINTEREST          ,
    trx.CLIENTPRICE              ,
    lpartitionid
  FROM S_NJSPOSITIONHOLDINGMAPPER holdingmapper
  inner join S_NJSTRANSACTIONMAPPER trxmapper
  on trxmapper.extrenalHoldingID = holdingmapper.externalid
  and trxmapper.sourceid = holdingmapper.sourceid
  AND holdingmapper.HOLDABLETYPE                          = 'TERM' 
   and bitand (trxmapper.ProcessStatusBF,1)=1
  and (trxmapper.Sourceid = dataSource or datasource=' ')
  INNER JOIN S_NJSTRANSACTION trx
  on trx.sourceid = trxmapper.sourceid
  AND trx.externalid   = trxmapper.externalid
  AND trx.reversalflag = trxmapper.reversalflag
  AND TRX.isoasofdate = pisoasofdate
  WHERE trxmapper.ISOAsOfDate                   = pisoasofdate
  AND trxmapper.inscandidate                      = 1 ;
  
  I_ROWS_PROCESSED :=SQL%ROWCOUNT;

    LOGENDNJSPROCESS_LOG2( PISOASOFDATE,(STEP)*100+10,I_ROWS_PROCESSED,'Insert Transactions Final - Term',PTARGETTABLE => 'NJTRANSACTIONXTERM',
  POPERATION => 'INSERT' );
  COMMIT;
  END IF;
  
  logEndNJSProcess ( pisoasofdate,(step),i_rows_total,'Insert Transactions Final: '||datasource);
  commit;
EXCEPTION
WHEN OTHERS THEN
  --Update njProcess with an error show err
  ROLLBACK;
  logFailNJSProcess ( pisoasofdate,(step),0,'An error was encountered [300] CONV Transactions- '|| SQLCODE||' -ERROR- '||SQLERRM||' -full-'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE ) ;
  commit;
END Conv_TransactionFinal;


PROCEDURE Conv_TransactionReversal
  (
    pISOasOfDate VARCHAR,
    step NUMBER:=312, dataSource VARCHAR:=' '
  )
AS
  i_rows_processed INTEGER := 0;
  i_rows_total     INTEGER := 0;
  nexjsauserid njuser.id%type;
  viewprincipalid njentity.id%type;
  lpartitionid njpartition.id%type;
  nexjsaentityid njentity.id%type;
  contacttypeid njentitytype.id%type;
  companytypeid njentitytype.id%type;
  currentTimeStamp TIMESTAMP ( 3);
  
  -- cursor declaration 
  -- brings original transaction, based on reversal head id
  
   cursor C_NJORIGINALTRXID is SELECT NX02.rowid, REVMAPPER.NJTRANSACTIONID NJORIGTRXID
   from  S_NJSTRANSACTIONMAPPER TRXMAPPER
   inner join NJTRANSACTIONX02 NX02
    on NX02.id = TRXMAPPER.NJTRANSACTIONID
   inner join S_NJSTRANSACTIONMAPPER REVMAPPER
    on REVMAPPER.EXTERNALID = TRXMAPPER.SOURCEHEADID
    and REVMAPPER.sourceid = TRXMAPPER.SOURCEID
    and bitand(REVMAPPER.processStatusBF,2) = 2
   where  TRXMAPPER.inscandidate    = 1
      AND TRXMAPPER.ISOAsOfDate         = pIsoasofDAte
      and bitand (TRXMAPPER.ProcessStatusBF,1)=1
      and (TRXMAPPER.sourceId      = dataSource
      OR dataSource        = ' ')
      and TRXMAPPER.reversalFlag >0 
      -- must be non-reversal
      and REVMAPPER.reversalFlag =0 
      -- only when correction is needed
      and NX02.id = NX02.originaltransactionid
    -- add if need separate handle for complex reversal, still have multple records
      and (TRXMAPPER.reversalFlag =1 or revMapper.SEQUENCENUM = (select max(SEQUENCENUM) from S_NJSTRANSACTIONMAPPER rev3
          where rev3.externalid = TRXMAPPER.SOURCEHEADID
        and rev3.reversalFlag = 0
        and rev3.sourceid = dataSource));
   
BEGIN
  -- show err
  
  
  
     SELECT id
     INTO nexjsauserid
     FROM njuser
    WHERE loginname LIKE 'nexjsa';
   SELECT id INTO viewprincipalid FROM njprincipal WHERE classcode = 'X';
   SELECT id INTO lpartitionid FROM njpartition WHERE classcode = 'S';
   SELECT id INTO nexjsaentityid FROM njentity  WHERE lastname$ ='NEXJSA';
   SELECT sys_extract_utc(systimestamp) INTO currentTimeStamp FROM dual;
   
   -- step 2.1 - count candidates 
   SELECT COUNT(trx.sourceid) INTO i_rows_total FROM S_NJSTRANSACTION trx
     WHERE pisoasofdate  = trx.isoasofdate
          AND (trx.sourceId = dataSource or dataSource = ' ')
          and trx.reversalflag >0;
    logStartNJSProcess ( pisoasofdate,step,i_rows_total,'Possible Insert Reversal Transactions' ) ;
    commit;
    
    -- first step inserts date with Demi originaltransactionid
    -- Step 3.1 insert reverse transaction information into njtransactionx02 show err
  I_ROWS_PROCESSED:=0;
   if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => (step)*100+1,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJTRANSACTIONX02',
    POPERATION => 'INSERT',
    PCUSTOMMESSAGE => 'Insert Transactions Final - Reversals  ',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'N'
  ) = glResponseContinue )then
   INSERT
     INTO njtransactionx02
    (
      id                   ,
      reversaldate         ,
      originaltransactionid,
      partitionid
    )
   SELECT 
    TRXMAPPER.njtransactionid                          ,
    TRX.reversaldate                                         ,
    TRXMAPPER.njtransactionid                          ,
    --NVL(revmapper.njtransactionid,trxmapper.njtransactionid) ,
    lpartitionid
     FROM  S_NJSTRANSACTIONMAPPER TRXMAPPER
    inner join S_NJSTRANSACTION TRX
      ON TRX.sourceid = TRXMAPPER.sourceid
      and TRXMAPPER.isoasofdate = pisoasofdate
      AND TRX.externalid   = TRXMAPPER.externalid
      AND TRX.reversalflag = TRXMAPPER.reversalflag
      AND TRX.isoasofdate = pisoasofdate
      -- to assure NOTHING going in TRXX02 which is not part of TRX
    INNER JOIN njTransactionType trxType
       ON trx.transactionType= trxType.Symbol
    INNER JOIN njcurrency crncy
       ON trx.currency = crncy.symbol
    INNER JOIN njcurrency crncy4trx
       ON (trx.transactioncurrency) = (crncy4trx.symbol)
    WHERE TRXMAPPER.inscandidate    = 1
      AND TRXMAPPER.ISOAsOfDate         = pIsoasofDAte
      and bitand (TRXMAPPER.ProcessStatusBF,1)=1
      and (TRXMAPPER.sourceId      = dataSource
      OR dataSource        = ' ')
      and TRXMAPPER.reversalFlag >0 
      and TRX.reversalFlag   >0
      and (TRX.sourceId      = dataSource
      OR dataSource        = ' ');
  I_ROWS_PROCESSED :=sql%ROWCOUNT;
  
      LOGENDNJSPROCESS_LOG2( PISOASOFDATE,(STEP)*100+1,I_ROWS_PROCESSED,'Insert Transactions Final - Reversals',PTARGETTABLE => 'NJTRANSACTIONX02',
  POPERATION => 'INSERT' );
  commit;
  END IF;
    
  -- update Direct Scenario from cursor
  
  
   -- Update Complex Scenario from the cursor
    if (FNLOGSTARTNJSPROCESS(
    PISOASOFDATE => PISOASOFDATE,
    PROCESSID => (step)*100+2,
    PAFFECTEDRECORDS => GLTOTALUNKNOWN,
    PTARGETTABLE => 'NJTRANSACTIONX02',
    POPERATION => 'UPDATE',
    PCUSTOMMESSAGE => 'Update Transactions Reversal Chains',
    EXECUTIONOVERRIDEFAILED => 'Y',
    EXECUTIONOVERRIDECOMPLETED => 'Y'
  ) = glResponseContinue )then
   i_rows_processed :=0;
      for c1 in C_NJORIGINALTRXID 
      loop
        update NJTRANSACTIONX02 NX02
        set originaltransactionid = C1.NJORIGTRXID
        where NX02.rowid = c1.rowid;
        i_rows_processed := i_rows_processed+1;
      end loop;
  
      LOGENDNJSPROCESS_LOG2( PISOASOFDATE,(STEP)*100+2,I_ROWS_PROCESSED,'Update Transactions Reversal Chains',PTARGETTABLE => 'NJTRANSACTIONX02',
  POPERATION => 'UPDATE' );
  commit;
  END IF;
  
    logEndNJSProcess ( pisoasofdate,step,i_rows_total,'Possible Insert Reversal Transactions' ) ;
    commit;
  
EXCEPTION
WHEN OTHERS THEN
  --Update njProcess with an error show err
  ROLLBACK;
  logFailNJSProcess ( pisoasofdate,(step),0,'An error was encountered [312] CONV Transactions Reversals- '|| SQLCODE||' -ERROR- '||SQLERRM||' -full-'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE ) ;
  commit;
END Conv_TransactionReversal;

END NJSConversionTransaction;