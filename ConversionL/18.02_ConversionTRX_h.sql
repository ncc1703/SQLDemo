
CREATE or replace PACKAGE NJSConversionTransaction AS
  
  PROCEDURE Conv_TransactionPreProcess(pISOasOfDate varchar, step NUMBER:=524, dataSource VARCHAR:=' ');
  PROCEDURE Conv_TransactionFinal(pISOasOfDate varchar, step NUMBER:=300, dataSource VARCHAR:=' ');
  procedure CONV_TRANSACTIONREVERSAL(PISOASOFDATE varchar, STEP number:=312, DATASOURCE varchar:=' ');
  -- Time limited function - scans daily dose for error and icompatibility
  --procedure CONV_TRANSACTIONSCAN(PISOASOFDATE varchar, STEP number:=700, DATASOURCE varchar:=' ');
  GLRESPONSECONTINUE CONSTANT NVARCHAR2(2) :='N';
  GLTOTALUNKNOWN CONSTANT number :=-1;


END NJSConversionTransaction;


