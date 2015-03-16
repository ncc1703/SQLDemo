CREATE OR REPLACE PACKAGE NJSConversionCRM AS

  PROCEDURE Conv_Contact(pISOasOfDate varchar, step NUMBER:=257, dataSource VARCHAR:=' ');
  PROCEDURE Conv_Telephone(pISOasOfDate varchar, step NUMBER:=263, dataSource VARCHAR:=' ');
  PROCEDURE Conv_Email(pISOasOfDate varchar, step NUMBER:=262, dataSource VARCHAR:=' ');
  PROCEDURE Conv_Address(pISOasOfDate varchar, step NUMBER:=261, dataSource VARCHAR:=' ');
  PROCEDURE Conv_ClientRelationship(pISOasOfDate varchar, step NUMBER:=264, dataSource VARCHAR:=' ');
  
  GLRESPONSECONTINUE CONSTANT NVARCHAR2(2) :='N';
  GLTOTALUNKNOWN CONSTANT number :=-1;
END NJSConversionCRM;

