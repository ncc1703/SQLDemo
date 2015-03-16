--ATTENTION: This script has been automatically generated. Do not modify it directly.
--X2S (XML2SQL) transformation script version: 13.5
--$Id: tX_CNAMES.sql 99066 2013-11-21 18:31:12Z dstelmakh $
--XML data version: V1 Mar 20 2011

set nocount on 

if object_id('X_CNAMES') is not null drop table dbo.X_CNAMES
go
if object_id('X_CNAMES') is null 
create table dbo.X_CNAMES(
	[SEQ_ID] int identity(1, 1)  not null,
	[CNAME] varchar(100)  not null)
go
go
if object_id('PK_X_CNAMES') is null 
	alter table X_CNAMES add constraint PK_X_CNAMES primary key nonclustered ([SEQ_ID])
go

exec mt_RM_VT 'X_CNAMES', '$Id: tX_CNAMES.sql 99066 2013-11-21 18:31:12Z dstelmakh $'
go
set noexec off
