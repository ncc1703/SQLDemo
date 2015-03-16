--ATTENTION: This script has been automatically generated. Do not modify it directly.
--X2S (XML2SQL) transformation script version: 13.5
--$Id: tX_FNAMES_F.sql 99066 2013-11-21 18:31:12Z dstelmakh $
--XML data version: 1 Mar 20 2011

set nocount on 

if object_id('X_FNAMES_F') is not null drop table dbo.X_FNAMES_F
go
if object_id('X_FNAMES_F') is null 
create table dbo.X_FNAMES_F(
	[SEQ_ID] int identity(1, 1)  not null,
	[FNAME] varchar(100)  not null)
go
go
if object_id('PK_X_FNAMES_F') is null 
	alter table X_FNAMES_F add constraint PK_X_FNAMES_F primary key nonclustered ([SEQ_ID])
go

exec mt_RM_VT 'X_FNAMES_F', '$Id: tX_FNAMES_F.sql 99066 2013-11-21 18:31:12Z dstelmakh $'
go
set noexec off
