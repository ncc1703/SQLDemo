--ATTENTION: This script has been automatically generated. Do not modify it directly.
--X2S (XML2SQL) transformation script version: 13.5
--$Id: $
--XML data version: 1 Mar 20 2011

set nocount on 

if object_id('X_SIN') is not null drop table dbo.X_SIN
go
if object_id('X_SIN') is null 
create table dbo.X_SIN(
	[SEQ_ID] int identity(1, 1)  not null,
	[RANDOM_SORT] uniqueidentifier  not null default (newid()),
	[SIN_ORIG] varchar(9)  not null,
	[SIN_NEW] varchar(9)  null)
go
go
if object_id('PK_X_SIN') is null 
	alter table X_SIN add constraint PK_X_SIN primary key clustered ([SEQ_ID])
go
if indexproperty(object_id('X_SIN'),'X_SIN_IDX1', 'IndexDepth') is null 
	create nonclustered index X_SIN_IDX1 on X_SIN([SIN_ORIG],[SIN_NEW])
go

exec mt_RM_VT 'X_SIN', '$Id: $'
go
set noexec off
