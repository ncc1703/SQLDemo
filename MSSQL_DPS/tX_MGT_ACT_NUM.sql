--ATTENTION: This script has been automatically generated. Do not modify it directly.
--X2S (XML2SQL) transformation script version: 13.5
--$Id: $
--XML data version: 1.0

set nocount on 

if object_id('X_MGT_ACT_NUM') is not null drop table dbo.X_MGT_ACT_NUM
go
if object_id('X_MGT_ACT_NUM') is null 
create table dbo.X_MGT_ACT_NUM(
	[SEQ_ID] int identity(1, 1)  not null,
	[MGT_ACT_NUM_ORIG] varchar(100)  not null,
	[MGT_ACT_NUM_NEW] varchar(100)  null,
	[MAX_LEN] int  not null,
	[RANDOM_SORT] uniqueidentifier  not null default (newid()))
go
go
if object_id('PK_X_ACT_NUM') is null 
	alter table X_MGT_ACT_NUM add constraint PK_X_ACT_NUM primary key clustered ([SEQ_ID])
go
if indexproperty(object_id('X_MGT_ACT_NUM'),'X_MGT_ACT_NUM_IDX1', 'IndexDepth') is null 
	create nonclustered index X_MGT_ACT_NUM_IDX1 on X_MGT_ACT_NUM([MGT_ACT_NUM_ORIG],[MGT_ACT_NUM_NEW])
go

exec mt_RM_VT 'X_MGT_ACT_NUM', '$Id: $'
go
set noexec off
