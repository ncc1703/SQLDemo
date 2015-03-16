--$Id: fx_GET_SCRAMBLED_VALUE.sql 80771 2013-01-22 06:07:35Z skale $

--this function will always return the same random name for the same existing name

if object_id('dbo.fx_GET_SCRAMBLED_VALUE') is not null drop function dbo.fx_GET_SCRAMBLED_VALUE
go
create function dbo.fx_GET_SCRAMBLED_VALUE	( 
												@table_name			varchar(100),
												@existing_value		varchar(100),
												@max_len			int = 0
											)
returns varchar(100)

begin

declare		@scrambled_value	varchar(100),
			@sqlstring			nvarchar(4000)
			
	if @table_name  = 'SIN_DATA'  
	begin
		select 	@scrambled_value	=	SCRAMBLED_VALUE
		from  	SIN_DATA 
		where 	ORIGINAL_VALUE		=	@existing_value 
	end

	if @table_name  = 'ACT_NUM_DATA'  
	begin
		select 	@scrambled_value	=	SCRAMBLED_VALUE 
		from  	ACT_NUM_DATA a
		join 	X_ACT_NUM	 x
		on 		a.ORIGINAL_VALUE 	= 	x.ACT_NUM
		and 	a.ORIGINAL_VALUE	=	@existing_value 
		and 	x.MAX_LEN			=	@max_len
	end

	return(@scrambled_value)
	
end
go

exec mt_RM_VT 'fx_GET_SCRAMBLED_VALUE', '$Id: fx_GET_SCRAMBLED_VALUE.sql 80771 2013-01-22 06:07:35Z skale $'
go
