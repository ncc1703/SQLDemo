--$Id: fx_GET_NEW_NUM.sql 55570 2011-04-04 17:18:46Z dstelmakh $

--this function will always return the same random name for the same existing name

if object_id('dbo.fx_GET_NEW_NUM') is not null drop function dbo.fx_GET_NEW_NUM
go
create function dbo.fx_GET_NEW_NUM(
				@existing_value		varchar(200))
				
returns varchar(100)

begin

declare	
		@len			int,
		@max_value		bigint,
		@name	varchar(100)

if ltrim(rtrim(isnull(@existing_value,''))) = ''		--if value is not set we will not modify it
	return(@existing_value)

select	@len = datalength(convert(varchar(8),@existing_value))			--limit maximum value to prevent arithmetic overflow
select	@max_value = power(10,@len)  - 1

return(abs(checksum(@existing_value)%@max_value) + 1)

end
go

exec mt_RM_VT 'fx_GET_NEW_NUM', '$Id: fx_GET_NEW_NUM.sql 55570 2011-04-04 17:18:46Z dstelmakh $'
go

/*
	select	top 1000 
			ADR_STRT_NUM, dbo.fx_GET_NEW_NUM(ADR_STRT_NUM)
	from	CET_ADR

	select	top 1000 
			TRX_WO_NUM, dbo.fx_GET_NEW_NUM(TRX_WO_NUM)
	from	TRX
	where	TRX_WO_NUM > 100

*/

