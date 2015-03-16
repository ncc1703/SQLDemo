--$Id: fx_GET_NEW_LABEL.sql 55570 2011-04-04 17:18:46Z dstelmakh $

--this function will always return the same random name for the same existing name

if object_id('dbo.fx_GET_NEW_LABEL') is not null drop function dbo.fx_GET_NEW_LABEL
go
create function dbo.fx_GET_NEW_LABEL(
				@existing_value		varchar(200),
				@id					int,
				@tag				varchar(50))
				
returns varchar(100)

begin

declare	@name	varchar(100)

if ltrim(rtrim(isnull(@existing_value,''))) = ''		--if value is not set we will not modify it
	return(@existing_value)

return(convert(varchar(15),@id) + ' ' + @tag)

end
go

exec mt_RM_VT 'fx_GET_NEW_LABEL', '$Id: fx_GET_NEW_LABEL.sql 55570 2011-04-04 17:18:46Z dstelmakh $'
go

/*
	select	count(*) from X_LNAMES
	
	select	top 100
			IVR_SYSID, dbo.fx_GET_NEW_LABEL('sadasda', IVR_SYSID, 'Univeris Ltd.')
	from	IVR	
*/

