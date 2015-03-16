--$Id: fx_GET_NEW_LNAME.sql 55570 2011-04-04 17:18:46Z dstelmakh $

--this function will always return the same random name for the same existing name

if object_id('dbo.fx_GET_NEW_LNAME') is not null drop function dbo.fx_GET_NEW_LNAME
go
create function dbo.fx_GET_NEW_LNAME(
				@existing_name	varchar(100),
				@max_candidates	int)			--total # of candidates from the random selection repository (need for performance optimization purposes)

returns varchar(100)

begin

declare	@name	varchar(100)

if ltrim(rtrim(isnull(@existing_name,''))) = ''		--if value is not set we will not modify it
	return(@existing_name)

select	@name = ''

select	@name = LNAME from X_LNAMES where SEQ_ID = (abs(checksum(@existing_name)%@max_candidates) + 1)

if @name = ''
	select	@name = 'NAME NOT FOUND'			--this can happen if id we generated is out of range or table id's out of sequence

return(@name)

end
go

exec mt_RM_VT 'fx_GET_NEW_LNAME', '$Id: fx_GET_NEW_LNAME.sql 55570 2011-04-04 17:18:46Z dstelmakh $'
go

/*
	select	count(*) from X_LNAMES
	
	select	top 100
			IVR_PRIM_LNAME, dbo.fx_GET_NEW_LNAME(IVR_PRIM_LNAME, 62379)
	from	IVR	
*/

