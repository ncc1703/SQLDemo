--$Id: fx_GET_NEW_FNAME.sql 55570 2011-04-04 17:18:46Z dstelmakh $

--this function will always return the same random name for the same existing name

if object_id('dbo.fx_GET_NEW_FNAME') is not null drop function dbo.fx_GET_NEW_FNAME
go
create function dbo.fx_GET_NEW_FNAME(
				@existing_name		varchar(100),
				@gender				char(1),
				@max_candidates		int,			--total # of candidates from the random selection repository (need for performance optimization purposes)
				@max_m_candidates	int,			--total # of male candidate table names
				@max_f_candidates	int)			--total # of female candidate table names
				
returns varchar(100)

begin

declare	@name	varchar(100)

if ltrim(rtrim(isnull(@existing_name,''))) = ''		--if value is not set we will not modify it
	return(@existing_name)

select	@gender = isnull(@gender,''),
		@name = ''
		
if @gender = 'M'
begin
	select	@name = FNAME from X_FNAMES_M where SEQ_ID = (abs(checksum(@existing_name)%@max_m_candidates) + 1)
	goto finalize
end

if @gender = 'F'
begin
	select	@name = FNAME from X_FNAMES_F where SEQ_ID = (abs(checksum(@existing_name)%@max_f_candidates) + 1)
	goto finalize
end

--if @gender is anything else
select	@name = FNAME from X_FNAMES_ALL where SEQ_ID = (abs(checksum(@existing_name)%@max_candidates) + 1)

finalize:

if @name = ''
	select	@name = 'NAME NOT FOUND'			--this can happen if id we generated is out of range or table id's out of sequence

return(@name)

end
go

exec mt_RM_VT 'fx_GET_NEW_FNAME', '$Id: fx_GET_NEW_FNAME.sql 55570 2011-04-04 17:18:46Z dstelmakh $'
go

/*
	select	count(*) from X_FNAMES_ALL
	select	count(*) from X_FNAMES_M
	select	count(*) from X_FNAMES_F

	select	top 100
			IVR_PRIM_FNAME, IVR_MKTG.GENDER_CD, dbo.fx_GET_NEW_FNAME(IVR_PRIM_FNAME, IVR_MKTG.GENDER_CD,1037, 455, 582)
	from	IVR inner join IVR_MKTG on IVR.IVR_SYSID = IVR_MKTG.IVR_SYSID
	where	IVR_MKTG.GENDER_CD in ('M', 'F')
	
	*/

