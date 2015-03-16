--$Id: fx_GET_NEW_CODE.sql 55570 2011-04-04 17:18:46Z dstelmakh $

--this function will always return the same random name for the same existing name

if object_id('dbo.fx_GET_NEW_CODE') is not null drop function dbo.fx_GET_NEW_CODE
go
create function dbo.fx_GET_NEW_CODE(
				@original		varchar(200))
				
returns varchar(100)

begin

declare @scrambled				varchar(max), 
		@i						int, 
		@char					char(1),
		@replacement_count		int, 
		@position_count			int, 
		@len					int

if ltrim(rtrim(isnull(@original,''))) = ''		--if value is not set we will not modify it
	return(@original)

set @scrambled = @original
set @len = len(@original)
set @i = 3

while @i < @len
begin
	set @char = substring(@original, @i, 1)

	if @char between 'A' and 'Z'
		set @scrambled = 
			substring(@scrambled, 1, @i - 1) +
			char(
				(
					(abs(checksum(@scrambled) % ascii(substring(@scrambled, @i, 1))) + 1)
					% 26
				) + 65) +
			substring(@scrambled, @i + 1, len(@scrambled))
	else if @char between 'a' and 'z'
		set @scrambled = 
			substring(@scrambled, 1, @i - 1) +
			char(
				(
					(abs(checksum(@scrambled) % ascii(substring(@scrambled, @i, 1))) + 1)
					% 26
				) + 97) +
			substring(@scrambled, @i + 1, len(@scrambled))
	else if @char between '0' and '9'
		set @scrambled = 
			substring(@scrambled, 1, @i - 1) +
			char(
				(
					(abs(checksum(@scrambled) % ascii(substring(@scrambled, @i, 1))) + 1)
					% 10
				) + 48) +
			substring(@scrambled, @i + 1, len(@scrambled))

	set @i = @i + 3
end

return(@scrambled)

end
go

exec mt_RM_VT 'fx_GET_NEW_CODE', '$Id: fx_GET_NEW_CODE.sql 55570 2011-04-04 17:18:46Z dstelmakh $'
go

/*
	select	count(*) from X_LNAMES
	
	select	top 100
			IVR_SYSID, dbo.fx_GET_NEW_CODE(IVR_SYSID)
	from	IVR	
*/

