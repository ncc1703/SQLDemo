--$Id: fx_OBFUSCATE_NUM.sql 80771 2013-01-22 06:07:35Z skale $
if object_id('dbo.fx_OBFUSCATE_NUM') is not null drop function dbo.fx_OBFUSCATE_NUM
go
create function dbo.fx_OBFUSCATE_NUM(@original varchar(max), @max_len int)
returns varchar(max)
begin

declare 
		@scrambled		varchar(max), 
		@len			int, 
		@padding		varchar(20)

if ltrim(rtrim(isnull(@original,''))) = ''					--if value is not set we will not modify it
	return(@original)

if datalength(@original) = 1 and ascii(@original) = 0		--if string termination #0 character in the field (looks like space in the output)
	return(@original)

set	@original = left(@original, @max_len - 2)

set @len = len(@original)

set @scrambled = reverse(@original)

set @padding = cast(@len * 4 as varchar(20))

if len(@padding) < 2
	set @padding = @padding + '0'

if len(@padding) = 2
begin
	set @scrambled = substring(@padding, 1, 1) + @scrambled +
		substring(@padding, 2, 1)
end

return(@scrambled)
end
go

exec mt_RM_VT 'fx_OBFUSCATE_NUM', '$Id: fx_OBFUSCATE_NUM.sql 80771 2013-01-22 06:07:35Z skale $'
go

/*
	select	top 10000
			MGT_NUM, dbo.fx_OBFUSCATE_NUM(MGT_NUM),
			TRX_WO_NUM, dbo.fx_OBFUSCATE_NUM(TRX_WO_NUM)
	from	TRX
	where	isnull(MGT_NUM,'') <> '' and TRX_WO_NUM <> 0
	
	select	dbo.fx_OBFUSCATE_NUM('3100077668-48045262', 20) 
	
	
*/
