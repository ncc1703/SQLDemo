--$Id: fx_GET_NEW_SIN.sql 80771 2013-01-22 06:07:35Z skale $

--this function will always return the same random name for the same existing name

if object_id('dbo.fx_GET_NEW_SIN') is not null drop function dbo.fx_GET_NEW_SIN
go
create function dbo.fx_GET_NEW_SIN(
				@existing_sin		varchar(9))
				
returns varchar(9)

begin

declare		@new_sin					varchar(9), 
			@scramble_multiplier_int	bigint,
			@multiplied_val				bigint

if ltrim(rtrim(isnull(@existing_sin,''))) = '' or isnumeric(@existing_sin) = 0		--if value is not set we will not modify it
	return(@existing_sin)

if datalength(@existing_sin) = 1 and ascii(@existing_sin) = 0						--if string termination #0 character in the field (looks like space in the output)
	return(@existing_sin)

set	@existing_sin = convert(varchar(9),abs(convert(int,@existing_sin)))						--ensure that if input is a negative # we would convert it to positive
set	@scramble_multiplier_int = 1 + cast(abs(checksum(@existing_sin)) % 99999999 as bigint)

set @multiplied_val = cast(@existing_sin as bigint) * @scramble_multiplier_int

set @new_sin = (
	select SIN_8_DIGIT +
		case
			when TOTAL % 10 = 0 then '0'
			else cast(10 - (TOTAL % 10) as char(10))
		end as SIN
	from (
		select
			SIN_8_DIGIT,
			cast(substring(SIN_8_DIGIT, 1, 1) as int) +
			case
				when cast(substring(SIN_8_DIGIT, 2, 1) as int) * 2 >= 10
					then cast(substring(SIN_8_DIGIT, 2, 1) as int) * 2 - 10 + 1
				else cast(substring(SIN_8_DIGIT, 2, 1) as int) * 2
			end +
			cast(substring(SIN_8_DIGIT, 3, 1) as int) +
			case
				when cast(substring(SIN_8_DIGIT, 4, 1) as int) * 2 >= 10
					then cast(substring(SIN_8_DIGIT, 4, 1) as int) * 2 - 10 + 1
				else cast(substring(SIN_8_DIGIT, 4, 1) as int) * 2
			end +
			cast(substring(SIN_8_DIGIT, 5, 1) as int) +
			case
				when cast(substring(SIN_8_DIGIT, 6, 1) as int) * 2 >= 10
					then cast(substring(SIN_8_DIGIT, 6, 1) as int) * 2 - 10 + 1
				else cast(substring(SIN_8_DIGIT, 6, 1) as int) * 2
			end +
			cast(substring(SIN_8_DIGIT, 7, 1) as int) +
			case
				when cast(substring(SIN_8_DIGIT, 8, 1) as int) * 2 >= 10
					then cast(substring(SIN_8_DIGIT, 8, 1) as int) * 2 - 10 + 1
				else cast(substring(SIN_8_DIGIT, 8, 1) as int) * 2
			end as TOTAL
		from (
			select
				'9' + left(cast(@multiplied_val as varchar(20)), 4) + right(cast(@multiplied_val as varchar(20)), 3)
				as SIN_8_DIGIT
		) as SIN_8_DIGIT
	) as TOTAL
)

return(@new_sin)

end
go

exec mt_RM_VT 'fx_GET_NEW_SIN', '$Id: fx_GET_NEW_SIN.sql 80771 2013-01-22 06:07:35Z skale $'
go

/*
	
	select	top 100
			IVR_PRIM_SIN, dbo.fx_GET_NEW_SIN(IVR_PRIM_SIN)
	from	IVR	
	where	isnull(IVR_PRIM_SIN, '') <> ''
	
	select	dbo.fx_GET_NEW_SIN('-134')	
*/

