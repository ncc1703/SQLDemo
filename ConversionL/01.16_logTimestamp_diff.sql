CREATE OR REPLACE FUNCTION timestamp_diff
(
start_time_in TIMESTAMP
, end_time_in TIMESTAMP
)
-- RETURN NUMBER
RETURN VARCHAR 
AS
l_days NUMBER;
l_hours NUMBER;
l_minutes NUMBER;
l_seconds NUMBER;
l_milliseconds NUMBER;
BEGIN
SELECT extract(DAY FROM end_time_in-start_time_in)
, extract(HOUR FROM end_time_in-start_time_in)
, extract(MINUTE FROM end_time_in-start_time_in)
, extract(SECOND FROM end_time_in-start_time_in)
INTO l_days, l_hours, l_minutes, l_seconds
FROM dual;

--l_milliseconds := l_seconds*1000 + l_minutes*60*1000 + l_hours*60*60*1000 + l_days*24*60*60*1000;
--RETURN ' Milliseconds ' || l_milliseconds;

l_milliseconds := (l_seconds - FLOOR(l_seconds) ) * 1000000 ; -- + l_minutes*60*1000 + l_hours*60*60*1000 + l_days*24*60*60*1000;
RETURN 'Days '|| L_DAYS ||' Hours '|| L_HOURS||' Minutes '||L_MINUTES||' Seconds '||FLOOR(L_SECONDS)||' Milliseconds '|| L_MILLISECONDS;
END;