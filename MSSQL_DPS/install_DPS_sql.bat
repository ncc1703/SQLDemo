
@echo off

If "%1" == "" goto :manual_page
If "%2" == "" goto :manual_page
If "%3" == "" goto :manual_page
If "%4" == "" goto :manual_page
If "%5" == "" goto :manual_page

echo SQL scripts install process started on %DATE% - %TIME%. >>%5
echo Running on %3.%4 by %1 >>%5

echo tX_MGT_ACT_NUM.sql >>%5 
sqlcmd -b -U %1 -P %2 -S %3  -d %4 -i tX_MGT_ACT_NUM.sql >>%5 
echo tX_CNAMES.sql >>%5 
sqlcmd -b -U %1 -P %2 -S %3  -d %4 -i tX_CNAMES.sql >>%5 
echo tX_FNAMES_ALL.sql >>%5 
sqlcmd -b -U %1 -P %2 -S %3  -d %4 -i tX_FNAMES_ALL.sql >>%5 
echo tX_FNAMES_F.sql >>%5 
sqlcmd -b -U %1 -P %2 -S %3  -d %4 -i tX_FNAMES_F.sql >>%5 
echo tX_FNAMES_M.sql >>%5 
sqlcmd -b -U %1 -P %2 -S %3  -d %4 -i tX_FNAMES_M.sql >>%5 
echo tX_LNAMES.sql >>%5 
sqlcmd -b -U %1 -P %2 -S %3  -d %4 -i tX_LNAMES.sql >>%5 
echo tX_SIN.sql >>%5 
sqlcmd -b -U %1 -P %2 -S %3  -d %4 -i tX_SIN.sql >>%5 
echo fx_GET_NEW_CNAME.sql >>%5 
sqlcmd -b -U %1 -P %2 -S %3  -d %4 -i fx_GET_NEW_CNAME.sql >>%5 
echo fx_GET_NEW_CODE.sql >>%5 
sqlcmd -b -U %1 -P %2 -S %3  -d %4 -i fx_GET_NEW_CODE.sql >>%5 
echo fx_GET_NEW_FNAME.sql >>%5 
sqlcmd -b -U %1 -P %2 -S %3  -d %4 -i fx_GET_NEW_FNAME.sql >>%5 
echo fx_GET_NEW_LABEL.sql >>%5 
sqlcmd -b -U %1 -P %2 -S %3  -d %4 -i fx_GET_NEW_LABEL.sql >>%5 
echo fx_GET_NEW_LNAME.sql >>%5 
sqlcmd -b -U %1 -P %2 -S %3  -d %4 -i fx_GET_NEW_LNAME.sql >>%5 
echo fx_GET_NEW_NUM.sql >>%5 
sqlcmd -b -U %1 -P %2 -S %3  -d %4 -i fx_GET_NEW_NUM.sql >>%5 
echo fx_GET_NEW_SIN.sql >>%5 
sqlcmd -b -U %1 -P %2 -S %3  -d %4 -i fx_GET_NEW_SIN.sql >>%5 
echo fx_GET_SCRAMBLED_VALUE.sql >>%5 
sqlcmd -b -U %1 -P %2 -S %3  -d %4 -i fx_GET_SCRAMBLED_VALUE.sql >>%5 
echo fx_OBFUSCATE_NUM.sql >>%5 
sqlcmd -b -U %1 -P %2 -S %3  -d %4 -i fx_OBFUSCATE_NUM.sql >>%5 

goto end


:manual_page

cls
echo This batch file needs to be run with the following parameters:
echo 1) User
echo 2) Password
echo 3) Server Name 
echo 4) Database Name
echo 5) Output Logfile Name
echo Sample call format: install_DPS_sql.bat username password server_name db_name	install_DPS.log
exit /B %ERRORLEVEL%

:end 
echo SQL Script install process completed on %DATE% - %TIME%. >>%5
exit /B 0
