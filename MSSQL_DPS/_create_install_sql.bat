@echo off
echo "Generating installation batch file for DPS module"

set result=install_DPS_sql.bat

if exist %result% del %result%
msxsl4 _install_DPS_sql.seq install_sql_simple_bat.xsl -o %result%

if exist %result% echo SUCCESS: Output file %result% generated successfully

pause