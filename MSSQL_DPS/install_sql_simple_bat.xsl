<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="text" encoding="iso-8859-1" />
<xsl:template match="/">
 <xsl:text>
@echo off

If "%1" == "" goto :manual_page
If "%2" == "" goto :manual_page
If "%3" == "" goto :manual_page
If "%4" == "" goto :manual_page
If "%5" == "" goto :manual_page

echo SQL scripts install process started on %DATE% - %TIME%. >>%5
echo Running on %3.%4 by %1 >>%5
</xsl:text>
<xsl:text>&#xa;</xsl:text>

<xsl:for-each select="Script_List/Scripts">
<xsl:if test="Run='1'">
<xsl:text>echo </xsl:text><xsl:value-of select="Name"/><xsl:text> >>%5 &#xa;</xsl:text>
<xsl:text>sqlcmd -b -U %1 -P %2 -S %3  -d %4 -i </xsl:text><xsl:value-of select="Name"/> <xsl:text> >>%5 &#xa;</xsl:text>
</xsl:if>
<xsl:if test="Run='0'">
<xsl:text>rem sqlcmd -b -U %1 -P %2 -S %3  -d %4 -i </xsl:text><xsl:value-of select="Name"/> <xsl:text> >>%5 &#xa;</xsl:text>
</xsl:if>
</xsl:for-each>
goto end
<xsl:text>&#xa;</xsl:text>
 <xsl:text>
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
</xsl:text>
 
</xsl:template>
</xsl:stylesheet>