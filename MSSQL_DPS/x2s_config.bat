@echo off
::This file gives oppotunity to override default locations for LIB, DDL and DOC folders
::Path locations specified here should never be absolute rather it is relative to the path passed to this file as a parameter %1
::Main use case is to support transformation of localized packages which do not have fully defined folder structure around them

:: %1 - path where XML file is located and sets relative point for defining all the other path values. Contains back slash at the end
:: Custom path assignments should have back slash at the end as well

::Uncommend the variable assignments and set them as required

set x2s_lib=%~1..\..\SQL\LIB_X2S\
::echo %x2s_lib%

set x2s_ddl=%~1
::echo %x2s_ddl%

set x2s_doc=%~1
::echo %x2s_doc%