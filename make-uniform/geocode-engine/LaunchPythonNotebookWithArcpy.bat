@echo off
set ANACONDA=C:\Users\dory\AppData\Local\Continuum\Anaconda32
set ESRI_PYTHON=C:\Python27\ArcGIS10.1
set path=%ESRI_PYTHON%;%ANACONDA%;%ANACONDA%\Scripts
set PYTHONPATH=%ANACONDA%\Lib\site-packages;%ESRI_PYTHON%\Lib\site-packages\;%ESRI_PYTHON%\Lib;%ANACONDA%\Lib;%pythonpath%
start %ESRI_PYTHON%\python.exe -c "import sys; from IPython.html.notebookapp import launch_new_instance; sys.exit(launch_new_instance())" %*
exit /B %ERRORLEVEL%
