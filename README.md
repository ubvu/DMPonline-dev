# DMPonline-dev

The files are used to download Data Management Plans from DMP Online (http://dmponline.vu.nl) as JSON files and also convert and keep a CSV version.
DMP Online is a tool from the DCC in England that allows the creation of structured Data Management Plans based on templates.

The tool can be approached to download data through two different API's: version 0 and version 1.
For each API there is a specific Python program. A separate version "new" was made in case you wish to log (un)succesful runs.

The Config_template file is an example file that is needed to provide credentials to allow access and download the data.

The day_job file is scheduled to run each day to download DMP json files that have changed. 

The API confog files have each a specific separate branch with changes made to add a logging option to catch connection errors to DMP Online servers.

The two DMP_stats_v1.1 and DMP_stats_v1.2 scripts should be used together and run in that order. The files process json files originally downloaded from DMP Online using the Dayjob script. Both files together generate a single csv file that should present an overview of research involving sensitive data. The original file (1.1) uses two specific VU DMP templates as a basis for processing downloaded Json files. In 2023 both main VU DMP templates were adjusted to more easily add a faculty name/abbrevation as the new templates contain options (questions) that avoid messy manually added information. Both scripts use both API scripts to make sure they have the latest available online versions of the information in the Data Management Plans. The scripts use the following files: faculty_abb.csv, and faculty_names.csv. The Excel file Structure_var_names.xlsx presents short descriptions of the data items that end up in the overview of research involving sensitive data.
In August 2023 some changes were necessary to files 1.1 and 1.2 because the usage of the term "None" became more strict in Python 3.11 and used libraries. I created 1.3 and 1.4 where this term was replaced with the term "Unknown". File 1.3 is the updated version of 1.1 and 1.4 is the update of 1.2. Files 1.3 and 1.4 should be run in this order.

The script DMPs_updates can be used to keep track of changes in Data Management Plans over time using Json files that have been downloaded with the Dayjob script. The script may require changes if used beyond 2023!
