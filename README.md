# DMPonline-dev

The files are used to download Data Management Plans from DMP Online (http://dmponline.vu.nl) as JSON files and also convert and keep a CSV version.
DMP Online is a tool from the DCC in England that allows the creation of structured Data Management Plans based on templates.

The tool can be approached to download data through two different API's: version 0 and version 1.
For each API there is a specific Python program. A separate version "new" was made in case you wish to log (un)succesful runs.

The Config_template file is an example file that is needed to provide credentials to allow access and download the data.

The day_job file is scheduled to run each day to download DMP json files that have changed. 

The API confog files have each a specific separate branch with changes made to add a logging option to catch connection errors to DMP Online servers.

The two DMP_stats_v1.1 and DMP_stats_v1.2 scripts should be used together and run in that order. The files parse json files originally downloaded from DMP Online using the Dayjob script. Both files together generate a single csv file that should present an overview of research involving sensitive data. The original file (1.1) uses to specific VU DMP templates as a basis for processing downloaded Json files. In 2023 both main VU DMP templates were adjusted to more easily add a faculty name/abbrevation as the new templates contain options (questions) that avoid messy manually added information. Both scripts use both API scripts to make sure they have the latest available online versions of the information in the Data Management Plans.
