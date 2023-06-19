# DMPonline-dev

The files are used to download Data Management Plans from DMP Online (http://dmponline.vu.nl) as JSON files and also convert and keep a CSV version.
DMP Online is a tool from the DCC in England that allows the creation of structured Data Management Plans based on templates.

The tool can be approached to download data through two different API's: version 0 and version 1.
For each API there is a specific Python program. A separate version "new" was made in case you wish to log (un)succesful runs.

The Config_template file is an example file that is needed to provide credentials to allow access and download the data.

The day_job file is scheduled to run each day to download DMP json files that have changed. 

The API confog files have each a specific separate branch with changes made to add a logging option to catch connection errors to DMP Online servers.
