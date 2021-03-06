# citygeo-ago-pipeline

# Intro
Script has evolved heavily and has had a lot of modifications thrown into it, would probably have done it differently now starting from scratch.
Originally written for scheduling updates for all of our layers, had to solve issues with pushing large amount of layers. Evolved to allow for updates to be triggered by script.


# Powershell
Powershell is the driver because it wraps arcpy for multithreading and stability
- Allows us to instantiate multiple arcpy threads
- handle unresponsive arcpy threads and kill them to retry
- timeouts
- higher level logging


# SSH and Jenkins vs windows task scheduler
- forced to use windows over AWS batch or other headless options because arcpy not fully implemented on Linux
- Dealing with licensing and proprietary environment of arcpy

# ago_update.py basic workflow
- APRX files (pro projet files) are required to be manually setup first and published to AGO. They can then be used in the script.

- Sign into portal, create our GIS connection object, pull connection info from our APRX file.
- Search AGO for the feature service and service defintion objects (both necessary) that were originally published, and get the returned AGO item ids. Table names in our data warehouse match the AGO items in our org. You must be using an AGO user that has permissions to view the ago item/layer.
- Run checks:
    - Row counts (missing data)
    - Field comparisons
    - Whether source table is actively being updated
- create SD (service definition) file.
- Publish SD
    - Uses 'update' and 'publish' methods to push to AGO, boilerplate esri code.

# Click flags
