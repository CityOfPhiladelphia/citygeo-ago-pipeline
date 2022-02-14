# ago_update_multithread

This script will update AGO items with their counterparts in Oracle Databridge. Before you can do this for any particular dataset, the item must first be made by manually by "publishing" the databridge dataset into AGO, which will create two items, a service definition, and then a feature layer. After that this script can be used to automatically update it with changes.

In this folder you'll find .ps1 scripts that will run many of these dataset updates in parallel to complete the update process as fast as possible. Our daily big ones are 'daily.ps1' and 'large-datasets-daily.ps1'. These can also be run manually, either on the machine in a powershell prompt or remotely (ask Alex or Roland.)

There is also a script 'email_summary.bat' that will email a summary email out to the concerned parties, this is scheduled as well.

If you need to update a dataset manually, follow these steps on the citygeo windows script server:

1. Identify the name of the .aprx file of your dataset in the 'aprx_files' folder, it is case sensitive.
2. Figure out which permissions you want to use, choosing the name of one of the 'perms' blocks as shown in the 'config.ini.example' file.
3. Run in a powershell or cmd prompt like so:

```
cd E:\Scripts\ago_update_multithread\
E:\arcpy\python.exe .\ago_update.py -d Zoning_BaseDistricts -o ago -p public_perms
```

4. If you're having issues with the dataset not updating because of field differences between AGO and databridge, or you're getting an error that the difference in total number of rows is too large, you may ignore them and force the update with the -r flag:

```
cd E:\Scripts\ago_update_multithread\
E:\arcpy\python.exe .\ago_update.py -d Zoning_BaseDistricts -o ago -p public_perms -r
```

5. Wait for it to finish.