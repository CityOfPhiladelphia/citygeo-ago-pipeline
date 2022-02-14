from datetime import datetime, timedelta, date
import arcpy
import os
import sys
import click
import pickle
from time import sleep,strftime
from arcgis.gis import GIS
from datetime import datetime, date
from configparser import ConfigParser
import shutil
import traceback
from random import random
import cx_Oracle
import traceback
import xml.dom.minidom as DOM

from citygeo_utils import get_logger, prune_logs

arcpy_version = arcpy.GetInstallInfo()['Version']
arcpy_build = arcpy.GetInstallInfo()['BuildNumber']
wanted_version = '2.3.3'
wanted_build = '15850'
if arcpy_version != wanted_version or arcpy_build != wanted_build:
    print('''
        Warning! This script was developed against arcpy version {}, build number {}! You are on version {},
        build {}. ESRI commonly breaks and fixes arcpy functions for various versions without any changelog or
        notice, so if the script doesn't function as expected make sure you have the right arcpro version and
        patches installed. Or upgrade to the latest and try that.
        '''.format(wanted_version, wanted_build, arcpy_version, arcpy_build))


# Contains all variables, methods, and progress indicators for a specific dataset
# So we can more easily track all the necessary vars
class DatasetUploadObj:
    # Run automatically when object initialized, useful if you need to do anything to build up object.
    def __init__(self, name, org, perms, ignore_st_fields, no_log):
        self.name = name
        self.org = org

        self.no_log = no_log

        self.config = ConfigParser()
        self.config.read(script_directory + os.sep + 'config.ini')
        # Set AGO connection options
        self.login = dict(self.config.items(org))
        self.perms = dict(self.config.items(perms))
        self.databridge_creds = dict(self.config.items('databridge_creds'))
        # this will be set to either 'table' or 'layer' once we do an ago search
        self.ago_type = None

        # Initialize here to be updated later in the checks method
        self.db_record_count = False
        self.ago_record_count = False

        # Sign in to default portal using ArcPy to ensure proper licensing for pro
        arcpy.SignInToPortal(self.login['portal'], self.login['user'], self.login['password'])

        # Define the AGO organization to connect to
        self.gis = GIS(self.login['portal'], self.login['user'], self.login['password'], proxy_host=self.login['proxy'], proxy_port=8080)

        self.ignore_st_fields = ignore_st_fields
        self.aprx_file = os.path.join(script_directory, 'aprx_files', name + '.aprx')
        self.sd_file = os.path.join(script_directory, 'sd_files', name + '.sd')
        if not os.path.isfile(self.aprx_file):
            raise AssertionError("Cannot find aprx file {}! Check that the filename case matches your arg".format(self.aprx_file))
        # Progress variables to inform main functions
        self.sd_file_created = False
        self.successfully_published = False

        # fail rate to track how many times we attempted to upload
        self.sd_fail_counter = 0
        self.publish_fail_counter = 0

        # error messages if we need to email them
        self.sd_error_msg = ''
        self.publish_error_msg = ''

        # Set arcpy options
        arcpy.SetLogHistory(False)
        arcpy.env.workspace = os.getcwd()
        arcpy.env.overwriteOutput = True

        self.project = arcpy.mp.ArcGISProject(self.aprx_file)

        self.project_map = self.project.listMaps()[0]

        # The following code will work if it's a map
        try:
            connection_properties = self.project_map.listLayers()[0].connectionProperties
        # The following code will work if it's a table
        except Exception as e:
            connection_properties = self.project_map.listTables()[0].connectionProperties

        # Connection properties has some goodies from the aprx about the datasource.
        # Fully qualified name, example value: "GIS_LNI.LI_CLEAN_SEAL"
        self.dataset = connection_properties['dataset']
        self.datasource_db_user = connection_properties['connection_info']['user']

        # Sometimes dbclient is a key in this dictionary, sometimes not.
        if 'instance' in connection_properties['connection_info']:
            self.datasource_db_type = connection_properties['connection_info']['instance']
        elif 'dbclient' in connection_properties['connection_info']:
            self.datasource_db_type = connection_properties['connection_info']['dbclient']
        else:
            self.datasource_db_type = '???'

        self.version = connection_properties['connection_info']['version']


        # Note: these both generate info log messages that I can't get rid of
        self.ago_sd_search_object = self.get_ago_data(sd=True)
        self.ago_fs_search_object = self.get_ago_data(fs=True)

        # Then create the proper connection string with the sde file specified in our permissions
        # Use our own specified SDE instead of the sde file embedded in the .aprx file so we can enforce
        # proper permissioning
        self.sde_connection = self.perms['sde'] + "\\" + self.dataset

        # A temporary sde made from the one embedded into the aprx file.
        # self.sde_connection = self.project_map.listLayers()[0].dataSource

        # Make sure the dataset in the connection exists with arcpy.Exists()
        # Note: I've had the arcpy.Exists function lie to me before, so don't fully trust it.
        # Second note: arcpy.Exists totally lies and then fails at the record count lines
        # So let's use that in addition to our Exists check..
        try:
            arcpy.Exists(self.sde_connection)
        except Exception as e:
            traceback.print_tb(e.__traceback__)
            #logger.error('Dataset failed an arcpy.Exists! Full connection is: {}'.format(self.sde_connection))
            raise AssertionError('Dataset failed an arcpy.Exists! Full connection is: {}'.format(self.sde_connection))



        # Also check to see if the dataset is privileged to gis_sde_viewer if it's a public upload.
        if self.org == 'ago' and self.perms == 'public_perms':
            perms_config = dict(self.config.items(perms))
            sde_connection_test = perms_config['sde']
            sde_connection_test = sde_connection_test + "\\" + self.dataset
            record_count_test = int(arcpy.GetCount_management(sde_connection_test)[0])

    def get_ago_data(self, fs=False, sd=False):
        # There are two objects in AGO for each dataset, a feature service and service definition.
        # As far as I understand, the feature service is the actual GIS layer that serves GIS info
        # The service definition is a file that contains all the data we use to update it with.
        # We'll need two self variables for each of these ago objects to create our own sd file and publish.
        # I've done this to try to make the code reusable. -Roland
        ago_query_string = "title:{} AND owner:{}".format(self.name, self.login['user'])
        if fs:
            search_type = "Feature Service"
        elif sd:
            search_type = "Service Definition"

        # the gis.content.search line can sometimes bomb out with a 503 gateway error because esri barely has a handle
        # on what they're doing with AGO, so we need to do a while loop to retry
        logger.debug("Searching ago with query: {},{}".format(ago_query_string, search_type))
        ago_output = self.gis.content.search(ago_query_string, item_type=search_type)

        # If we get more than one search result from AGO, loop through the list of search results.
        if len(ago_output) > 1:
            logger.debug("Got multiple hits from AGO search, looking for exact match.")
            ago_item = False
            for item in ago_output:
                logger.debug("title match check, '{}' == '{}' ?".format(item.title, self.name))
                # Make sure the item title in AGO matches exactly to our .aprx file.
                if item.title == self.name:
                    ago_item = item
                    break
                else:
                    continue
        elif len(ago_output) == 1:
            ago_item = ago_output[0]
        else:
            #logger.error("Could not find our item in AGO. Check your query and user permissions.")
            raise AssertionError("Could not find our item in AGO, make sure the items are owned by AGO user '{}' and check the item permissions.".format(self.login['user']))

        # Find not-None items in the AGO layer. I don't know why but sometimes they're none.
        if ago_item.homepage is None:
            logger.info("Chosen AGO '{}' item url: {}".format(search_type, ago_item.url))
        else:
            logger.info("Chosen AGO '{}' item url: {}".format(search_type, ago_item.homepage))

        if fs:
            if ago_item.tables:
                self.ago_type = 'table'
            if ago_item.layers:
                self.ago_type = 'layer'
            # Pull our layers from the feature service layer in AGO, test if the list is empty first.
            # this function should return properly for tables
            if ago_item._has_layers():
                return ago_item
            if not ago_item.layers:
                error_message = "Layer value of this AGO fs item is non-existent! Make sure the item \
                                is a feature service in AGO or republish the feature service."
                raise Exception(error_message)

        return ago_item

    def checks(self):
        try:
            # With the fully qualified name, we can check to see if the oracle table has seen any changes
            # If we're pulling from Postgres, then don't run this.
            if 'oracle11g' in self.datasource_db_type:
                logger.info('Checking last modified time of dataset in Oracle databridge.')
                changed_return = self.oracle_has_dataset_changed(record=False)
                if changed_return[0] == True:
                    # If True, continue with the update
                    logger.info("Dataset '{}' has changed in Oracle Databridge, SCN number: {}".format(self.dataset,
                                                                                                       changed_return[
                                                                                                           1]))
                elif changed_return[0] == False:
                    message = "Dataset '{}' has not changed in Oracle Databridge! Not updating. SCN number: {}".format(
                        self.dataset, changed_return[1])
                    logger.info(message)
                    self.write_report_and_exit(message, error=False)
                elif changed_return[0] == None:
                    message = 'Dataset in  databridge updated too recently to push to AGO, probably actively updating?'
                    logger.error(message)
                    self.write_report_and_exit(message, error=True)
                else:
                    message = "Unknown error from oracle_has_dataset_changed() function, this should not happen!!!"
                    logger.error(message)
                    self.write_report_and_exit(message, error=True)

            # If we've determined that the dataset is probably no longer updating, then run the record counts.
            # The ago portion fails often with a 500 if we're hitting it hard, so loop it.
            loop = 0
            while True:
                try:
                    # Get record count in the database
                    self.db_record_count = int(arcpy.GetCount_management(self.sde_connection)[0])
                    # Get record count of the AGO feature layer/service
                    if self.ago_type == 'layer':
                        self.ago_record_count = self.ago_fs_search_object.layers[0].query(return_count_only=True)
                        # if it's a table:
                    if self.ago_type == 'table':
                        self.ago_record_count = self.ago_fs_search_object.tables[0].query(return_count_only=True)
                    break
                except Exception as e:
                    logger.error('Failed to get a record count on the datasets, trying again...')
                    loop += 1
                    if loop > 3:
                        # Pass the exception up the stack trace if we still failed.
                        raise Exception('Failed at getting record counts! Error: {}'.format(str(e)))
                    else:
                        sleep(1 + random())
                # Print our record counts first for the log's sake.
            logger.info("DB Record Count: {}, AGO Record Count: {}".format(self.db_record_count, self.ago_record_count))

            # Throw in a random sleep to try to prevent multiple processes from being well-lined up.
            # Reason being that the function oracle_has_dataset_changed reads and writes to a single file.
            # Hasn't been an issue yet but I thought I'd do this additional step. Probably worthless.
            sleep(1 + random())

            # Get the field names of the dataset
            db_fields = [f.name for f in arcpy.ListFields(self.sde_connection)]
            # Remove shape fields from list of fields because they won't match AGO shape fields
            shapes = [f.name for f in arcpy.ListFields(self.sde_connection, 'shape*')]
            db_fields = sorted(set(db_fields) - set(shapes))

            ago_fields = []
            # Build our AGO fields list for comparison
            if self.ago_type == 'layer':
                for f in self.ago_fs_search_object.layers[0].properties.fields:
                    if 'Shape' not in f['name']:
                        logger.debug("  - Adding field from AGO: {}".format(f['name']))
                        ago_fields.append(f['name'])
                    else:
                        logger.debug("  - Passing field from AGO: {}".format(f['name']))
            if self.ago_type == 'table':
                for f in self.ago_fs_search_object.tables[0].properties.fields:
                    if 'Shape' not in f['name']:
                        logger.debug("  - Adding field from AGO: {}".format(f['name']))
                        ago_fields.append(f['name'])
                    else:
                        logger.debug("  - Passing field from AGO: {}".format(f['name']))

            # Get the differences between our lists of fields in ago and the db.
            field_differences = list(set(ago_fields).symmetric_difference(set(db_fields)))

            # If there are field differences and we're ignoring geometric fields arcgis desktop apps sometimes adds,
            # then make sure the field differences aren't those fields.
            # 'ignore_system_fields' is a command-line flag that will enable this conditional check.
            if len(field_differences) > 0 and self.ignore_st_fields is True:
                for field_name in field_differences:
                    # Does the differing field not start with 'st_' ?
                    if field_name[0:3] != 'st_':
                        # We cannot ignore this field difference, return an error message and bomb out.
                        error_message = "Found field differences: {}".format(field_differences)
                        logger.error("Source DB fields: {}".format(list(set(db_fields))))
                        logger.error("AGO fields: {}".format(list(set(ago_fields))))
                        self.write_report_and_exit(error_message, error=True)
                    continue
                # If the only field differences begin with "st_", then we're good to go.
                logger.info("ago_fields value matches databridge: {}".format(sorted(ago_fields)))
                return True
            # Else there are valid field differences and we need to error out..
            elif len(field_differences) > 0:
                error_message = "Found field differences: {}".format(field_differences)
                logger.error("Source DB fields: {}".format(list(set(db_fields))))
                logger.error("AGO fields: {}".format(list(set(ago_fields))))
                logger.error(error_message)
                self.write_report_and_exit(error_message, error=True)

            # If we get here and there are no field differences, we are g2g, continue
            elif len(field_differences) == 0:
                logger.info("ago_fields value matches databridge: {}".format(sorted(ago_fields)))
            else:
                error_message = "Unhandled situation in checks function. This should not happen."
                logger.error(error_message)
                self.write_report_and_exit(error_message, error=True)

            # Arbitrarily, let's say if the DB row count is less than 85% of what's in ago there might
            # have been a botched partial update. We'd like to avoid having partial datasets
            # get pushed to AGO.
            threshold = int(self.ago_record_count * 0.85)
            if self.db_record_count < threshold:
                error_message = """
                                DB record count is less than 85% of AGO! Did a failed update occur?
                                AGO Record Count: {}, DB Record Count: {}
                                Run this again with the -r flag to ignore and update anyway.
                                """.format(self.ago_record_count, self.db_record_count)
                logger.error(error_message)
                self.write_report_and_exit(error_message, error=True)

            # If the record count is zero bomb out immediately.
            if self.db_record_count == 0:
                error_message = "Dataset record count in the db is 0! Something bad happened, start panicking."
                logger.error(error_message)
                self.write_report_and_exit(error_message, error=True)

            # If we pass all these check blocks with sys.exit()'ing, then return True to continue with the ago update.
            return True

        except Exception as e:
            error_message = "Error in checks function: {}.".format(str(e))
            logger.exception(error_message)
            self.write_report_and_exit(error_message, error=True)

    '''
    This function is perhaps a bit too long with it's conditional blocks, could be simplified.
    It handles recording the SCN number from oracle, if the 'record' value is set to True.
    SCN: System Change Number, a number associated with Oracle transactions that have been committed
      and altered data on a system table. If we pull this number and compare it to the last number we saw,
      we can determine if a change to the table occurred.
    The following function manages a pickle file in the current directory which contains a dictionary that has
      keys of the names of the datasets, and the value is the previously recording SCN number for that table.
    'Record' arg is a boolean that determines if we record the value for a successful update or if we're just checking.
    '''
    def oracle_has_dataset_changed(self, record):
        pickle_file = './scns.pkl'

        user = self.databridge_creds['user']
        password = self.databridge_creds['password']
        database = self.databridge_creds['database']
        try:
            # Connect to database
            connection_string = user + '/' + password + '@' + database
            db_connect = cx_Oracle.connect(connection_string)

            cursor = db_connect.cursor()
        except Exception as e:
            raise Exception(str(e))
            sys.exit(1)

        query1 = 'select max(ora_rowscn) from {}'.format(self.dataset)
        cursor.execute(query1)
        scn_number = cursor.fetchall()[0][0]


        def has_scn_changed(dataset_name, current_scn):
            # If the pickle file exists, pull in it's dictionary variable.
            if os.path.isfile(pickle_file):
                scns_dict = pickle.load(open(pickle_file, 'rb'))
                # check if dataset is already recorded as a key in the saved dictionary
                if dataset_name in scns_dict:
                    previous_scn = scns_dict[dataset_name]
                    # If the key exists, check it's value(scn) against the current scn, return False for no changes to the table
                    if previous_scn == current_scn:
                        return False
                    # Else, the number has changed, overwrite the scn with the new number.
                    else:
                        scns_dict[dataset_name] = current_scn
                        # Write the now changed dictionary back to the pickle file.
                        if record == True:
                            pickle.dump(scns_dict, open(pickle_file, 'wb'))
                        return True
                # Else the dataset has not been recorded yet. Record it and then return True for changed so the update runs.
                else:
                    scns_dict[dataset_name] = current_scn
                    if record == True:
                        pickle.dump(scns_dict, open(pickle_file, 'wb'))
                    return True

            # If the pickle file doesn't exist, dump a new dict containing this one key to it.
            else:
                scns_dict = {}
                scns_dict[dataset_name] = current_scn
                if record == True:
                    pickle.dump(scns_dict, open(pickle_file, 'wb'))
                return True

        # If we're simply recording the number, just do this and end execution
        if record == True:
            if os.path.isfile(pickle_file):
                scns_dict = pickle.load(open(pickle_file, 'rb'))
                scns_dict[self.dataset] = scn_number
                pickle.dump(scns_dict, open(pickle_file, 'wb'))
                return
            else:
                scns_dict = {}
                scns_dict[self.dataset] = scn_number
                pickle.dump(scns_dict, open(pickle_file, 'wb'))
                return

        date_modified = None
        loop_count = 0
        keep_looping = True

        # While loop to check for transaction timestamps that have occurred within 20 minutes. This
        # could indicate that an update is occurring right now.
        while keep_looping and loop_count <= 3:
            # print("SCN: ", scn_number)
            try:
                query2 = 'select distinct scn_to_timestamp({}) from {}'.format(scn_number, self.dataset)
                cursor.execute(query2)
                date_modified = cursor.fetchall()[0][0]
                # Returned timestamp comes as a datetime object
                # print("Timestamp': ", date_modified)
            # we will get an exception from cx_Oracle when scn_to_timestamp fails if the date is older than 5 days.
            except Exception as e:
                if 'specified number is not a valid system change number' in str(e):
                    logger.info('Older than 5 days, continuing SCN check.')
                    keep_looping = False
                elif 'ORA-00904' in str(e):
                    msg = "ORA-00904 error received trying to check the scn timestamp, is the dataset empty?"
                    logger.error(msg)
                    self.write_report_and_exit(msg, error=True)
                else:
                    msg = "Unexpected error checking scn timestamp!: {}".format(str(e))
                    logger.error(msg)
                    self.write_report_and_exit(msg, error=True)

            # If we managed to get a date without failing, than see how long ago it was.
            if date_modified:
                # date_time_obj = datetime.strptime(date_modified_str, '%Y-%m-%d %H:%M:%S')
                age = datetime.now() - date_modified
                total_seconds = age.total_seconds()
                diff = 1200 - int(total_seconds)
                # print("Total seconds: ", total_seconds)
                if int(total_seconds) > 1200 or diff == int(0):
                    logger.info("'{}' occurred more than 20 minutes ago, continuing SCN check.".format(date_modified))
                    keep_looping = False
                else:
                    diff = 1200 - int(total_seconds)
                    logger.info(
                        "'{}' occurred within the last 20 minutes, too recent! Sleeping {} seconds.".format(date_modified, diff))
                    loop_count += 1
                    # Sleep the amount of time it would take to get to 20 minutes, plus some.
                    # Let's only do this loop max 3 times so we're waiting at most 1 hour.
                    sleep(diff+10)

        if loop_count > 3:
            logger.info('Waited too long, returning None to indicate we had to wait too long.')
            return None, 0

        # If we passed the prior loop block, then continue with checking the recent SCN number against the last, and then
        # saving it into the pickle file.
        # Throw a try around this because I'm afraid the pickle.dump function might clobber over another process
        # since the ago_update script is heavily multi-process, I can see these clobbering each other at some point
        # and maybe failing in an odd way.
        try:
            changed = has_scn_changed(self.dataset, scn_number)
        except Exception as e:
            raise Exception(str(e))
        return changed, scn_number

    '''I'm getting sloppy here but I need a way to write to the report in the class, whereas traditionally I've
    been doing it outside in the main function. Oh well, this all needs to be rewritten already because I've
    already added too many features again after the original plan.'''
    def write_report_and_exit(self, message, error):
        # Exit early if we don't want to log anything.
        if self.no_log == True:
            if error == True:
                sys.exit(1)
            else:
                sys.exit(0)

        today = date.today()
        summary_file = os.path.join(script_directory, 'logs', str(today) + '-summary.txt')
        if error == True:
            msg = '<span style="color:DarkRed;">Failed!</span>, {} at {}<br>'.format(self.name, strftime("%H:%M:%S"))
            msg = msg + '\n&emsp;&#8226;initial error encountered: {}<br>'.format(message)
        else:
            msg = '<span style="color:DarkBlue;">No update needed</span>: {}. Checked at {}<br>'.format(self.name, strftime("%H:%M:%S"))
        with open(summary_file, 'a') as file:
            file.write(msg + '\n')
        if error == True:
            sys.exit(1)
        else:
            sys.exit(0)

    def create_sd_file(self, enable_editing):
        sddraft_file = self.name + '.sddraft'

        def verbose_print_xml():
            doc = DOM.parse(sddraft_file)
            typeNames = doc.getElementsByTagName('TypeName')
            for typeName in typeNames:
                logger.info(str(typeName.firstChild.data))
                extension = typeName.parentNode
                for extElement in extension.childNodes:
                    logger.info(str(extElement.tagName))
                    for propArray in extElement.childNodes:
                        try:
                            logger.info("  " + str(propArray.tagName))
                        except Exception as e:
                            pass
                        for propSet in propArray.childNodes:
                            for prop in propSet.childNodes:
                                for prop1 in prop.childNodes:
                                    try:
                                        if prop1.tagName == "Key":
                                            logger.info("     " + str(prop1.firstChild.data))
                                    except Exception as e:
                                        pass

        def EnableEditing():
            capabilities = "Query,Create,Delete,Update,Editing"
            # Modify feature layer capabilities to enable Create and Sync
            doc = DOM.parse(sddraft_file)
            typeNames = doc.getElementsByTagName('TypeName')
            for typeName in typeNames:
                if typeName.firstChild.data == "FeatureServer":
                    extension = typeName.parentNode
                    for extElement in extension.childNodes:
                        if extElement.tagName == 'Definition':
                            for propArray in extElement.childNodes:
                                if propArray.tagName == 'Info':
                                    for propSet in propArray.childNodes:
                                        for prop in propSet.childNodes:
                                            for prop1 in prop.childNodes:
                                                if prop1.tagName == "Key":
                                                    if prop1.firstChild.data == 'webCapabilities':
                                                        if prop1.nextSibling.hasChildNodes():
                                                            prop1.nextSibling.firstChild.data = capabilities
                                                        else:
                                                            txt = doc.createTextNode(capabilities)
                                                            prop1.nextSibling.appendChild(txt)
            f = open(sddraft_file, 'w')
            doc.writexml(f)
            f.close()

        try:
            # Keep having issues with .sd files already existing in the destination so I'm just gonna attempt a delete first.
            if os.path.isfile(self.sd_file):
                os.remove(self.sd_file)

            logger.debug("Creating the '{}' file.".format(sddraft_file))
            # Old way sddraft way
            # arcpy.mp.CreateWebLayerSDDraft(project_map, sddraft_file, project_map.name, "MY_HOSTED_SERVICES", "FEATURE_ACCESS")
            # New sddraft way provided by Alex Brown
            draft = self.project_map.getWebLayerSharingDraft("HOSTING_SERVER", "FEATURE", self.name)
            draft.exportToSDDraft(sddraft_file)

            if enable_editing:
                logger.info("Modifying capabilities of sddraft to allow editing in AGO..")
                EnableEditing()
            #verbose_print_xml()

            # Convert the sddraft file to a 'fully consolidated service definition file'.
            # New pro way
            logger.info("Creating the '{}' file.".format(self.sd_file))
            arcpy.StageService_server(sddraft_file, self.sd_file)

            logger.debug("Successfully made the sd file, removing sddraft.")
            # Something wonky happening with duplicate files appearing... gonna try to throw in a sleep
            # in case arcgis is doing something asynchronously...
            sleep(5)
            os.remove(sddraft_file)

            self.sd_file_created = True
            return True

        except Exception as e:
            if "No Layer or Table was initialized." in str(e):
                error_message = "ESRI error: No Layer or Table was initialized. Is the dataset a feature class?"
                self.write_report_and_exit("message", error=False)
            error_message = "Failed to create {} file, error: {} <br>".format(self.sd_file, str(e))
            self.sd_fail_counter += 1
            self.sd_error_msg = error_message
            logger.exception(error_message)

            if os.path.isfile(sddraft_file):
                os.remove(sddraft_file)
            if os.path.isfile(self.sd_file):
                os.remove(self.sd_file)

            # When making an sd file, arcpy creates a folder in the cwd named with this python script's PID
            # The folder can get quite large since it's all the data on the feature, so if we fail we want to
            # make sure it's cleaned up.
            esri_temp_work_dir = os.path.join(script_directory, str(pid))
            if os.path.isdir(esri_temp_work_dir):
                try:
                    shutil.rmtree(esri_temp_work_dir)
                except:
                    pass
            return False

    def sd_publish(self, preserve_editor_tracking):
        try:
            logger.debug("Updating layer: '{}' with sd file: '{}'...".format(self.name, self.sd_file))
            self.ago_sd_search_object.update(data=self.sd_file)
            logger.info("AGO Service Definition object updated, moving on to publishing to the Feature Service..")
            sleep(1 + random())
            # Publish it to the feature service
            # if we got the preserve_editor_tracking flag, pass special publish params to preserve them.
            # https://support.esri.com/en/technical-article/000021839
            # https://developers.arcgis.com/rest/users-groups-and-items/publish-item.htm
            if preserve_editor_tracking:
                logger.info("Passing publish params to preserve editor tracking..")
                pub_params = {"editorTrackingInfo": {"enableEditorTracking": 'true', "preserveEditUsersAndTimestamps": 'true'}}
                logger.debug(pub_params)
                fs = self.ago_sd_search_object.publish(publish_parameters=pub_params, overwrite=True)
            else:
                fs = self.ago_sd_search_object.publish(overwrite=True)
            # Now set AGO share permissions
            fs.share(org=self.perms['shrorg'], everyone=self.perms['shreveryone'], groups=self.perms['shrgroups'])
            logger.info("Layer '{}' published! Publish output: {}".format(self.name, fs))
            # Remove the sd file once finished
            if os.path.isfile(self.sd_file):
                os.remove(self.sd_file)
            self.successfully_published = True
            # Finally, record the changed SCN if we successfully updated. This is to avoid a small edge case
            # where we record an SCN before we know if we updated.
            if 'oracle11g' in self.datasource_db_type:
                self.oracle_has_dataset_changed(record=True)
            return True
        except Exception as e:
            error_msg = 'AGO upload failed! Exception Error: ' + str(e)
            self.publish_fail_counter += 1
            self.publish_error_msg = str(e)
            logger.exception(error_msg)
            return False


@click.command()
@click.option('--org', '-o', required=True,
              help='Either eoc or ago, matches config groups in config.ini')
@click.option('--perms', '-p', required=True,
              help='Type of permission you want set in AGO, check config.ini for more info')
@click.option('--dataset-name', '-d', required=True,
              help='If using "manual" type, specifiy the name of your dataset that will be in the name of the sd file.')
@click.option('--ignore-st-fields', '-i', is_flag=True,
              help='Flag to ignore fields that begin with "st_", which are metadata fields that dont get pushed to AGO')
@click.option('--republish', '-r', is_flag=True,
              help='Will not perform consistency checks between the DB and AGO, essentially republishing the dataset into AGO.')
@click.option('--preserve-editor-tracking', is_flag=True,
              help='Preserves editor tracking of the dataset, see: https://support.esri.com/en/technical-article/000021839')
@click.option('--enable-editing', is_flag=True,
              help='Enable editing in AGO, see: https://www.esri.com/arcgis-blog/products/arcgis-pro/mapping/publish-and-overwrite-web-layers-in-modelbuilder/')
@click.option('--email-dept', '-e', is_flag=True,
              help='To be implemented, email departments a notification that a dataset has been updated.')
@click.option('--no-log', '-n', is_flag=True,
              help='Flag to turn off summary logging and set logger level to "warning" from "info".')
def main(dataset_name, org, perms, ignore_st_fields, republish, preserve_editor_tracking, enable_editing, email_dept, no_log):
    global_start = datetime.now()

    today = date.today()
    summary_file = os.path.join(script_directory, 'logs', str(today) + '-summary.txt')
    global logger
    log_name = dataset_name
    # if no_log passed, only provide warning logs and worse
    # https://docs.python.org/3/library/logging.html#levels
    if no_log is True:
        logger = get_logger(log_name=log_name,log_level='warning')
    else:
        logger = get_logger(log_name=log_name, log_level='info')
    logger.debug("************* Script {0} started at: {1} *************".format(script_name, global_start))

    initialize_retry_counter = 0
    upload_instance = 'uninitialized'
    # The DatasetUploadObj is the class where the main logic happens.
    # There are a few AGO and arcpy things that happen on instance initialization and may fail
    # and we'll get them back as exceptions. Any of them mean we can't continue, so try to catch them,
    # and write it into the summary report.
    while not isinstance(upload_instance, DatasetUploadObj) and initialize_retry_counter < 3:
        try:
            # A few esri functions that will fail for no reason in the main, just gotta retry.
            upload_instance = DatasetUploadObj(dataset_name, org, perms, ignore_st_fields, no_log)
        except Exception as e:
            logger.error(str(e))
            traceback.print_tb(e.__traceback__)
            if ("Cannot find aprx file" in str(e) or "Could not find our item in AGO" in str(e)) and no_log is False:
                msg = '<span style="color:DarkRed;">Failed!</span>, {} at {}<br>'.format(dataset_name,
                                                                                         strftime("%H:%M:%S"))
                msg = msg + '\n&emsp;&#8226;initial error encountered: {}<br>'.format(str(e))
                with open(summary_file, 'a') as file:
                    file.write(msg + '\n')
                sys.exit(1)
            initialize_retry_counter += 1
            initialize_error = str(e)


    # If our instance was never made, then fail out.
    if not isinstance(upload_instance, DatasetUploadObj):
        logger.error("Initialization_error: " + initialize_error)
        if no_log is False:
            msg = '<span style="color:DarkRed;">Failed!</span>, {} at {}<br>'.format(dataset_name, strftime("%H:%M:%S"))
            msg = msg + '\n&emsp;&#8226;initial error encountered: {}<br>'.format(initialize_error)
            with open(summary_file, 'a') as file:
                file.write(msg + '\n')
        sys.exit(1)

    # Run checks to make sure the datasets are g2g, unless the --republish/-r flag was passed.
    if republish is False:
        upload_instance.checks()
    else:
        logger.info("--republish flag passed, not running checks function.")

    if initialize_retry_counter > 0:
        logger.warning("Initialization errors encountered but passed: {}".format(initialize_retry_counter))

    logger.debug(upload_instance.name)
    logger.debug(upload_instance.aprx_file)

    # SD loop block, while sd file isn't made and we're under the failure counter..
    while not upload_instance.sd_file_created and upload_instance.sd_fail_counter < 5:
        upload_instance.create_sd_file(enable_editing)
        # Throw in a sleep in case arcpy is doing something asynchronously...
        sleep(5)

    # AGO upload loop block
    # the sd_file_created param should be true now
    # while we're not successfully published and we're under the failure counter..
    if upload_instance.sd_file_created:
        while not upload_instance.successfully_published and upload_instance.publish_fail_counter < 5:
            upload_instance.sd_publish(preserve_editor_tracking)
            # Throw in a sleep in case arcpy is doing something asynchronously...
            sleep(5)

    # Next block is writing info for an email to go out later.
    duration = datetime.now() - global_start
    duration_formatted = str(timedelta(seconds=duration.seconds))
    # color the text of the duration spent uploading if it's greater than 15 minutes
    if duration.seconds > 900:
        duration_formatted = '<span style="color:orange;">{0}</span>'.format(duration_formatted)
    logger.info("Total time: {}".format(duration_formatted))
    # Write duration and failure information if applicable
    if no_log is False:
        with open(summary_file, 'a') as file:
            if upload_instance.successfully_published:
                msg = '<b>Success!</b>, {}, duration: <b>{}</b> at {}<br>'.format(upload_instance.name, duration_formatted, strftime("%H:%M:%S"))
            else:
                msg = '<span style="color:DarkRed;">Failed!</span>, {}, duration: <b>{}</b> at {}<br>'.format(upload_instance.name, duration_formatted, strftime("%H:%M:%S"))
            if upload_instance.sd_fail_counter > 0:
                msg = msg + '\n&emsp;&#8226;sd creation retries: {}<br>'.format(upload_instance.sd_fail_counter)
                msg = msg + '\n&emsp;&#8226;error message: {}<br>'.format(upload_instance.sd_error_msg)
            if upload_instance.publish_fail_counter > 0:
                msg = msg + '\n&emsp;&#8226;fs publish retries: {}<br>'.format(upload_instance.publish_fail_counter)
                msg = msg + '\n&emsp;&#8226;error message: {}<br>'.format(upload_instance.publish_error_msg)
            file.write(msg + '\n')


if __name__ == '__main__':
    try:
        prune_logs()

        global script_directory
        global script_name
        global pid

        script_name = os.path.basename(sys.argv[0])
        script_directory = os.path.dirname(os.path.realpath(__file__))
        pid = os.getpid()
        os.chdir(script_directory)

        main()

    except Exception as e:
        # If an exception falls through here, that means it's an error we didn't catch.
        # Exit with code 1 to alert us.
        print("Unhandled exception!!: " + str(e))
        sys.exit(1)
