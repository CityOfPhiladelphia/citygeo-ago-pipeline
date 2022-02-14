import socket
from citygeo_utils import sendemail,get_logger
from configparser import ConfigParser
from datetime import date
import os
import sys

global_config_parser = ConfigParser()
global_config_parser.read("E:\\Scripts\\global-email-config.ini")

data_engineers = global_config_parser.get('email', 'data_engineers').split('\n')
matt = global_config_parser.get('email', 'matt').split('\n')
maps = global_config_parser.get('email', 'maps').split('\n')
email_recipients = data_engineers + matt + maps

today = date.today()
script_name = os.path.basename(sys.argv[0])
script_directory = os.path.dirname(os.path.realpath(__file__))


def main():
    # Roundabout way to get our true ip address, because socket.gethostbyname() gives us a virtual IP instead.
    # Connect to our default gateway, then get the ip of the socket created.
    ip = [(s.connect(('192.168.0.1', 53)), s.getsockname()[0], s.close()) for s in
          [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]
    hostname = socket.getfqdn()

    email_footer = """
                        <br>
                        <hr>
                        Script running on server: {} ({}) <br>
                        Script path: {}
                    """.format(hostname, ip, os.path.join(script_directory, script_name))

    summary_file = os.path.join(script_directory, 'logs', str(today) + '-summary.txt')
    if not os.path.isfile(summary_file):
        sys.exit(0)
    body = 'AGO Update Summary Report for {}<br> \
           Check out the github readme for this script for purpose and usage: https://github.com/CityOfPhiladelphia/CityGeo-Automation-Scripts/tree/master/ago_update_multithread <br>\
           The following AGO datasets have been \
           updated from their counterparts in Databridge.<hr><br>'.format(str(today))

    failure_occurred = False
    with open(summary_file) as file:
        lines = file.readlines()
        for line in lines:
            if ('Fail' in line) or ('fail' in line):
                failure_occurred = True
            body = body + line

    email_body = body + email_footer
    email_subject = 'AGO Update Summary Report'
    if failure_occurred:
        print("Failures detected, email sent!")
        sendemail(email_recipients, email_subject, email_body)
    else:
        print("No failures detected, no email sent.")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        # If an exception falls through here, that means it's an error we didn't catch.
        # Exit with code 1 to alert us.
        print("Unhandled exception!!: " + str(e))
        sys.exit(1)

