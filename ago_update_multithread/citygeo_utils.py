import configparser
import os
import sys
import logging
from datetime import datetime, timedelta, date
import time
from logging import handlers
import smtplib
from email.mime.text import MIMEText


def get_logger(log_dir='logs',log_name=None,log_level='INFO'):
    # Ensure directory exists
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    # Get the process ID so we can create a unique log file for writing to
    #pid = os.getpid()

    scriptDirectory = os.path.dirname(os.path.realpath(__file__))
    os.chdir(scriptDirectory)

    config = configparser.ConfigParser()
    config.read(scriptDirectory + os.sep + 'config.ini')

    # Logging variables
    #MAX_BYTES = config.get('logging', 'max_bytes')  # in bytes
    # Max number appended to log files when MAX_BYTES reached
    #BACKUP_COUNT = config.get('logging', 'file_count')
    #LOG_LEVEL = config.get('logging', 'log_level')
    scriptName = os.path.basename(sys.argv[0])

    logger = logging.getLogger()
    logger.setLevel(log_level.upper())
    if log_name is None:
        today = date.today()
        logfilename = os.path.join(log_dir, str(today) + "-log.txt")
    else:
        logfilename = os.path.join(log_dir, str(log_name) + "-log.txt")

    log_file = os.path.join(scriptDirectory, logfilename)

    if log_name is None:
        pass
    else:
        stdout_logging = logging.StreamHandler(sys.stdout)
        logger.addHandler(stdout_logging)

    formatter = logging.Formatter('%(asctime)s - PID:%(process)d - %(levelname)s - %(message)s')
    #filelogger = handlers.RotatingFileHandler(log_file, 'a', int(MAX_BYTES), int(BACKUP_COUNT))
    filelogger = handlers.WatchedFileHandler(log_file, 'a')
    filelogger.setFormatter(formatter)
    logger.addHandler(filelogger)

    if (log_level == 'DEBUG'):
        logger.debug('DEBUG logging enabled, check log file at %s' % str(log_file))
    return logger


def prune_logs(log_dir='./logs'):
    import os
    from pathlib import Path

    scriptDirectory = os.path.dirname(os.path.realpath(__file__))
    os.chdir(scriptDirectory)

    def get_size(log_dir):
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(log_dir):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                # skip if it is symbolic link
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)

        #Convert bytes to megabytes
        total_mb = float(total_size) / 1048576
        return round(total_mb,1)

    #print("Total size of logs directory is: {} megabytes".format(get_size(log_dir)))

    # If the log directory is over 50 megabytes, delete the oldest log file.
    if get_size(log_dir) >= 50:
        # Sorts files in the directory by most recently modified files last
        paths = sorted(Path(log_dir).iterdir(), key=os.path.getmtime)
        # Remove 5 oldest log files.
        #print("Removing old log file: {}".format(paths[0]))
        try:
            os.remove(paths[0])
            os.remove(paths[1])
            os.remove(paths[2])
            os.remove(paths[3])
            os.remove(paths[4])
        except:
            pass


def sendemail(recipients, subject, text):
    global_config = configparser.ConfigParser()
    global_config.read("E:\\Scripts\\global-email-config.ini")

    sender = global_config.get('email', 'sender')
    relay = global_config.get('email', 'relay')

    commaspace = ', '
    msg = MIMEText(text, 'html')
    msg['To'] = commaspace.join(recipients)
    msg['From'] = sender
    msg['X-Priority'] = '5'
    msg['Subject'] = subject
    server = smtplib.SMTP(relay)
    server.sendmail(sender, recipients, msg.as_string())
    server.quit()