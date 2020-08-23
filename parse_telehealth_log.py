"""Identify errors in telehealth log file and create list of id's to update

Usage:
"""
import mysql.connector
from mysql.connector import errorcode
import config.database as database
# import datetime as dt

# import pprint
import os

_ini_errors = []
_five_day_errors = []

def main():
    """ loop through a log file looking for fsock open errors, and save the pid 
        for the record on which it pid it occurs on
    """
    
    # hl7_directoryops.get_incoming()
    create_error_list()
    telehealth_log_cleanup()

def create_error_list():
    global _ini_errors
    global _five_day_errors
    
    """loop through records in a log file.
        buffer the pid and read forward
        if fsockopen is encountered before new pid, identify that pid as an error
        need to know which type of mail was being sent as well, which is idenfied by a section heading
    Args:
        none
    Returns:
        list of lists, fsockerrors(ini_errors, 5day_errors)
    """

    log_file = open(os.path.join('log_file','Telehealth_Email_Ini2020-08-22.txt'), 'r')
    
    ini_errors = []
    five_day_errors = []
    
    # beginning of 5-day section starts with 'start send_five_day_premail'
    # initial until proven otherwise
    initial = True
    five_day = False
    
    for log_entry in log_file:
        if log_entry.find('five_day_premail')>0 :
            initial = not initial
            five_day = not five_day
        
        if (log_entry[0:3]) == 'pid':
            # pid = log_entry.split('|')[0].split(':')[1].strip()
            appt_id = log_entry.split('|')[1].split(':')[1].strip()
            print ('appt_id', appt_id)
        if log_entry.find('fsockopen')>0 and len(appt_id)>0: 
            if initial:
                _ini_errors.append(appt_id)
            elif five_day:
                _five_day_errors.append(appt_id)
            appt_id = ''
    
    log_file.close()
    
    
    
def telehealth_log_cleanup():
    cnx = mysql.connector.connect(**database.pat_sched_local)
    cursor = cnx.cursor(dictionary=True)

    for ini_error in _ini_errors:
        print('ini_error: ', ini_error)
        sql = ''' UPDATE patient_email_log
                    SET initial_date_sent = NULL
                    WHERE f_appt_id = %s'''
        cursor.execute (sql,(ini_error,))   
    for five_day_error in _five_day_errors:
        print('five_day_error: ', five_day_error)                   
        sql = ''' UPDATE patient_email_log
                    SET secondary_date_sent = NULL
                    WHERE f_appt_id = %s'''
        cursor.execute (sql,(five_day_error,))   
    
    cnx.commit
    

if __name__ == "__main__":
    main()

