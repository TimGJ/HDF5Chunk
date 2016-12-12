# -*- coding: utf-8 -*-
"""
Reads the consolidated table from the mml audit database
one chunk at a time (as otherwise the date returned would melt the VM).

Appends the chunks of data to the H5 store.

@author: tim.greening
"""

import pandas as pd
import logging
import sqlalchemy
import argparse
import sys
import os
import gc


def ParseArguments():
    """
    Parse the command line arguments (the script will be run on different
    hosts, mostly in batch mode, so a CLI interface is highly desirable)
    
    Syntax is:
        test_h5_chunking.py --rated=<ratedtable> --unrated=<unratedtable>
            --host=<database host IP> --user=<database user> --password=<database password>
            --logfile=<logfile name> --debug=<debug level>
    """
    parser = argparse.ArgumentParser(description="Test performance of H5 chunking")
    parser.add_argument("--username", help="DB user name", default="mmladmin")
    parser.add_argument("--hostname", help="DB hostname/IP address", default="localhost")
    parser.add_argument("--password", help="DB password", default="mml-admin")
    parser.add_argument("--table",  help="Table of all CDRs", default="unrated_subset")
    parser.add_argument("--dbname", help="Name of database", default="audit")
    parser.add_argument("--chunksize", type=int, default=1000)
    parser.add_argument("--logfile", default=GetLogfileName())
    parser.add_argument("--debug", default=False, action="store_true")
    parser.add_argument("--repository", help="H5 repository name", default="test.h5")
    
    return parser.parse_args()

def GetLogfileName():
    """
    Returns the name of the logfile. (Done as a separate function as it's invoked
    as part of the command line agumment parsing)
    """

    basename = os.path.basename(sys.argv[0]).split(".")[0]  
    return basename + ".log"

def CreateLogging(logfile, debug=False):
    """
    Sets up the logging environment. The debug flag controls the logging level.
    by default it is set to logging.INFO
    
    """
    if debug:
        level = logging.DEBUG
    else:
        level = logging.INFO
    
    logging.basicConfig(filename=logfile, level=level, format='%(asctime)s: %(levelname)s: %(message)s')
    logging.info("Create logfile with level {}".format(level))
    
if __name__ == '__main__':

    args = ParseArguments()
    CreateLogging(logfile=args.logfile, debug=args.debug)
    logging.debug("Invoked with options: {}".format(args))
    logging.debug("Opening H5 repository {}".format(args.repository))

    connectstr = "mysql+mysqlconnector://{}:{}@{}/{}".format(args.username, args.password, args.hostname, args.dbname)
    logging.debug("Attempting to connect to database with connection string {}".format(connectstr))
    engine = sqlalchemy.create_engine(connectstr)
    
    # Now try reading the table chunk by chunk in to a pandas dataframe. If we 
    # attempt to load the entire table in one go we will blow the top off the 
    # VM so we have to read and analyse it in chunks.

    h5 = pd.HDFStore(args.repository, mode="w", complevel=9)
    offset = 0
    i = 0
    while True:    
        sql = "SELECT * FROM {} ORDER BY SETUP LIMIT {} OFFSET {}".format(args.table, args.chunksize, offset) 
        logging.debug("Trying to execute {}".format(sql))
        try:
            df = pd.read_sql_query(sql, engine)
        except sqlalchemy.exc.DatabaseError:
            logging.error("Database read failed! Hull breached!")
            raise sqlalchemy.exc.DatabaseError("Read failed on {}".format(sql))
        logging.debug("Read chunk #{} (shape {})".format(i, df.shape))
        logging.debug("Appending chunk")
        h5.append(args.table, df, data_columns=True, min_itemsize={'source':50, 
                                'destination':52, 'cdpn':20, 'cgpn':20, 'md5_hex':32})
        i += 1
        offset += args.chunksize
        if df.shape[0] < args.chunksize:
            break
        gc.collect()
        # We've had issues with the top being blown off the VM, possibly due
        # to slightly tardy garbage collection, so exposlictly invoke it after each chunk
        # is read/written
    logging.debug("Done!")
    h5.close()
    
    
        

