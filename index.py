# -*- coding: utf-8 -*-
"""
Indexes an HDF dataset on a daily basis. Iterates through a potentially
humongous dataset assumed to be stored in chronological order and returns 
a pandas dataframe containing an offset in to the dataset marking the first
record of each date therein.

Requred because I am dealing with a dataset so large (> 10**8 records)
that even attempting to do a mask for a subsequent .loc[] operation 
blows the top of the virtual memory of a 16GB linux box. 

@author: User
"""

import pandas as pd
import datetime
import logging
import argparse
import os

def ChunkSizeGenerator(size, chunksize=10000):
    """
    A generator function. Returns the chunksize and offset to split 
    a number "size" in to chunks of "chunksize". Returns offset and chunksize
    """
    n, r = divmod(size, chunksize)
    for i in range(n):
        yield i*chunksize, chunksize
    if r:
        yield n*chunksize, r
    

def CreateIndex(group, dataset, key, chunksize=10000, indexname="index"):
    """
    Creates the index of the HDF5 group/dataset and stores it in a dataset
    <group>/<index>. Column <key> is assumed to be a datetime and the dataset
    is assumed to be stored in chronological order. 
    """
    logging.info("Opening {}".format(group))
    if os.path.exists(group):       
        h5 = pd.HDFStore(group)
        if '/' + dataset in h5.keys():       
            rows = h5.get_storer(dataset).nrows
            logging.debug("Dataset is {} rows in length".format(rows))
            for offset, size in ChunkSizeGenerator(rows, chunksize):
                logging.debug("Reading {} rows from offset {}".format(size, offset))
                block = h5.select(dataset, start=offset, stop=offset+size)
                logging.debug("From {} to {}".format(block[key].iloc[0], block[key].iloc[-1]))
            logging.info("Closing {}".format(group))
            h5.close()
        else:
            logging.error("Dateset {} not found in group {}".format(dataset, group))
            raise KeyError("Can't find dataset {} in group {}".format(dataset, group))
    else:
        logging.error("Group {} does not exist.".format(group))
        raise FileNotFoundError("Can not find group {}".format(group))
        
    
if __name__ =='__main__':
    
    parser=argparse.ArgumentParser(description="Produce index of time series in an HDF5 data store")
    parser.add_argument("group", help="HDF5 group file")
    parser.add_argument("dataset", help="Dataset within the group")
    parser.add_argument("key", help="Key within dataset")
    parser.add_argument("--chunksize", type=int, default=1000000, help="Records to read at a time")
    parser.add_argument("--index", default="setup", help="Index name")
    parser.add_argument("--debug", help="Log debug information", action="store_true")
    args = parser.parse_args()
        
    logging.basicConfig(filename="chunky.log", 
                        format="%(asctime)s:%(levelname)s:%(message)s", 
                        level=logging.DEBUG if args.debug else logging.INFO)

    logging.info("Creating index '{}' for {}/{}".format(args.index, args.group, args.dataset))
    CreateIndex(args.group, args.dataset, args.key, args.chunksize, args.index)