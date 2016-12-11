# -*- coding: utf-8 -*-
"""
Created on Fri Dec  9 19:32:26 2016

@author: User
"""

import pandas as pd
import datetime
import numpy



def CreateH5TestGroup(group,              # Name of the H5 repository e.g. "foo.h5"
                      dataset,            # Name of the dataset within the group e.g. "test"
                      start,              # Start time
                      stop,               # Stop time
                      chunksize=1e5,      # Size of chunks in which records will be calculated
                      maxrecords=1e6,     # Maximum number of records to generate
                      mindelta=5000,       # Min time difference in ms
                      maxdelta=60000):     # Max time difference in ms
                
    colours = ['red', 'yellow', 'green', 'blue', 'pink', 'purple', 'orange']
    fruit = ['apple', 'banana', 'blackcurrant', 'cherry', 'grape', 'loganberry', 
             'kiwi', 'orange', 'pineapple', 'plum', 'raspberry', 'strawberry']
    animals = ['dog', 'cat', 'goldfish', 'horse', 'hamster', 'chimpanzee']
    
    chunk=0
    current=start
    count=0
    h5=pd.HDFStore(group)
    try:
        h5.remove(dataset)
    except KeyError:
        print("Empty group")        

    while True:
        chunk += 1
        r=[]
        for i in range(int(chunksize)):
            current += datetime.timedelta(milliseconds=numpy.random.randint(mindelta, maxdelta))
            r.append(current)
            count += 1
            if current > stop:
                break
        df = pd.DataFrame({'setup':r, 
                           'x':numpy.random.rand(len(r)), 
                           'y':numpy.random.rand(len(r)),
                            'colour':numpy.random.choice(colours, len(r)),
                            'fruit':numpy.random.choice(fruit, len(r)),
                            'animal':numpy.random.choice(animals, len(r))})
        print("Writing chunk #{} [{} to {}] ({} rows). {} total records.".format(
                chunk, 
                df.setup.iloc[0], 
                df.setup.iloc[-1], 
                i+1, count))
        h5.append(dataset, df, format='table', data_columns=True)
        if current >= stop or count >= maxrecords:
            break
    print("Closing")
    h5.close()

CreateH5TestGroup('medium.h5', 'bar', 
                  datetime.datetime(2016,1,1), 
                    datetime.datetime(2017,1,1),
                    chunksize=1e5, maxrecords=1e6,
                    mindelta=5000, maxdelta=30000)
CreateH5TestGroup('large.h5', 'bar', 
                  datetime.datetime(2016,1,1), 
                    datetime.datetime(2017,1,1),
                    chunksize=1e6, maxrecords=1e8,
                    mindelta=50, maxdelta=500)
