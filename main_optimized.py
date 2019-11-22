import numpy as np
import pandas as pd
import csv
import sys
from datetime import datetime

sourceCSV = './STL_Raw.csv'
columns = ['station', 'valid', 'lon', 'lat', 'tmpf', 'dwpf', 'relh', 'drct', 'sknt', 'p01i', 'alti', 'mslp', 'vsby', 'gust', 'skyc1', 'skyc2', 'skyc3', 'skyc4', 'skyl1',
           'skyl2', 'skyl3', 'skyl4', 'wxcodes', 'ice_accretion_1hr', 'ice_accretion_3hr', 'ice_accretion_6hr', 'peak_wind_gust', 'peak_wind_drct', 'peak_wind_time', 'feel', 'metar']
destinationCSV = './m_vectors.csv'

def rowCount(csvFile):
    with open(csvFile) as f:
        count = sum(1 for line in f)
        return count

def createCheckpoint(start) :
    f = open( './checkpoint.txt', 'a+' )
    f.write( '\ndate: %s , row: %s ' % (datetime.now() ,start) )
    f.close()

def preprocessing(row):
    date = row['valid'].values[0]
    # m_count = 0
    m_count = row.apply(lambda x: x == 'M', axis=1 )

    m_vector = [date, m_count]
    return m_vector 


def writeCSV(chunk):
    with open(destinationCSV, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(chunk)

def processChunks(processed, total):
    if processed != 0:
        df_chunk = pd.read_csv(sourceCSV, chunksize=1, skiprows=[i for i in range(1,processed+1)])
    else :
        df_chunk = pd.read_csv(sourceCSV, chunksize=1)

    for chunk in df_chunk:
        chunk_filter = preprocessing(chunk)
        writeCSV(chunk_filter)
        processed = processed + 1
        printProgressBar(processed, total, prefix=processed)

# Print iterations progress
def printProgressBar(iteration, total, prefix='', suffix='', data='', decimals=1, length=100, fill='█', printEnd="\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 *
               (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    
    print('\r%s %s |%s| %s%% %s' % (data, prefix, bar, percent, suffix), end=printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()

def __main__():
    try:
        print('counting rows')
        total_source_rows = rowCount(sourceCSV)
        print('Rows to Process: %s' % (total_source_rows))
        total_processed_rows = rowCount(destinationCSV)
        print('File found. Processing from row %s' % (total_processed_rows))
        processed = total_processed_rows

        processChunks(processed, total_source_rows)
    except:
        createCheckpoint(total_processed_rows)


__main__()
