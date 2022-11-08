#####Preamble

import time
import pandas as pd
from glob import glob
import numpy as np
import timeit

from occupationcoder.coder import coder
myCoder = coder.Coder()

import os.path
import re
import os
from os import walk
import nltk
nltk.download('punkt')
nltk.download('wordnet')

import multiprocessing
# We must import this explicitly, it is not imported by the top-level
# multiprocessing module.
import multiprocessing.pool
class NoDaemonProcess(multiprocessing.Process):
    @property
    def daemon(self):
        return False

    @daemon.setter
    def daemon(self, value):
        pass


class NoDaemonContext(type(multiprocessing.get_context())):
    Process = NoDaemonProcess

# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.
class MyPool(multiprocessing.pool.Pool):
    def _init_(self, *args, **kwargs):
        kwargs['context'] = NoDaemonContext()
        super(MyPool, self)._init_(*args, **kwargs)



#####set directory/

dir = '/home/cc18002/Adzuna/ALL/'
dir1 = '/home/cc18002/Adzuna/'

#dir1 = 'D:/Adzuna/Dataset/New.rar'
#patoolib.extract_archive(dir1, outdir=dir)

#####create file


d0 = [[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]]

df0 = pd.DataFrame(data=d0, columns=[ 'year', 'month', 'day', 'category_id', 'company', 'date_created','location_raw', 'salary_max', 'salary_min', 'salary_predicted',
       'salary_currency', 'job_title', 'job_sector', 'location_path', 'contract_time',
       'contract_type', 'company_id', 'company_name', 'job_description'] )
df0.to_csv(os.path.join(dir1, 'vacancy_stock_raw1.csv'))
df0=pd.read_csv(os.path.join(dir1, 'vacancy_stock_raw1.csv'))
#print(df0)


##### list all files

EXT = "*.csv"
all_csv_files = [file
                 for path, subdir, files in os.walk(dir)
                 for file in glob(os.path.join(path, EXT))]

leng=len(all_csv_files)
leng=2


    
#####Define function

def main(x):
    
    
    start = timeit.timeit()
    print(all_csv_files[x])

                
    d = pd.read_csv(all_csv_files[x], index_col=None, header=0)
    d = d.rename(columns={'title': 'job_title'})
    d = d.rename(columns={'description': 'job_description'})
    d = d.rename(columns={'category_name': 'job_sector'})
    d=d.astype(str)
                
    d['experience']=0
                
                
                # substring to be searched
    sub ='experience'
 
                # creating and passing series to new column
    d['EX']= d['job_description'].str.find(sub)
               
    d['experience'] = np.where(d['EX']!= '[-1]', 1, 0)
                    

    d['job_title'] = d['job_title'].str.slice(0, 100)
    d['job_sector'] = d['job_sector'].str.slice(0, 100)
    d['job_description'] = d['job_description'].str.slice(0, 200)



    d = d[[ 'year', 'month', 'day', 'category_id', 'company', 'date_created','location_raw', 'salary_max', 'salary_min', 'salary_predicted',
'salary_currency', 'job_title', 'job_sector', 'location_path', 'contract_time',
'contract_type', 'company_id', 'company_name', 'job_description', 'experience']]

         
    df1=myCoder.codedataframe(d)
            #print('socdone')
    df2 = df1[[ 'year', 'month', 'day', 'category_id', 'company', 'date_created','location_raw', 'salary_max', 'salary_min', 'salary_predicted',
   'salary_currency', 'job_title', 'job_sector', 'location_path', 'contract_time',
   'contract_type', 'company_id', 'company_name', 'SOC_code', 'experience']]


                    #df2=df2.append(df1)
                    #df2.to_csv(os.path.join(dir1, 'vacancy_stock_raw.csv'))
                 
    end = timeit.timeit()
    print( end - start)
    print(all_csv_files[x])
     
    return df2
                
if __name__ == '__main__':
        p = MyPool(processes=leng)
        data = p.map(main,[ i for i in range(0,leng,1)])
        p.close()
        #p.join()
        print(data)
        results = pd.concat(data)
        #print(results)
        results.to_csv(os.path.join(dir1, 'vacancy_list.csv'))


        
    
    
