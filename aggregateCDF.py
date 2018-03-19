
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
from netCDF4 import Dataset
import xarray as xr
import glob
import os

def minsAggregate(df,required): #takes a dataframe and finds the mean over every 5 mins
    df5min=df[required].resample("5T").mean() #aggregating it to 5 mins interval
    return df5min


def dataAggregate(input_file,required): #reads each netcdf file to a dataframe and then aggregates and appends to a final dataframe
    if input_files:
        outputDF=pd.DataFrame() #empty dataframe to hold the output
        for i in range(len(input_files)): #for every file in the Input DATA folder
            df = (xr.open_dataset(input_files[i])).to_dataframe() #convert the netCDF4 data to a dataframe
            outputDF=outputDF.append(minsAggregate(df,required)) #keep appending to the result data frame

        outputDF.rename(columns={'atmos_pressure':'atmospheric_pressure','rh_mean':'relative_humidity',
                         'temp_mean':'mean_temperature'}, inplace=True) #changing the column names of the dataframe to required format
        return outputDF   


def writeToCDF(output_file_path,output_file_name,outputDF): #read the final aggregated dataframe and writes to a CDF file format
    if not os.path.exists(output_file_path): #check if output folder exists, else create a newone
        os.makedirs(output_file_path)
    
    if output_file_name in (glob.glob(output_file_path+"/*.cdf")): #checks if output file name exists,if exists then 
                                                                    #requests user to change the name of the output file
        return "Output file name already exists. Please rename your output CDF file"
    else:  
        f =Dataset(output_file_path+"/"+output_file_name,'w', format='NETCDF4') #'w' stands for write

        time=f.createDimension('time',len(outputDF))
        atmospheric_pressure=f.createVariable('atmospheric_pressure','f4','time')
        relative_humidity=f.createVariable('relative_humidity','f4','time')
        mean_temperature=f.createVariable('mean_temperature','f4','time')
        timeVar=f.createVariable('time','f8','time')#,'f8','time')

        atmospheric_pressure[:]=outputDF['atmospheric_pressure'].values
        relative_humidity[:]=outputDF['relative_humidity'].values
        mean_temperature[:]=outputDF['mean_temperature'].values


        timeVar[:]=outputDF.index.values #TODO- date time conversion problem. check in the definition of datatype and units
        f.close()
        return "Completed writing output CDF file"



required=['atmos_pressure','rh_mean','temp_mean'] #required columns to aggregate. If needed can add more column names to add in the output file

output_file_name='sgpmetavgE13.b1.20180101.000000.cdf'
output_file_path='OUTPUT_DATA'

input_files=glob.glob("INPUT_DATA/*.cdf")

#FUNCTION CALLS
outputDF=dataAggregate(input_files,required)
writeToCDF(output_file_path,output_file_name,outputDF)


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:



