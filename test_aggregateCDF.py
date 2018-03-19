
# coding: utf-8

# In[1]:

import pytest
import unittest
import pandas as pd
import numpy as np
from netCDF4 import Dataset
import xarray as xr
import os
import glob

def minsAggregate(df,required):
    df5min=df[required].resample("5T").mean() #aggregating it to 5 mins interval
    return df5min

def dataAggregate(input_files,required):
    if input_files:
        outputDF=pd.DataFrame() #empty dataframe to hold the output
        for i in range(len(input_files)): #for every file in the Input DATA folder
            df = (xr.open_dataset(input_files[i])).to_dataframe() #convert the netCDF4 data to a dataframe
            outputDF=outputDF.append(minsAggregate(df,required)) #keep appending to the result data frame

        outputDF.rename(columns={'atmos_pressure':'atmospheric_pressure','rh_mean':'relative_humidity',
                         'temp_mean':'mean_temperature'}, inplace=True) #changing the column names of the dataframe to required format
        return outputDF   

def writeToCDF(output_file_path,output_file_name,outputDF):
    if not os.path.exists(output_file_path):
        os.makedirs(output_file_path)
    
    if output_file_name in (glob.glob(output_file_path+"/*.cdf")):
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
        
#checks the dataframe returned from dataAggregate
def test_dataFrames():
    test_input_path='test_DataFrame'
    test_input_file='test_input.cdf'
    input_files=[test_input_path+'/'+test_input_file]
    required=['atmos_pressure','rh_mean','temp_mean'] #required columns to aggregate. If needed can add more column names to add in the output file

    outputDF=dataAggregate(input_files,required)
    
    #getting required/expected outputDF from file
    requiredDF=pd.DataFrame()
    requiredDF=requiredDF.from_csv(test_input_path+'/test_dataframe_requiredOutput.csv')
    
    #checking if the required output and the output from the function are equal
    pd.testing.assert_frame_equal(requiredDF, outputDF, check_dtype=False) #checks if the required/expected dataframe and the output dataframe are equal

#checks the NETCDF file written by writeToCDF() function
def test_outputCDF():
    test_output_file_path="test_OutputCDF"
    test_output_file_name="test_output.cdf"
    test_input_path='test_DataFrame'
    outputDF=pd.DataFrame()
    outputDF=outputDF.from_csv(test_input_path+'/test_dataframe_requiredOutput.csv')
    
    writeToCDF(test_output_file_path,test_output_file_name,outputDF) #calling the function
    
    #getting required/expected outputDF from file
    requiredDF=pd.DataFrame()
    requiredDF=requiredDF.from_csv(test_output_file_path+'/test_outputCDF_requiredOutput.csv')
    
    #reading the file written by writeToCDF() function
    actualDF=(xr.open_dataset(test_output_file_path+'/'+test_output_file_name)).to_dataframe() #convert the netCDF4 data to a dataframe
    
    pd.testing.assert_frame_equal(requiredDF, actualDF, check_dtype=False) #checks if the required/expected dataframe and the output dataframe are equal

#checks if there are 1440 rows from the input CDF file, since there should be 24x60 minutes in a day.
def test_getInputRows():
    test_input_path='test_DataFrame'
    test_input_file='test_input.cdf'
    try:
        df = (xr.open_dataset(test_input_path+'/'+test_input_file)).to_dataframe() #convert the netCDF4 data to a dataframe
    except IOError:
        print('cannot open file')
    
    lenDF=len(df)
    assert (lenDF==1440)

#checks if the required attributes are present in the output CDF files
def test_outputOutputColumns():
    test_output_path='test_OutputCDF'
    test_output_file='test_output.cdf'
    try:
        df = (xr.open_dataset(test_output_path+'/'+test_output_file)).to_dataframe() #convert the netCDF4 data to a dataframe
    except IOError:
        print('cannot open file')
    
    output_columns= list(df.columns.values)
    required_columns=['atmospheric_pressure', 'relative_humidity', 'mean_temperature']
    assert (set(required_columns).issubset(output_columns))    


# In[93]:




# In[ ]:



