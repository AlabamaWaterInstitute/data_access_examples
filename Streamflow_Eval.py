#!/usr/bin/env python
# coding: utf-8
#author: Ryan Johnson, PHD, Alabama Water Institute
#Date: 6-6-2022


'''
Run using the OWP_env: 
https://www.geeksforgeeks.org/using-jupyter-notebook-in-virtual-environment/
https://github.com/NOAA-OWP/hydrotools/tree/main/python/nwis_client

https://noaa-owp.github.io/hydrotools/hydrotools.nwm_client.utils.html#national-water-model-file-utilities
will be benefitical for finding NWM reachs between USGS sites
'''

# Import the NWIS IV Client to load USGS site data
from hydrotools.nwis_client.iv import IVDataService
import pandas as pd
import numpy as np
import data
import matplotlib.pyplot as plt
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
from sklearn.metrics import max_error
from sklearn.metrics import mean_absolute_percentage_error
import hydroeval as he

# In[3]:



class Reach_Eval():
    
    def __init__(self, NWISreach, NWMsegment, startDT, endDT, freq):
        self = self
        self.NWISreach = NWISreach
        self.NWM_segment = NWMsegment
        self.startDT = startDT
        self.endDT = endDT
        self.freq = freq
        self.cms_to_cfs = 35.314666212661
    
    #A function for accessing NWIS data and procesing to daily mean flow
    def NWIS_retrieve(self):
        # Retrieve data from a single site
        print('Retrieving USGS site ', self.NWISreach, ' data')
        service = IVDataService()
        self.usgs_data = service.get(
            sites=self.NWISreach,
            startDT= self.startDT,
            endDT=self.endDT
            )

        #Get Daily mean for NWM comparision
        self.usgs_meanflow = pd.DataFrame(self.usgs_data.reset_index().groupby(pd.Grouper(key = 'value_time', freq = self.freq))['value'].mean())
        self.usgs_meanflow = self.usgs_meanflow.reset_index()

        #add key site information
        #make obs data the same as temporal means
        self.usgs_data = self.usgs_data.head(len(self.usgs_meanflow))

        #remove obs streamflow
        del self.usgs_data['value']
        del self.usgs_data['value_time']

        #connect mean temporal with other key info
        self.usgs_meanflow = pd.concat([self.usgs_meanflow, self.usgs_data], axis=1)
        self.usgs_meanflow = self.usgs_meanflow.rename(columns={'value_time':'Datetime', 'value':'USGS_flow','usgs_site_code':'USGS_ID', 'variable_name':'variable'})
        self.usgs_meanflow = self.usgs_meanflow.set_index('Datetime')
        
        
    # A function for accessing NWM data and processing to daily mean flow
    def NWM_retrieve(self):
        print('Retrieving NWM reach ', self.NWM_segment, ' data')
        self.nwm_predictions = data.get_nwm_data(self.NWM_segment,  self.startDT,  self.endDT)
        self.NWM_meanflow = self.nwm_predictions.resample(self.freq).mean()*self.cms_to_cfs
        self.NWM_meanflow = self.NWM_meanflow.reset_index()
        self.NWM_meanflow = self.NWM_meanflow.rename(columns={'time':'Datetime', 'value':'Obs_flow','feature_id':'NWM_segment', 'streamflow':'NWM_flow', 'velocity':'NWM_velocity'})
        self.NWM_meanflow = self.NWM_meanflow.set_index('Datetime')

        
    def NWM_Eval(self):
        
        #merge NWM and USGS
        self.Evaluation = pd.concat([self.usgs_meanflow, self.NWM_meanflow], axis=1)

        #create two plots, a hydrograph and a parity plot
        discharge = 'Discharge ' + '('+ self.Evaluation['measurement_unit'][0]+')'
        max_flow = max(max(self.Evaluation.USGS_flow), max(self.Evaluation.NWM_flow))
        min_flow = min(min(self.Evaluation.USGS_flow), min(self.Evaluation.NWM_flow))


        fig, ax = plt.subplots(1,2, figsize = (10,5))
        ax[0].plot(self.Evaluation.index, self.Evaluation.USGS_flow, color = 'blue', label = 'USGS')
        ax[0].plot(self.Evaluation.index, self.Evaluation.NWM_flow, color = 'orange',  label = 'NWM')
        ax[0].fill_between(self.Evaluation.index, self.Evaluation.NWM_flow, self.Evaluation.USGS_flow, where= self.Evaluation.NWM_flow >= self.Evaluation.USGS_flow, facecolor='orange', alpha=0.2, interpolate=True)
        ax[0].fill_between(self.Evaluation.index, self.Evaluation.NWM_flow, self.Evaluation.USGS_flow, where= self.Evaluation.NWM_flow < self.Evaluation.USGS_flow, facecolor='blue', alpha=0.2, interpolate=True)
        ax[0].set_xlabel('Datetime')
        ax[0].set_ylabel(discharge)
        ax[0].tick_params(axis='x', rotation = 45)
        ax[0].legend()

        ax[1].scatter(self.Evaluation.USGS_flow, self.Evaluation.NWM_flow, color = 'black')
        ax[1].plot([min_flow, max_flow],[min_flow, max_flow], ls = '--', c='red')
        ax[1].set_xlabel('Observed USGS (cfs)')
        ax[1].set_ylabel('Predicted NWM (cfs)')
        
        #calculate some performance metrics
        r2 = r2_score(self.Evaluation.USGS_flow, self.Evaluation.NWM_flow)
        rmse = mean_squared_error(self.Evaluation.USGS_flow, self.Evaluation.NWM_flow, squared=False)
        maxerror = max_error(self.Evaluation.USGS_flow, self.Evaluation.NWM_flow)
        MAPE = mean_absolute_percentage_error(self.Evaluation.USGS_flow, self.Evaluation.NWM_flow)*100
        kge, r, alpha, beta = he.evaluator(he.kge, self.Evaluation.NWM_flow, self.Evaluation.USGS_flow)
        
        print('The NWM demonstrates the following model performance')
        print('R2 = ', r2)
        print('RMSE = ', rmse, self.Evaluation['measurement_unit'][0])
        print('Maximum error = ', maxerror, self.Evaluation['measurement_unit'][0])
        print('Mean Absolute Percentage Error = ', MAPE, '%')
        print('Kling-Gupta Efficiency = ', kge[0])
        
        

  


