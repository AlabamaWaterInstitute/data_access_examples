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
from hydrotools.nwm_client import utils
import pandas as pd
import numpy as np
import data
import matplotlib.pyplot as plt
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
from sklearn.metrics import max_error
from sklearn.metrics import mean_absolute_percentage_error
import hydroeval as he
import dataretrieval.nwis as nwis
import streamstats
import geopandas as gpd
from IPython.display import display
import warnings
from progressbar import ProgressBar
import folium
import matplotlib
import mapclassify
warnings.filterwarnings("ignore")


class Reach_Eval():
    
    def __init__(self, NWISsite, startDT, endDT, freq, cwd):
        self = self
        self.NWISsite = NWISsite
        self.NWM_NWIS_df= utils.crosswalk(usgs_site_codes=self.NWISsite)
        self.NWM_segment = self.NWM_NWIS_df.nwm_feature_id.values[0]
        self.startDT = startDT
        self.endDT = endDT
        self.freq = freq
        self.cwd = cwd
        self.cms_to_cfs = 35.314666212661
    
    #A function for accessing NWIS data and procesing to daily mean flow
    def NWIS_retrieve(self):
        # Retrieve data from a single site
        print('Retrieving USGS site ', self.NWISsite, ' data')
        service = IVDataService()
        self.usgs_data = service.get(
            sites=self.NWISsite,
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
        
        #Get watershed information
        #self.get_StreamStats()
        #display(self.Catchment_Stats)
        
        
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
        
        #remove rows with NA
        self.Evaluation = self.Evaluation.dropna(axis = 0)
          
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
        
      
    
    def get_StreamStats(self):
        print('Calculating the summary statistics of the catchment')
        NWISinfo = nwis.get_record(sites=self.NWISsite, service='site')

        #Get site information for streamstats
        lat, lon = NWISinfo['dec_lat_va'][0],NWISinfo['dec_long_va'][0]
        ws = streamstats.Watershed(lat=lat, lon=lon)

        NWISindex = ['NWIS_site_id', 'Drainage_area_mi2', 'Mean_Basin_Elev_ft', 'Perc_Forest', 'Perc_Develop',
                     'Perc_Imperv', 'Perc_Herbace', 'Perc_Slop_30', 'Mean_Ann_Precip_in', 'Ann_low_cfs', 'Ann_mean_cfs', 'Ann_hi_cfs']
        
        
        #get stream statististics
        self.Param="00060"
        StartYr='1970'
        EndYr='2021'
        
        annual_stats = nwis.get_stats(sites=self.NWISsite,
                              parameterCd=self.Param,
                              statReportType='annual',
                              startDt=StartYr,
                              endDt=EndYr)

        mean_ann_low = annual_stats[0].nsmallest(1, 'mean_va')
        mean_ann_low = mean_ann_low['mean_va'].values[0]

        mean_ann = np.round(np.mean(annual_stats[0]['mean_va']),0)

        mean_ann_hi = annual_stats[0].nlargest(1, 'mean_va')
        mean_ann_hi = mean_ann_hi['mean_va'].values[0]

        

        try:
            darea = ws.get_characteristic('DRNAREA')['value']
        except (KeyError, ValueError):
            darea = 'na'
            
        try:
            elev = ws.get_characteristic('ELEV')['value']
        except (KeyError, ValueError):
            elev = 'na'
            
        try:
            forest = ws.get_characteristic('FOREST')['value']
        except (KeyError, ValueError):
            forest = 'na'
        
        try:
            dev_area = ws.get_characteristic('LC11DEV')['value']
        except (KeyError, ValueError):
            dev_area = 'na'
        
        try:
            imp_area = ws.get_characteristic('LC11IMP')['value']
        except (KeyError, ValueError):
            imp_area = 'na'
            
        try:
            herb_area = ws.get_characteristic('LU92HRBN')['value']
        except (KeyError, ValueError):
            herb_area = 'na'
            
        try:
            perc_slope = ws.get_characteristic('SLOP30_10M')['value']
        except (KeyError, ValueError):
            perc_slope = 'na'
            
        try:
            precip = ws.get_characteristic('PRECIP')['value']
        except (KeyError, ValueError):
            precip = 'na'
        
        #Put data into data frame and display
        NWISvalues = [self.NWISsite,darea, elev,forest, dev_area, imp_area, herb_area, perc_slope, precip, mean_ann_low, mean_ann, mean_ann_hi]

        Catchment_Stats = pd.DataFrame(data = NWISvalues, index = NWISindex)

        self.Catchment_Stats = Catchment_Stats.T
        display(self.Catchment_Stats)
        
        #plot the watershed
        title = 'Catchment for USGS station: '+self.NWISsite
        poly = gpd.GeoDataFrame.from_features(ws.boundary["features"], crs="EPSG:4326")
        df = poly.to_crs(epsg=3857)
        self.WatershedMap = df.explore(color = 'yellow', tiles = 'Stamen Terrain')
        
    def get_USGS_site_info(self, state):
    
        #url for state usgs id's
        url = 'https://waterdata.usgs.gov/'+state+'/nwis/current/?type=flow&group_key=huc_cd'

        NWIS_sites = pd.read_html(url)

        NWIS_sites = pd.DataFrame(np.array(NWIS_sites)[1]).reset_index(drop = True)

        cols = ['StationNumber', 'Station name','Date/Time','Gageheight, feet', 'Dis-charge, ft3/s']

        self.NWIS_sites = NWIS_sites[cols].dropna()
        
        self.NWIS_sites = self.NWIS_sites.rename(columns ={'Station name':'station_name', 
                                                               'Gageheight, feet': 'gageheight_ft',
                                                               'Dis-charge, ft3/s':'Discharge_cfs'})
        
        self.NWIS_sites = self.NWIS_sites[self.NWIS_sites.gageheight_ft != '--']


        self.NWIS_sites = self.NWIS_sites.set_index('StationNumber')

        # Remove unnecessary site information
        for i in self.NWIS_sites.index:
            if len(str(i)) > 8:
                self.NWIS_sites = self.NWIS_sites.drop(i)

        #remove when confirmed it works
       # NWIS_sites = NWIS_sites[2:3]

        site_id = self.NWIS_sites.index

        #set up Pandas DF for state streamstats

        Streamstats_cols = ['NWIS_siteid', 'Drainage_area_mi2', 'Mean_Basin_Elev_ft', 'Perc_Forest', 'Perc_Develop',
                         'Perc_Imperv', 'Perc_Herbace', 'Perc_Slop_30', 'Mean_Ann_Precip_in']

        self.State_NWIS_Stats = pd.DataFrame(columns = Streamstats_cols)

        pbar = ProgressBar()
        for site in pbar(site_id):

            siteinfo = self.NWIS_sites['station_name'][site]

            print('Calculating the summary statistics of the catchment for ', siteinfo, ', USGS: ',site)
            NWISinfo = nwis.get_record(sites=site, service='site')

            lat, lon = NWISinfo['dec_lat_va'][0],NWISinfo['dec_long_va'][0]
            ws = streamstats.Watershed(lat=lat, lon=lon)

            NWISindex = ['NWIS_site_id', 'NWIS_sitename', 'Drainage_area_mi2', 'Mean_Basin_Elev_ft', 'Perc_Forest', 'Perc_Develop',
                         'Perc_Imperv', 'Perc_Herbace', 'Perc_Slop_30', 'Mean_Ann_Precip_in', 'Ann_low_cfs', 'Ann_mean_cfs', 'Ann_hi_cfs']
        
        
            #get stream statististics
            self.Param="00060"
            StartYr='1970'
            EndYr='2021'

            annual_stats = nwis.get_stats(sites=self.NWISsite,
                                  parameterCd=self.Param,
                                  statReportType='annual',
                                  startDt=StartYr,
                                  endDt=EndYr)

            mean_ann_low = annual_stats[0].nsmallest(1, 'mean_va')
            mean_ann_low = mean_ann_low['mean_va'].values[0]

            mean_ann = np.round(np.mean(annual_stats[0]['mean_va']),0)

            mean_ann_hi = annual_stats[0].nlargest(1, 'mean_va')
            mean_ann_hi = mean_ann_hi['mean_va'].values[0]


            try:
                darea = ws.get_characteristic('DRNAREA')['value']
            except (KeyError, ValueError):
                darea = 'na'

            try:
                elev = ws.get_characteristic('ELEV')['value']
            except (KeyError, ValueError):
                elev = 'na'

            try:
                forest = ws.get_characteristic('FOREST')['value']
            except (KeyError, ValueError):
                forest = 'na'

            try:
                dev_area = ws.get_characteristic('LC11DEV')['value']
            except (KeyError, ValueError):
                dev_area = 'na'

            try:
                imp_area = ws.get_characteristic('LC11IMP')['value']
            except (KeyError, ValueError):
                imp_area = 'na'

            try:
                herb_area = ws.get_characteristic('LU92HRBN')['value']
            except (KeyError, ValueError):
                herb_area = 'na'

            try:
                perc_slope = ws.get_characteristic('SLOP30_10M')['value']
            except (KeyError, ValueError):
                perc_slope = 'na'

            try:
                precip = ws.get_characteristic('PRECIP')['value']
            except (KeyError, ValueError):
                precip = 'na'

            NWISvalues = [site,siteinfo, darea, elev,forest, dev_area, imp_area, herb_area, perc_slope, precip, mean_ann_low, mean_ann, mean_ann_hi]


            Catchment_Stats = pd.DataFrame(data = NWISvalues, index = NWISindex).T

            self.State_NWIS_Stats = self.State_NWIS_Stats.append(Catchment_Stats)

        State_NWIS_Stats.to_csv(self.cwd+'/State_NWIS_StreamStats/'+state+'StreamStats.csv')

  
