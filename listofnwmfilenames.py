#!/usr/bin/env python
# coding: utf-8

# ## List of NWM File Names
# ### This code will output a list of NWM file names corresponding to the NWM output files according to the following givens:
# - Date Range
# - Geography (conus, hawaii, or puerto rico)
# - Run type (short_range, medium_range, analysis_assim, etc.)
# - Variable type (channel_rt, land, reservoir, terrain_rt, or forcing)

# Cell 1 defines dictionaries that hold the different options for geography, run type, variable type. An additional dictionary "memdict" holds options which correspond to different types of medium_range and long_range run types.

# In[1]:

from dateutil import rrule
from datetime import datetime

rundict = {
    1: "short_range",
    2: "medium_range",
    3: "medium_range_no_da",
    4: "long_range",
    5: "analysis_assim",
    6: "analysis_assim_extend",
    7: "analysis_assim_extend_no_da",
    8: "analysis_assim_long",
    9: "analysis_assim_long_no_da",
    10: "analysis_assim_no_da",
    11: "short_range_no_da"
}
memdict = {
    1: "mem_1",
    2: "mem_2",
    3: "mem_3",
    4: "mem_4",
    5: "mem_5",
    6: "mem_6",
    7: "mem_7"
}
vardict = {
    1: "channel_rt",
    2: "land",
    3: "reservoir",
    4: "terrain_rt",
    5: "forcing"
}
geodict = {
    1: "conus",
    2: "hawaii",
    3: "puertorico"
}
# Cell 2 contains some notes about the limitations of the program.

# In[2]:


#although forcing is a type of run, it is handled as a variable type

#for _no_da run types, the only variable input can be channel_rt

#short_range_hawaii, short_range_hawaii_no_da, analysis_assim_hawaii, and analysis_assim_hawaii_no_da cannot be accomodated because they output every 15 minutes

#runsuffix is not being used, but i'm not going to delete it from everything just in case this whole thing needs to be restructured


# Cell 3 defines the function "makename" which forms the file name outputs.

# In[3]:


def makename (date, run_name, var_name, fcst_cycle, fcst_hour, geography, run_type, fhprefix = "", runsuffix = "", varsuffix = "", run_typesuffix = ""):
    return f"nwm.{date}/{run_type}{run_typesuffix}/nwm.t{fcst_cycle}z.{run_name}{runsuffix}.{var_name}{varsuffix}.{fhprefix}{fcst_hour}.{geography}.nc"


# Cells 5 - 7 define selection functions that reach into the previously created dictionaries and return the necessary variable given a dictionary key input.

# In[5]:


def selectvar(vardict, varinput):
    return vardict[varinput]


# In[6]:

def selectgeo(geodict, geoinput):
    return geodict[geoinput]



# In[7]:

def selectrun(rundict, runinput):
    return rundict[runinput]

# Cell 8 deals with inconsistencies in the run type name between the folder names and the file names for specific run types.

# In[8]:


#setting run_type 
def run_type(runinput, varinput, default):
    if varinput == 5: #if forcing
        if runinput == 5 and geoinput == 2: #if analysis_assim and hawaii
            return "forcing_analysis_assim_hawaii"
        elif runinput == 5 and geoinput == 3: #if analysis_assim and puerto rico
            return "forcing_analysis_assim_puertorico"
        elif runinput == 1 and geoinput == 2: #if short range and hawaii
            return "forcing_short_range_hawaii"
        elif runinput == 1 and geoinput == 3: #if short range and puerto rico
            return "forcing_short_range_puertorico" 
        elif runinput == 5: #if analysis assim
            return "forcing_analysis_assim"
        elif runinput == 6: #if analysis_assim_extend
            return "forcing_analysis_assim_extend"
        elif runinput == 2: #if medium_range
            return "forcing_medium_range"
        elif runinput == 1: #if short range
            return "forcing_short_range"

    elif runinput == 5 and geoinput == 3: #if analysis_assim and puertorico
        return "analysis_assim_puertorico"

    elif runinput == 10 and geoinput == 3: #if analysis_assim_no_da and puertorico
        return "analysis_assim_puertorico_no_da"

    elif runinput == 1 and geoinput == 3: #if short_range and puerto rico
        return "short_range_puertorico"

    elif runinput == 11 and geoinput ==3: #if short_range_no_da and puerto rico
        return "short_range_puertorico_no_da"

    else:
        return default


# Cell 9 creates the _mem# and _# additions to the names of medium_range and long_range run types.

# In[9]:


#setting varsuffix and run_typesuffix (for the file path folder name)
def run_typesuffix(memdict):
    if memdict == 1:
        return "_mem1"
    elif memdict == 2:
        return "_mem2"
    elif memdict == 3:
        return "_mem3"
    elif memdict == 4:
        return "_mem4"
    elif memdict == 5:
        return "_mem5"
    elif memdict == 6:
        return "_mem6"
    elif memdict == 7:
        return "_mem7"
    else:
        return ""

def varsuffix(memdict):
    if memdict == 1:
        return "_1"
    elif memdict == 2:
        return "_2"
    elif memdict == 3:
        return "_3"
    elif memdict == 4:
        return "_4"
    elif memdict == 5:
        return "_5"
    elif memdict == 6:
        return "_6"
    elif memdict == 7:
        return "_7"
    else:
        return ""


# Cell 10 deals with inconsistencies of the forecast hour prefix between analysis_assim and all other run types.

# In[10]:


#setting fhprefix
def fhprefix(runinput):
    if runinput == 4 or runinput == 5 or runinput == 6 or runinput == 7 or runinput == 8 or runinput == 9 or runinput == 10: #if analysis_assim (any type)
        return "tm"
    else:
        return "f"


# Cell 11 does the actual work of creating the list of file names for each scenario of inputs.

# In[11]:


def create_file_list(start_date, end_date, runinput, varinput, geoinput, memdict):
#for given date,  run, var, fcst_cycle, and geography, print file names for the valid time (the range of fcst_hours) and dates

    runsuffix = ""
    
    try:
        geography = selectgeo(geodict, geoinput)
    except:
        geography = "geography_error"
    try:
        run_name = selectrun(rundict, runinput)
    except:
        run_name = "run_error"
    try:
        var_name = selectvar(vardict, varinput)
    except:
        var_name = "variable_error"


    r = []
    if runinput == 1: #if short_range
        if varinput == 5: #if forcing
            for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
                date = dt.strftime('%Y%m%d') #create date range

                for fc in range (0, 29, 1):
                    fcst_cycles = (f"{fc:02d}") #create forecast cycle range

                    for fh in range (1,19,1):
                        fcst_hours = (f"{fh:03d}") #create forecast hours range
                        r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))

        elif varinput == 5 and geoinput == 2: #if forcing and hawaii
            for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
                date = dt.strftime('%Y%m%d') 

                for fc in range (0, 13, 12):
                    fcst_cycles = (f"{fc:02d}") 

                    for fh in range (1,49,1):
                        fcst_hours = (f"{fh:03d}") 
                        r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))

        elif varinput == 5 and geoinput == 3: #if forcing and puerto rico
            for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
                date = dt.strftime('%Y%m%d') 

                fcst_cycles = "06"

                for fh in range (1,48,1):
                    fcst_hours = (f"{fh:03d}") 
                    r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))

        elif geoinput == 3: #if puerto rico
            for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
                date = dt.strftime('%Y%m%d') 

                for fc in range (6, 19, 12):
                    fcst_cycles = (f"{fc:02d}") 

                    for fh in range (1,48,1):
                        fcst_hours = (f"{fh:03d}") 
                        r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))
        else:
            for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
                date = dt.strftime('%Y%m%d')
                for fc in range (0, 24, 1):
                    fcst_cycles = (f"{fc:02d}") 

                    for fh in range (1,19,1):
                        fcst_hours = (f"{fh:03d}") 
                        r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))

    elif runinput == 2: #if medium_range
        if varinput == 5: #if forcing
            for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
                date = dt.strftime('%Y%m%d') 

                for fc in range (0, 13, 6):
                    fcst_cycles = (f"{fc:02d}") 

                    for fh in range (0,240,1):
                        fcst_hours = (f"{fh:03d}") 
                        r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))
        else:
            for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
                date = dt.strftime('%Y%m%d') 
                for fc in range (0, 19, 6):
                    fcst_cycles = (f"{fc:02d}")

                    if memdict == 1: #if medium_range_mem1
                        if varinput == 1 or varinput == 3: #if channel or reservoir
                            for fh in range (1, 241, 1):
                                fcst_hours = (f"{fh:03d}") 
                                r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))

                        elif varinput == 2 or varinput ==4: #if land or terrain
                            for fh in range (3, 241, 3):
                                fcst_hours = (f"{fh:03d}") 
                                r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))
                        else:
                            r.append ("varinput error")

                    elif memdict == 2 or memdict == 3 or memdict == 4 or memdict == 5 or memdict == 6 or memdict == 7:#if medium_range_mem_2 through _mem7
                        if varinput == 1 or varinput ==3: #if channel or reservoir
                            for fh in range (1, 205, 1):
                                fcst_hours = (f"{fh:03d}") 
                                r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))

                        elif varinput == 2 or varinput == 4: #if land or terrain
                            for fh in range (3, 205, 3):
                                fcst_hours = (f"{fh:03d}") 
                                r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))
                        else:
                            r.append ("varinput error")
                    else:
                        r.append ("memdict error")

    elif runinput == 3: #if medium_range_no_da
        if varinput ==1:
            for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
                date = dt.strftime('%Y%m%d') 

                for fc in range (0, 13, 6):
                    fcst_cycles = (f"{fc:02d}") 

                    for fh in range (3,240,3):
                        fcst_hours = (f"{fh:03d}") 
                        r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))
        else:
            r.append ("only valid variable for a _no_da type run is channel_rt")    

    elif runinput == 4: #if long_range
        for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
            date = dt.strftime('%Y%m%d') 
            for fc in range (0, 19, 6):
                fcst_cycles = (f"{fc:02d}")

                if varinput == 1 or varinput ==3: #if channel or reservoir
                    for fh in range (6, 721, 6):
                        fcst_hours = (f"{fh:03d}")
                        r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))

                elif varinput == 2: #if land (no terrain)
                    for fh in range (24, 721, 24):
                        fcst_hours = (f"{fh:03d}")
                        r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))
                else:
                    r.append ("varinput error")

    elif runinput == 5: #if analysis_assim (simplest form)
        if varinput == 5: #if forcing
           for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
                date = dt.strftime('%Y%m%d') 

                for fc in range (0, 20, 1):
                    fcst_cycles = (f"{fc:02d}")

                    for fh in range (0,3,1):
                        fcst_hours = (f"{fh:02d}")
                        r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))

        elif varinput == 5 and geoinput == 2: #if forcing and hawaii
           for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
                date = dt.strftime('%Y%m%d') 

                for fc in range (0, 19, 1):
                    fcst_cycles = (f"{fc:02d}")

                    for fh in range (0,3,1):
                        fcst_hours = (f"{fh:02d}")
                        r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))
        else:
            for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
                date = dt.strftime('%Y%m%d') 

                for fc in range (0, 24, 1):
                    fcst_cycles = (f"{fc:02d}")

                    for fh in range (0,3,1):
                        fcst_hours = (f"{fh:02d}")
                        r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))

    elif runinput == 6: #if analysis_assim_extend
        for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
            date = dt.strftime('%Y%m%d') 
            fcst_cycles = "16"

            for fh in range (0,28,1):
               fcst_hours = (f"{fh:02d}")
               r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))

    elif runinput == 7: #if analysis_assim_extend_no_da
        if varinput == 1:
            for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
                date = dt.strftime('%Y%m%d') 
                fcst_cycles = "16"

                for fh in range (0,28,1):
                    fcst_hours = (f"{fh:02d}")
                    r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))
        else:
            r.append ("only valid variable for a _no_da type run is channel_rt")

    elif runinput == 8: #if analysis_assim_long
        for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
            date = dt.strftime('%Y%m%d') 

            for fc in range (0, 24, 6):
                fcst_cycles = (f"{fc:02d}")

                for fh in range (0,12,1):
                    fcst_hours = (f"{fh:02d}")
                    r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))

    elif runinput == 9: #if analysis_assim_long_no_da
        if varinput == 1:

            for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
                date = dt.strftime('%Y%m%d') 

                for fc in range (0, 24, 6):
                    fcst_cycles = (f"{fc:02d}")

                    for fh in range (0,12,1):
                        fcst_hours = (f"{fh:02d}")
                        r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))
        else: 
            r.append ("only valid variable for a _no_da type run is channel_rt")

    elif runinput == 10: #if analysis_assim_no_da
        if varinput == 1:

            for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
                date = dt.strftime('%Y%m%d') 

                for fc in range (0, 21, 1):
                    fcst_cycles = (f"{fc:02d}")

                    for fh in range (0,3,1):
                        fcst_hours = (f"{fh:02d}")
                        r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))
        else:
            r.append ("only valid variable for a _no_da type run is channel_rt")

    elif runinput == 11 and geoinput ==3 : #if short_range_puertorico_no_da
        if varinput == 1:

            for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
                date = dt.strftime('%Y%m%d') 

                for fc in range (6, 19, 12):
                    fcst_cycles = (f"{fc:02d}") 
                    for fh in range (1,49,1):
                        fcst_hours = (f"{fh:03d}") 
                        r.append (makename(date, run_name, var_name, fcst_cycles, fcst_hours, geography, run_type(runinput, varinput, run_name), fhprefix(runinput), runsuffix, varsuffix(memdict), run_typesuffix(memdict)))
        else:
            r.append ("only valid variable for a _no_da type run is channel_rt")
    else:
        r.append ("run error")
    
    return r


# The next step will be to create an interactive dashboard

# 

# Cell 4 introduces the date range function that will be utilized, given a start date and an end date.

# In[4]:

def main():
    start_date = "20220822"
    end_date = "20220824"

    for dt in rrule.rrule(rrule.DAILY, dtstart=datetime.strptime(start_date, '%Y%m%d'), until=datetime.strptime(end_date, '%Y%m%d')):
        date = dt.strftime('%Y%m%d')

    fcst_cycle = "00"
    fcst_hour = "001"
    runsuffix = ""

    memdict = 3
    
    geoinput = 1

    runinput = 2

    varinput = 2
    print(create_file_list(start_date, end_date, runinput, varinput, geoinput, memdict))
    
    
if __name__ == "__main__":
    main()
