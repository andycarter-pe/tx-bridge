# From the '08_05_mjr_axis_xs_w_feature_id_nbi.gpkg' 
# (1) fix the abutment armpits (lefts and right)
# (2) determine the low chord elevation list
# (3) compute the conveyance area between the ground and low chord
# (4) compute min low chord and min ground elevation
#
# Created by: Andy Carter, PE
# Created - 2022.11.09
# Last revised - 2023.09.11
#
# tx-bridge - sub-process of the 8th processing script
# Uses the 'pdal' conda environment

# ************************************************************


# ************************************************************
import argparse

import geopandas as gpd
import pandas as pd
import ast # converting sting of list to list
import numpy as np

import os

import time
import datetime
# ************************************************************


# ----------------------------------------------  
# Print iterations progress
def fn_print_progress_bar (iteration,
                           total,
                           prefix = '', suffix = '',
                           decimals = 0,
                           length = 100, fill = '█',
                           printEnd = "\r"):
    """
    from: https://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console
    Call in a loop to create terminal progress bar
    Keyword arguments:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()
# ----------------------------------------------  

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def fn_area_under_bridge(df_bridge):
    """
    compute the area under the low chord of the bridge
    
    Args:
        df_bridge: pands dataframe: contains sta, height_under_bridge as float

    Returns:
        float of area under low chord and above the ground
    """
    # -----determine the area under the low chord ------
    df_bridge['height_under_bridge'] = df_bridge['max_ground_low_chord'] - df_bridge['ground_elv']
    
    flt_previous_sta = 0
    flt_previous_height = 0
    flt_area_total = 0
    
    for index2, row2 in df_bridge.iterrows():
        flt_current_sta = row2['sta']
        flt_current_height = row2['height_under_bridge']

        flt_width = flt_current_sta - flt_previous_sta
        flt_average_height = (flt_current_height + flt_previous_height) / 2
        flt_area = flt_width * flt_average_height

        flt_area_total += flt_area

        flt_previous_sta = flt_current_sta
        flt_previous_height = flt_current_height
        
    return(flt_area_total)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# ============================================================
def fn_list_of_start_end_deck_index(df_deck):
    
    """
    extend the left abutment to the ground if a ground elevation point is found
    within the specified vertical tolerance

    Args:
        df_deck: pands dataframe: contains sta, ground_elv, deck_elev as float

    Returns:
        list of lists of indecies where a deck starts and ends (integers)
         list[0] = list of indecies where a deck starts
         list[1] = list of indecies where a deck ends
    """
    
    df_deck["elevation_delta"] = df_deck['deck_elev'] - df_deck['ground_elv']

    int_end_deck = 0
    int_start_deck = 0

    list_start_index = []
    list_end_index = []

    for i in range(0, df_deck.shape[0]-1):
        if df_deck.iloc[i]['elevation_delta'] == 0 and df_deck.iloc[i+1]['elevation_delta'] > 0:
            int_start_deck +=1
            list_start_index.append(i+1)

        elif df_deck.iloc[i]['elevation_delta'] > 0 and df_deck.iloc[i+1]['elevation_delta'] == 0:
            int_end_deck +=1
            list_end_index.append(i)
            
    list_start_end_index = [list_start_index, list_end_index]
    return(list_start_end_index)
# ============================================================


# ------------------------------------------------------------
def fn_fix_deck_left_abut(df_deck_connect, flt_tolerance, list_start_index):

    """
    extend the left abutment to the ground if a ground elevation point is found
    within the specified vertical tolerance

    Args:
        df_deck: pands dataframe: contains sta, ground_elv, deck_elev as float
        flt_tolerance: maximum vertical difference between deck and ground where a connection is valid
        list_start_index: list of panda indecies where the left deck starts
           list_start_index[0] is the left most point on deck
        
    Returns:
        list of deck elevations (floats)
    """
    
    # TODO - changed "df_deck" to "df_deck_connect" - 2022.11.09 - May cause errors
    
    # default list to return if nothing needs to be revised
    list_return_deck = df_deck_connect['deck_elev'].to_list()
    
    # work backwards from list_start_index in df_deck
    # if ground elevation is within flt_tolerance then
    # this is the point to connect the deck

    # elevation of the left most deck point
    flt_abut_elev = df_deck_connect.iloc[list_start_index[0]]['deck_elev']

    #df_deck_connect = df_deck.copy()
    df_deck_connect['elev_delta_abut'] = flt_abut_elev - df_deck_connect['ground_elv']

    b_found_connection_point = False
    i = list_start_index[0]

    if abs(df_deck_connect.iloc[i]['elev_delta_abut']) > flt_tolerance:
        b_found_connection_point = False

        while not b_found_connection_point:
            if i < 0:
                # no connection found -  looked all the way to the beginning point
                b_found_connection_point = True
                int_connection_index = -1

            if abs(df_deck_connect.iloc[i]['elev_delta_abut']) < flt_tolerance:
                # connection point found
                b_found_connection_point = True
                int_connection_index = i
            i-=1 # subtract one from the counter

        if int_connection_index > -1:
            # connection point was found within vertical tolerance
            # before getting to the left most point
            df_deck_interpolate = df_deck_connect[['sta', 'ground_elv', 'deck_elev']].copy()

            # set point to interpolate on deck to nan
            for i in range(int_connection_index, list_start_index[0]):
                df_deck_interpolate.at[i, 'deck_elev'] = np.nan

            # interpolate the new deck points
            df_deck_interpolate['deck_elev_interp'] = df_deck_interpolate['deck_elev'].interpolate()

            # deck should be maximum between deck and ground
            df_deck_interpolate['max_deck_ground'] = df_deck_interpolate[["ground_elv", "deck_elev_interp"]].max(axis=1)

            list_return_deck = df_deck_interpolate['max_deck_ground'].to_list()
            
    return(list_return_deck)
# ------------------------------------------------------------


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def fn_fix_deck_right_abut(df_deck, flt_tolerance, list_end_index):
    
    """
    extend the left abutment to the ground if a ground elevation point is found
    within the specified vertical tolerance

    Args:
        df_deck: pands dataframe: contains sta, ground_elv, deck_elev as float
        flt_tolerance: maximum vertical difference between deck and ground where a connection is valid
        list_end_index: list of panda indecies where the deck ends
           list_end_index[-1] is the right most point on deck
        
    Returns:
        list of deck elevations (floats)
    """
    # default list to return if nothing needs to be revised
    list_return_deck = df_deck['deck_elev'].to_list()
    
    # elevation of the left most deck point
    flt_abut_elev = df_deck.iloc[list_end_index[-1]]['deck_elev']

    df_deck_connect = df_deck.copy()
    df_deck_connect['elev_delta_abut'] = flt_abut_elev - df_deck_connect['ground_elv']

    b_found_connection_point = False
    i = list_end_index[-1]

    if abs(df_deck_connect.iloc[i]['elev_delta_abut']) > flt_tolerance:
        b_found_connection_point = False

        while not b_found_connection_point:
            if i == len(df_deck_connect) - 1:
                # no connection found -  looked all the way to the beginning point
                b_found_connection_point = True
                int_connection_index = -1

            if abs(df_deck_connect.iloc[i]['elev_delta_abut']) < flt_tolerance:
                # connection point found
                b_found_connection_point = True
                int_connection_index = i
            i+=1 # add one to counter

        if int_connection_index > -1:
            # connection point was found within vertical tolerance
            # before getting to the left most point
            df_deck_interpolate = df_deck_connect[['sta', 'ground_elv', 'deck_elev']].copy()

            # set point to interpolate on deck to nan
            for i in range(list_end_index[-1], int_connection_index):
                df_deck_interpolate.at[i, 'deck_elev'] = np.nan

            # interpolate the new deck points
            df_deck_interpolate['deck_elev_interp'] = df_deck_interpolate['deck_elev'].interpolate()

            # deck should be maximum between deck and ground
            df_deck_interpolate['max_deck_ground'] = df_deck_interpolate[["ground_elv", "deck_elev_interp"]].max(axis=1)

            list_return_deck = df_deck_interpolate['max_deck_ground'].to_list()
            
    return(list_return_deck)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# ...................................................
def fn_fix_ground_nulls(gdf):

    # added 2023.09.11
    
    # Create an empty list to store rows with malformed values
    list_malformed_rows = []
    int_count_bad = 0

    # determine the number of malformed ground profiles
    for index, row in gdf.iterrows():
        ground_elv = row['ground_elv']
        try:
            ast.literal_eval(ground_elv)
        except (ValueError, SyntaxError):
            # If ast.literal_eval raises an exception, add the row to the list
            list_malformed_rows.append(index)
            int_count_bad += 1

    if len(list_malformed_rows) > 0:
        # correct the ground profile data
        print("+-----------------------------------------------------------------+")
        print('Fixing ground profiles: ' + str(len(list_malformed_rows)))
        
        for bad_index in list_malformed_rows:

            input_str = gdf.loc[bad_index]['ground_elv']

            # Remove brackets and split the string by ','
            values_str = input_str.strip('[]').split(',')

            # Convert values to floats, handling 'nan' as np.nan
            values = [float(val.strip()) if val.strip().lower() != 'nan' else np.nan for val in values_str]

            # Find the indices of 'nan' values
            nan_indices = [i for i, val in enumerate(values) if np.isnan(val)]

            # Replace 'nan' values with interpolated values
            for nan_index in nan_indices:
                # Find the nearest non-'nan' values before and after the current 'nan' value
                prev_index = nan_index
                while np.isnan(values[prev_index]):
                    prev_index -= 1
                next_index = nan_index
                while np.isnan(values[next_index]):
                    next_index += 1

                # Linear interpolation
                interpolated_value = np.interp(nan_index, [prev_index, next_index], [values[prev_index], values[next_index]])

                # Round the interpolated value to two decimal places
                interpolated_value = round(interpolated_value, 2)

                # Update the 'nan' value with the interpolated value
                values[nan_index] = interpolated_value

            # Convert the updated list of values back to a string
            updated_values_str = [str(val) for val in values]
            updated_str = '[' + ', '.join(updated_values_str) + ']'

            gdf.at[bad_index, 'ground_elv'] = updated_str
        return(gdf)
    else:
        # return the original geodataframe
        return(gdf)
# ...................................................    


# ---------------------------------------------------
def fn_compute_low_chord_attributes(str_input_dir):
    
    flt_default_thickness = 3.0 # hard coded thickness if there isn't a value
    flt_tolerance = 0.25 # vertical tolerance to connect the abutment to the ground
    
    # --- build file paths to the required input folders ---
    str_path_to_mjr_axis_gpkg = os.path.join(str_input_dir, '08_cross_sections', '08_05_mjr_axis_xs_w_feature_id_nbi.gpkg')

    if os.path.exists(str_path_to_mjr_axis_gpkg):
        # file is found
        
        gdf_mjr_read = gpd.read_file(str_path_to_mjr_axis_gpkg)
        
        # Fix nulls in ground elevation lists - 2023.09.11
        gdf_mjr = fn_fix_ground_nulls(gdf_mjr_read)
        
        print("+-----------------------------------------------------------------+")
        print("Calculating low chord...")
        
        gdf_mjr['low_ch_elv'] = '' # string list of low chord elevations
        gdf_mjr['convey_ar'] = '' # conveyance area below the low chord
        
        # intial deck creation before fixing abutments
        for index, row in gdf_mjr.iterrows():
            # --- create a low chord for each bridge ---
            str_bridge_thickness = row['nbi_thick']
            
            # set the default if no thickness or nbi found
            if str_bridge_thickness == '' or str_bridge_thickness == None:
                flt_bridge_thickness = flt_default_thickness
            else:
                flt_bridge_thickness = float(str_bridge_thickness)
                b_valid_nbi_thickness = True
            
            # create a list of the low chord elevations
            
            # note: ast.literal_eval(row['sta']) - converts string to list
            # create a pandas dataframe
            
            # Fixed 2023.09.11 - Ground elevation may contain nan
            
            

            df_bridge = pd.DataFrame(list(zip(ast.literal_eval(row['sta']),
                                              ast.literal_eval(row['ground_elv']),
                                              ast.literal_eval(row['deck_elev'])
                                             )),columns =['sta', 'ground_elv', 'deck_elev'])
            
            # -------------
            # given the df_bridge, get a list of the low chord
            # create an empty coloumn
            df_bridge['low_chord'] = df_bridge['deck_elev'] - flt_bridge_thickness
            df_bridge["max_ground_low_chord"] = df_bridge[["ground_elv", "low_chord"]].max(axis=1)
            list_low_chord = df_bridge["max_ground_low_chord"].to_list()
            
            # round the elevation values
            list_low_chord_round = [round(flt_elev, 2) for flt_elev in list_low_chord]
            
            # append the values to the dataframe
            gdf_mjr.at[index, 'low_ch_elv'] = str(list_low_chord_round)
        
            # compute the area under the bridge - before fixing abutments
            flt_conveyance_area = fn_area_under_bridge(df_bridge)
            
            # append the conveyance area - before fixing abutments
            gdf_mjr.at[index, 'convey_ar'] = round(flt_conveyance_area,1)
            
        print("+-----------------------------------------------------------------+")
        
        int_count = 0
        l = len(gdf_mjr)
        str_prefix = "Fix Abutments " + str(int_count) + ' of ' + str(l)
        fn_print_progress_bar(0, l, prefix = str_prefix , suffix = 'Complete', length = 29)
            
        
        # adding a coloumn
        gdf_mjr['min_low_ch'] = -99.0 # minimum low chord of bridge
        gdf_mjr['min_ground'] = 0.0 # minimum ground elevation
        
        # fixing the right and left abutments (armpit) where the ground does not meet the bridge deck
        for index, row in gdf_mjr.iterrows():
            time.sleep(0.05)
            int_count += 1
            str_prefix = "Fix Abutments " + str(int_count) + ' of ' + str(l)
            fn_print_progress_bar(int_count, l, prefix = str_prefix , suffix = 'Complete', length = 29)
            
            list_station = ast.literal_eval(row['sta'])
            list_ground_elv = ast.literal_eval(row['ground_elv'])
            list_deck_elev = ast.literal_eval(row['deck_elev'])
            
            df_deck = pd.DataFrame(list(zip(list_station,
                                            list_ground_elv,
                                            list_deck_elev)),columns =['sta','ground_elv', 'deck_elev'])
            
        
            list_start_end_index = fn_list_of_start_end_deck_index(df_deck)
            list_start_index = list_start_end_index[0]
            list_end_index = list_start_end_index[1]
            
            
            
            if row['convey_ar'] > 0:
                if len(list_start_index) > 0:
                    list_new_deck = fn_fix_deck_left_abut(df_deck, flt_tolerance, list_start_index)
                    # create a new df_deck with the adjusted left abutment
                    df_deck = pd.DataFrame(list(zip(list_station,
                                        list_ground_elv,
                                        list_new_deck)),columns =['sta','ground_elv', 'deck_elev'])
                    
                if len(list_end_index) > 0:
                    list_new_deck = fn_fix_deck_right_abut(df_deck, flt_tolerance, list_end_index)
                    # create a new df_deck with the adjusted left abutment
                    df_deck = pd.DataFrame(list(zip(list_station,
                                        list_ground_elv,
                                        list_new_deck)),columns =['sta','ground_elv', 'deck_elev'])
        
        
                # round all the values in list
                list_new_deck_round = [round(flt_elev, 2) for flt_elev in list_new_deck]
                
                # list to string
                str_list_new_deck = str(list_new_deck_round)
                
                # append the gdf_mjr at index with new str_list_new_deck
                gdf_mjr.at[index, 'deck_elev'] = str_list_new_deck
                
                # -------------------
                # compute a new low chord
                str_bridge_thickness = row['nbi_thick']
            
                # set the default if no thickness or nbi found
                b_valid_nbi_thickness = False
                if str_bridge_thickness == '' or str_bridge_thickness == None:
                    flt_bridge_thickness = flt_default_thickness
                else:
                    flt_bridge_thickness = float(str_bridge_thickness)
                    b_valid_nbi_thickness = True
                
                # given the df_deck, get a list of the low chord
                df_deck['low_chord'] = df_deck['deck_elev'] - flt_bridge_thickness
                df_deck["max_ground_low_chord"] = df_deck[["ground_elv", "low_chord"]].max(axis=1)
                list_low_chord = df_deck["max_ground_low_chord"].to_list()
                
                # round the elevation values
                list_low_chord_round = [round(flt_elev, 2) for flt_elev in list_low_chord]
        
                # append the values to the dataframe
                gdf_mjr.at[index, 'low_ch_elv'] = str(list_low_chord_round)
                # -------------------
                
                # ...................
                # compute the area under the bridge - after fixing abutments
                flt_conveyance_area = fn_area_under_bridge(df_deck)
        
                # append the conveyance area - after fixing abutments
                gdf_mjr.at[index, 'convey_ar'] = round(flt_conveyance_area, 2)
                # ...................
                
                # ~~~~~~~~~~
                # determine the minimum low chord elevation
                # diference between ground and low chord
                df_deck['low_above_ground'] = df_deck['max_ground_low_chord'] - df_deck['ground_elv']
        
                # filter to drop rows where ground is equal to low choord
                df_deck_filtered = df_deck[df_deck['low_above_ground'] > 0.01]
        
                # get the minimum elevation value of low chord
                flt_min_low_chord = df_deck_filtered['max_ground_low_chord'].min()
                
                # append the minimum low chord
                gdf_mjr.at[index, 'min_low_ch'] = round(flt_min_low_chord, 2)
                # ~~~~~~~~~~
                
            # append the minimum ground elevation
            gdf_mjr.at[index, 'min_ground'] = round(df_deck['ground_elv'].min(), 2)
        
        # ------- Exporting the revised attributed major axis lines
        str_path_xs_folder = os.path.join(str_input_dir, '08_cross_sections')
                
        # create the output directory if it does not exist
        os.makedirs(str_path_xs_folder, exist_ok=True)
        
        str_major_axis_xs_file = os.path.join(str_path_xs_folder, '08_06_mjr_axis_xs_w_feature_id_nbi_low.gpkg')
        
        # export the geopackage
        gdf_mjr.to_file(str_major_axis_xs_file, driver='GPKG')
        
        print("+-----------------------------------------------------------------+")
            
    else:
        print("  ERROR: Required File Not Found: " + str_path_to_mjr_axis_gpkg)
# ---------------------------------------------------


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='================= COMPUTE LOW CHORD ATTRIBUTES ===================')
    
    parser.add_argument('-i',
                        dest = "str_input_dir",
                        help=r'REQUIRED: input directory of processed data for area of interest [contains 00 to 08 folders]: Example: C:\bridge_data\folder_location',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    args = vars(parser.parse_args())
    
    str_input_dir = args['str_input_dir']
    
    print(" ")
    print("+=================================================================+")
    print("|       COMPUTE LOW CHORD ATTRIBUTES FOR MAJOR AXIS LINE          |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) PATH TO INPUT FOLDERS: " + str_input_dir)
    print("===================================================================")

    fn_compute_low_chord_attributes(str_input_dir)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~