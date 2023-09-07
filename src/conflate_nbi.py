# From the '08_02_mjr_axis_xs_w_feature_id.gpkg' 
# assign the National Bridge Inventory (nbi) to each major axis line
#
# Created by: Andy Carter, PE
# Created - 2022.11.09
# Last revised - 2022.11.09
#
# tx-bridge - sub-process of the 8th processing script
# Uses the 'pdal' conda environment

# ************************************************************

# ************************************************************
import argparse

import geopandas as gpd
import pandas as pd
import numpy as np
import ast # converting sting of list to list

import os

import difflib # compare two string and score

import time
import datetime
# ************************************************************


# ````````````````````````````````````````
def fn_filelist(source, tpl_extenstion):
    # walk a directory and get files with suffix
    # returns a list of file paths
    # args:
    #   source = path to walk
    #   tpl_extenstion = tuple of the extensions to find (Example: (.tig, .jpg))
    #   str_dem_path = path of the dem that needs to be converted
    matches = []
    for root, dirnames, filenames in os.walk(source):
        for filename in filenames:
            if filename.endswith(tpl_extenstion):
                matches.append(os.path.join(root, filename))
    return matches
# ````````````````````````````````````````


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


# ==============================================
def fn_percent_difference(flt_input_1, flt_input_2):
    # note: both values must be greater than zero
    if flt_input_1 <= 0 or flt_input_2 <= 0:
        return(-1)
    else:
        flt_numerator = abs(flt_input_1 - flt_input_2)
        flt_denominator = (flt_input_1 + flt_input_2) / 2
        flt_decimal_difference = flt_numerator / flt_denominator
        flt_perct_difference = flt_decimal_difference * 100
        return(flt_perct_difference)
# ==============================================
    

# ----------------------------------------------
def fn_dec_similar(flt_perct_difference):
    if flt_perct_difference < 100 and flt_perct_difference > 0:
        flt_dec_similar = 1 - (flt_perct_difference / 100)
        return(flt_dec_similar)
    else:
        # percent differnce is either greater than 1 OR percent difference has an error
        return(0)
# ----------------------------------------------


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def fn_calc_match_score(gdf_mjr_axis_with_current_nbi, gdf_current_nbi, flt_nearest_snap_dist):

    # get pairing information for the current nbi point

    # a matching nbi per 'Assest Name' was found
    str_nbi_roadname = gdf_current_nbi.iloc[0]['Facility_C'].lower()
    str_nbi_crossing = gdf_current_nbi.iloc[0]['Feature_In'].lower()
    flt_nbi_span_length = gdf_current_nbi.iloc[0]['Structur_1']
    
    flt_max_score = 0.0

    # get a list of gdf_current_duplicate indecies
    list_nbi = gdf_mjr_axis_with_current_nbi.index.values.tolist()
    
    for index, row in gdf_mjr_axis_with_current_nbi.iterrows():
        # loop through the geodataframe of major axis lines
        
        # ------------
        flt_dist_from_nbi = row['nbi_dist']
        # determine a weighted value from 0 to 1 where 1 = "on the line"
        # and 0 = 'flt_nearest_snap_dist' from line (max allowed sanp distance)
        flt_dist_score = (-1 * flt_dist_from_nbi / flt_nearest_snap_dist) + 1
        
        # -------------
        flt_hull_len = row['hull_len']
        # determine the percent similar span length (ranking from 0 to 1)
        flt_similarity_len = fn_dec_similar(fn_percent_difference(flt_hull_len, flt_nbi_span_length))
        
        # -------------
        # match Open Street Map name/ref to the NBI Name
        b_valid_road_name = False
        
        if row['name'] != None:
            str_name = str(row['name']).lower()
            seq=difflib.SequenceMatcher(a=str_name, b=str_nbi_roadname)
            flt_name_match_score = seq.ratio()
            b_valid_road_name = True
        else:
            flt_name_match_score = 0
        
        if row['ref'] != None:
            str_ref = str(row['ref']).lower()
            seq=difflib.SequenceMatcher(a=str_ref, b=str_nbi_roadname)
            flt_ref_match_score = seq.ratio()
            b_valid_road_name = True
        else:
            flt_ref_match_score = 0
            
        flt_highest_road_name_match = max(flt_name_match_score, flt_ref_match_score)
        # -------------
        
        # ------------
        # determine if the "flt_nhd_name_match_score" should be given weight in 'score'
        b_valid_stream = False
        if row['dist_river'] <= flt_nearest_snap_dist:
            # stream is within the "flt_nearest_snap_dist"
            b_valid_stream = True
        
        if row['nhd_name'] == '99-No NHD Streams':
            b_valid_stream = False
        
        # score of name of the stream (nhd vs nbi_crossing) that bridge is crossing
        if row['nhd_name'] != None:
            str_nhd_name = str(row['nhd_name']).lower()
            seq=difflib.SequenceMatcher(a=str_nhd_name, b=str_nbi_crossing)
            flt_nhd_name_match_score = seq.ratio()
        else:
            b_valid_stream = False
            str_nhd_name = ''
            flt_nhd_name_match_score = 0
        # ------------
        
        
        # ------------
        # weighting of paramters for scoring
        flt_dist_weight = 1.0 # weight of the distance between nbi and mjr_axis
        flt_len_weight = 2.0  # weight of the difference between nbi span and 'hull_len'
        flt_road_name_weight = 0.2 # weight string matching (nbi road and OpenStreetMap)
        flt_stream_name_weight = 0.2 # weight string matching (nbi road and nhd)
        
        flt_total_weight = flt_dist_weight + flt_len_weight
        flt_aggregate_score = (flt_dist_score * flt_dist_weight) + (flt_similarity_len * flt_len_weight)
        
        if b_valid_road_name:
            flt_total_weight += flt_road_name_weight
            flt_aggregate_score += (flt_highest_road_name_match * flt_road_name_weight)
        
        if b_valid_stream:
            flt_total_weight += flt_stream_name_weight
            flt_aggregate_score += (flt_nhd_name_match_score * flt_stream_name_weight)
        
        flt_match_score =  flt_aggregate_score / flt_total_weight
        
        if flt_match_score >= flt_max_score:
            flt_max_score = flt_match_score
            int_best_index = index
            dict_best_match = {'best_index': int_best_index,
                               'max_score': flt_max_score,
                              'dist_score': flt_dist_score,
                              'similar_len_score': flt_similarity_len,
                              'road_match_score': flt_highest_road_name_match,
                              'stream_score': flt_nhd_name_match_score,
                              'list_nbi_remove': []}
            
    # remove the int_best_index from the list_nbi
    list_nbi.remove(int_best_index)
    
    # update the list in the dictionary
    dict_best_match.update({'list_nbi_remove': list_nbi})
    
    return(dict_best_match)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# -----------------------------------------
def fn_determine_nbi(list_input_files):
    
    # maximum distance nbi points and major axis line can be from each other to "conflate"
    flt_nearest_snap_dist = 200
    
    str_mjr_axis_ln_shp = list_input_files[0]
    str_aoi_ar_shp = list_input_files[1]
    str_texas_nbi_shp = list_input_files[2]
    str_input_dir = list_input_files[3]
    
    print("+-----------------------------------------------------------------+")
    print('Loading the National Bridge Inventroy Data ~ 15 sec')
    # read the input shapefiles
    gdf_major_axis_ln = gpd.read_file(str_mjr_axis_ln_shp)
    gdf_aoi_ar = gpd.read_file(str_aoi_ar_shp)
    gdf_nbi_pnt = gpd.read_file(str_texas_nbi_shp)
    
    # convert area-of-interest and nbi points to gdf_major_axis_ln projection
    gdf_aoi_ar_local_prj = gdf_aoi_ar.to_crs(gdf_major_axis_ln.crs)
    gdf_nbi_pnt_local_prj = gdf_nbi_pnt.to_crs(gdf_major_axis_ln.crs)
    
    print("+-----------------------------------------------------------------+")
    print('Clipping NBI to area of interest...')
    
    # clip the nbi points to the area-of-interest
    gdf_nbi_pnts_in_aoi_local_prj = gpd.clip(
        gdf_nbi_pnt_local_prj,  gdf_aoi_ar_local_prj, keep_geom_type=True)
    
    
    
    # save a copy of the nbi points within the area of interest
    str_path_xs_folder = os.path.join(str_input_dir, '08_cross_sections')
    str_major_axis_xs_file = os.path.join(str_path_xs_folder, '08_04_nbi_pt_in_aoi.gpkg')
    
    # export the geopackage
    gdf_nbi_pnts_in_aoi_local_prj.to_file(str_major_axis_xs_file, driver='GPKG')
    
    gdf_major_axis_ln_nearest_nbi = gdf_major_axis_ln.copy()

    # add the nbi fields
    gdf_major_axis_ln_nearest_nbi['nbi_asset'] = ''
    gdf_major_axis_ln_nearest_nbi['nbi_thick'] = ''
    gdf_major_axis_ln_nearest_nbi['nbi_dist'] = ''
    
    print("+-----------------------------------------------------------------+")
    int_count = 0
    l = len(gdf_major_axis_ln)
    str_prefix = "Find NBI " + str(int_count) + ' of ' + str(l)
    fn_print_progress_bar(0, l, prefix = str_prefix , suffix = 'Complete', length = 29)
    
    for index, row in gdf_major_axis_ln.iterrows():
        
        time.sleep(0.05)
        int_count += 1
        str_prefix = "Find NBI " + str(int_count) + ' of ' + str(l)
        fn_print_progress_bar(int_count, l, prefix = str_prefix , suffix = 'Complete', length = 29)
        
        # get the the nearest point in gdf_nbi_pnt
        pd_dist_to_nearest_nbi = gdf_nbi_pnts_in_aoi_local_prj.distance(row['geometry']).sort_values()
        
        # distance to nearest nbi
        flt_nearest_nbi_dist = pd_dist_to_nearest_nbi.iloc[0]
        
        # index in gdf_nbi_pnts_in_aoi_local_prj of nearest nbi
        int_nearest_nbi_index = gdf_nbi_pnts_in_aoi_local_prj.distance(row['geometry']).sort_values().index[0]
        
        if flt_nearest_nbi_dist < flt_nearest_snap_dist:
            # nbi point is within the provided search radius
            # append the gdf_major_axis_ln
            gdf_major_axis_ln_nearest_nbi.at[index, 'nbi_asset'] = gdf_nbi_pnts_in_aoi_local_prj.loc[int_nearest_nbi_index]['Asset_Name']
            gdf_major_axis_ln_nearest_nbi.at[index, 'nbi_thick'] = gdf_nbi_pnts_in_aoi_local_prj.loc[int_nearest_nbi_index]['Bridge_Thi']
            gdf_major_axis_ln_nearest_nbi.at[index, 'nbi_dist'] = round(flt_nearest_nbi_dist, 2)
    
    # -----determine conflation score (likehood of correct match)-----
    gdf_mjr = gdf_major_axis_ln_nearest_nbi.copy()
    gdf_mjr[['score', 'score_dist', 'score_span', 'score_road', 'score_strm']] = 0.0
    
    # select only records with an 'nbi_asset' value
    gdf_mjr_has_nbi = gdf_mjr[gdf_mjr['nbi_asset'] != ''].copy()
    
    # get a list of the unique nbi_assest - there may be duplicates
    arr_nbi_unique = gdf_mjr_has_nbi.nbi_asset.unique()
    
    for str_current_nbi in arr_nbi_unique:
        
        # get a geodataframe major_axis lines with the 'str_current_nbi'
        gdf_mjr_axis_with_current_nbi = gdf_mjr_has_nbi.loc[gdf_mjr_has_nbi['nbi_asset'] == str_current_nbi]
        
        # get a geodataframe of nbi with the 'Asset_Name' = str_current_nbi
        gdf_current_nbi = gdf_nbi_pnts_in_aoi_local_prj.loc[gdf_nbi_pnts_in_aoi_local_prj['Asset_Name'] == str_current_nbi] 
        
        # get a dictionary of the 'most likely' nbi match
        dict_best_match = fn_calc_match_score(gdf_mjr_axis_with_current_nbi, gdf_current_nbi, flt_nearest_snap_dist)
        
        int_index_to_append = dict_best_match['best_index']
        gdf_mjr.at[int_index_to_append, 'score'] = round(dict_best_match['max_score'],3)
        gdf_mjr.at[int_index_to_append, 'score_dist'] = round(dict_best_match['dist_score'],3)
        gdf_mjr.at[int_index_to_append, 'score_span'] = round(dict_best_match['similar_len_score'],3)
        gdf_mjr.at[int_index_to_append, 'score_road'] = round(dict_best_match['road_match_score'],3)
        gdf_mjr.at[int_index_to_append, 'score_strm'] = round(dict_best_match['stream_score'],3)
        
        if len(dict_best_match['list_nbi_remove']) > 0:
            list_to_remove = dict_best_match['list_nbi_remove']
            
            for index_to_revise in list_to_remove:
                gdf_mjr.at[index_to_revise, 'nbi_asset'] = ''
                gdf_mjr.at[index_to_revise, 'nbi_thick'] = ''
                gdf_mjr.at[index_to_revise, 'nbi_dist'] = ''
    
    return(gdf_mjr)
    
# -----------------------------------------

# -------------------------------------------------------------
def fn_conflate_nbi(str_input_dir, str_texas_nbi_filepath):

    # --- build file paths to the required input folders ---
    str_path_to_mjr_axis_gpkg = os.path.join(str_input_dir, '08_cross_sections', '08_02_mjr_axis_xs_w_feature_id.gpkg')
    
    str_path_to_aoi_folder = os.path.join(str_input_dir, '00_input_shapefile')
    str_aoi_shapefile_path = ''
    
    # find a shapefile in the str_path_to_aoi_folder and get list
    list_shapefiles = fn_filelist(str_path_to_aoi_folder, ('.SHP', '.shp'))
    if len(list_shapefiles) > 0:
        str_aoi_shapefile_path = list_shapefiles[0]
    
    list_input_files = [str_path_to_mjr_axis_gpkg, str_aoi_shapefile_path, str_texas_nbi_filepath]
    
    # --- check to see if all the required input files exist ---
    list_files_exist = []
    for str_file in list_input_files:
        list_files_exist.append(os.path.isfile(str_file))
    
    if all(list_files_exist):
        # all input files were found
        list_input_files.append(str_input_dir)
        
        gdf_mjr = fn_determine_nbi(list_input_files)
        
        # ------- Exporting the revised attributed major axis lines
        str_path_xs_folder = os.path.join(str_input_dir, '08_cross_sections')
                
        # create the output directory if it does not exist
        os.makedirs(str_path_xs_folder, exist_ok=True)
        
        str_major_axis_xs_file = os.path.join(str_path_xs_folder, '08_05_mjr_axis_xs_w_feature_id_nbi.gpkg')
        
        # export the geopackage
        gdf_mjr.to_file(str_major_axis_xs_file, driver='GPKG')
        
        #print(gdf_mjr)
        
    else:
        int_item = 0
        for item in list_files_exist:
            if not item:
                print(" ERROR: Input file not found: " + list_input_files[int_item])
            int_item += 1
    
# -------------------------------------------------------------


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='============ CONFLATE NBI POINTS TO MAJOR AXIS LINES ==============')
    
    parser.add_argument('-i',
                        dest = "str_input_dir",
                        help=r'REQUIRED: input directory of processed data for area of interest [contains 00 to 08 folders]: Example: C:\bridge_data\folder_location',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    # TODO - Note the hard coded path to the file path for Tim's bridge shapefile (NBI) - 2022.11.09
    parser.add_argument('-n',
                        dest = "str_texas_nbi_filepath",
                        help=r'OPTIONAL: directory to national water model input datasets: Example: G:\X-NBI\nbi_bridges_texas_4326.shp',
                        required=False,
                        default=r'G:\X-NBI\nbi_bridges_texas_4326.shp',
                        metavar='PATH',
                        type=str)
    
    args = vars(parser.parse_args())
    
    str_input_dir = args['str_input_dir']
    str_texas_nbi_filepath = args['str_texas_nbi_filepath']
    
    print(" ")
    print("+=================================================================+")
    print("|    CONFLATE NATIONAL BRIDGE INVENTORY TO MAJOR AXIS LINES       |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) PATH TO INPUT FOLDERS: " + str_input_dir)
    print("  ---[n]   Optional: PATH TO TEXAS NBI: " + str_texas_nbi_filepath)
    print("===================================================================")

    fn_conflate_nbi(str_input_dir, str_texas_nbi_filepath)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~