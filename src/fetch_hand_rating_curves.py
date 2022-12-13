# From the '08_07_mjr_axis_xs_w_feature_id_nbi_low.gpkg' 
# Using the HAND derived streams (demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg)
# and the HAND devived synthetic rating curves (hydroTable.csv),
# determine the rating curve for each major axis line
# NOTE - while flows are determined at each National Water Model - feature-id,
# each feature-id may be too long to get a good synthetic rating curve.  Others
# have broken many of the feature-id's into shorter reaches.  Each of these reaches
# has its own synthetic rating curve... in this case from "Height Above Nearest Drainage"
# workflow ... or "HAND"
#
# Created by: Andy Carter, PE
# Created - 2022.11.15
# Last revised - 2022.11.15
#
# tx-bridge - sub-process of the 8th processing script
# Uses the 'pdal' conda environment
# ************************************************************

# ************************************************************
import argparse

import geopandas as gpd
import pandas as pd

from shapely.geometry import Point, mapping
import numpy as np

import warnings
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


# ````````````````````````````````````````
def fn_filelist(source, tpl_extenstion):
    # walk a directory and get files with suffix
    # returns a list of file paths
    # args:
    #   source = path to walk
    #   tpl_extenstion = tuple of the extensions to find (Example: (.tig, .jpg))
    matches = []
    for root, dirnames, filenames in os.walk(source):
        for filename in filenames:
            if filename.endswith(tpl_extenstion):
                matches.append(os.path.join(root, filename))
    return matches
# ````````````````````````````````````````


# ...........................................................
def fn_merge(list1, list2):
    merged_list = [(list1[i], list2[i]) for i in range(0, len(list1))]
    return merged_list
# ...........................................................


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def fn_hydro_id_rating(str_segment_field_name, int_segment_id, str_hydro_table_csv):
    # given a str_segment_field_name - get a list of tuples of the rating curve
    
    df_hydro_table = pd.read_csv(str_hydro_table_csv)

    df_hydro_id = df_hydro_table.loc[df_hydro_table[str_segment_field_name] == int_segment_id]

    list_stage = df_hydro_id['stage'].tolist()
    list_discharge = df_hydro_id['discharge_cms'].tolist()

    list_stage_ft = [round(x * 3.28084,1) for x in list_stage]
    list_discharge_cfs = [round(x * 35.314666212661,1) for x in list_discharge]

    list_of_tuples = fn_merge(list_discharge_cfs, list_stage_ft)
    
    return(str(list_of_tuples))
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# .....................................................
def fn_determine_segment_id(list_input_files, str_segment_field_name):
    
    print('Getting HAND segment rating curves...')
    
    nwm_prj = "ESRI:102039"
    
    # Need to supress warnings
    warnings.simplefilter(action='ignore', category=FutureWarning)
    
    str_major_axis_lines = list_input_files[0]
    str_aoi_ar = list_input_files[1]
    str_hand_stream_ln = list_input_files[2]
    str_hydro_table_csv = list_input_files[3]
    str_input_dir = list_input_files[4]

    # for the area of interest polygon
    gdf_aoi_ar = gpd.read_file(str_aoi_ar)
    gdf_mjr_axis_ln = gpd.read_file(str_major_axis_lines)
    
    # convert the aoi to nwm projection
    gdf_aoi_ar_nwm_proj = gdf_aoi_ar.to_crs(nwm_prj)
    
    # Union of aoi polygons - creates shapely polygon
    shp_aoi_union = gdf_aoi_ar_nwm_proj.geometry.unary_union
    
    # Create dataframe of the bounding coordiantes
    tuple_aoi_extents = shp_aoi_union.bounds

    # Read hand stream line geopackage with bounding box filter
    gdf_hand_stream_nwm_prj = gpd.read_file(str_hand_stream_ln,
                                            bbox=tuple_aoi_extents)
    
    # clip the streams to the area of interest
    # should be same crs... cleaning up to supress crs mismatch error
    gdf_hand_stream_nwm_prj = gdf_hand_stream_nwm_prj.to_crs(gdf_aoi_ar_nwm_proj.crs)
    
    gdf_hand_stream_in_aoi_nwm_prj = gpd.overlay(gdf_hand_stream_nwm_prj,
                                             gdf_aoi_ar_nwm_proj, how='intersection')
    
    # reproject stream lines to major hull crs
    gdf_hand_stream_in_aoi_input_prj = gdf_hand_stream_in_aoi_nwm_prj.to_crs(gdf_mjr_axis_ln.crs)
    
    # filter stream lines geodataframe to only selected coloumns
    gdf_hand_stream_in_aoi_input_prj = gdf_hand_stream_in_aoi_input_prj[['feature_id',
                                                                         str_segment_field_name ,
                                                                         'order_',
                                                                         'geometry']]
    
    # --- determine intersecting points between two line features ---
    # merge all the stream lines into one shapely object
    shp_hand_stream_merge = gdf_hand_stream_in_aoi_input_prj.geometry.unary_union
    
    # merge all the major axis lines into one shapely object
    shp_mjr_axis_ln = gdf_mjr_axis_ln.geometry.unary_union
    
    
    # intersect the two shapely line groups
    shp_intersection_pt = shp_hand_stream_merge.intersection(shp_mjr_axis_ln)
    
    
    if shp_intersection_pt.geom_type == 'MultiPoint':
        gs_multipoints = gpd.GeoSeries(shp_intersection_pt)
        gs_points = gs_multipoints.explode(index_parts=False)
        
        gdf_points = gpd.GeoDataFrame(geometry=gpd.GeoSeries(gs_points))
        gdf_points.crs = gdf_mjr_axis_ln.crs
        
        # need to reindex the returned geoDataFrame
        gdf_points = gdf_points.reset_index(drop=True)
        
    # buffer to get stream attributes on points
    gdf_hand_stream_in_aoi_input_prj_buffer = gdf_hand_stream_in_aoi_input_prj.copy()
    gdf_hand_stream_in_aoi_input_prj_buffer['geometry'] = gdf_hand_stream_in_aoi_input_prj_buffer.geometry.buffer(0.1)
    
    # Spatial join of the points and buffered stream
    gdf_intersection_points_feature_id = gpd.sjoin(
        gdf_points,
        gdf_hand_stream_in_aoi_input_prj_buffer,
        how='left',
        op='intersects')
    
    # delete index_right
    del gdf_intersection_points_feature_id['index_right']
    
    
    # buffer the major axis lines to get attributes on points
    gdf_mjr_axis_ln_buffer = gdf_mjr_axis_ln.copy()
    gdf_mjr_axis_ln_buffer['geometry'] = gdf_mjr_axis_ln_buffer.geometry.buffer(0.1)
    
    # Spatial join the 'feature_id' attributed streams points with the buffers major axis
    gdf_intersection_points_major_axis = gpd.sjoin(
        gdf_intersection_points_feature_id,
        gdf_mjr_axis_ln_buffer,
        how='left',
        op='intersects')
    
    # delete index_right
    del gdf_intersection_points_major_axis['index_right']
    # ------
    
    
    # ---- determine major axis lines with two or more stream points ----
    # it doen't happen often, but it can happen
    gdf_duplicates = gdf_intersection_points_major_axis[gdf_intersection_points_major_axis.duplicated(subset=['uuid'], keep=False)]
    
    if len(gdf_duplicates) > 1:
        # there should be at least two rows
        
        # get a list of unique uuid of the duplicates
        arr_unique_uuid_duplicates = gdf_duplicates.uuid.unique()
        
        list_index_to_keep = []
    
        for x in arr_unique_uuid_duplicates:
            # dataframe of just the matching rows
            gdf_matching = gdf_duplicates.loc[gdf_duplicates['uuid'] == x]
    
            # determine the highest stream order
            # if stream orders are all the same... it just picks the first value
            # TODO - 2022.10.07 - Do we want the biggest drainage area?
            max_index = gdf_matching['order__right'].idxmax()
    
            list_index_to_keep.append(max_index)
    
        # list of all the gdf_duplicates indecies
        list_all_index = gdf_duplicates.index.tolist()
    
        # create a list of indecies to remove from gdf_intersection_points_major_axis
        list_index_to_drop = np.setdiff1d(list_all_index,list_index_to_keep).tolist()
        
        # remove the duplicates on gdf_intersection_points_major_axis 
        gdf_intersection_points_major_axis = gdf_intersection_points_major_axis.drop(index=list_index_to_drop)
        
        print(str(len(list_index_to_drop)) + " duplicate stream crossings removed.")
    # --------
    
    # create a dist_river coloumn in gdf_intersection_points_major_axis and set to '0'
    # because the major axis line intersects a stream segement line (at least once)
    gdf_intersection_points_major_axis['dst_new_rv'] = 0
    
    # left join the major axis lines with points to get str_segment_field_name and order on lines

    gdf_stream_merge_crossing = gdf_mjr_axis_ln.merge(gdf_intersection_points_major_axis[[str_segment_field_name ,
                                                                                          'order__left',
                                                                                          'dst_new_rv',
                                                                                          'feature_id_right',
                                                                                          'uuid']],
                                               on='uuid',
                                               how='left')
    
    # dataframe where a str_segment_field_name was assigned -- str_segment_field_name is not null
    gdf_crosses_stream = gdf_stream_merge_crossing[gdf_stream_merge_crossing[str_segment_field_name ].notna()]
    
    # dataframe where a str_segment_field_name was not assigned -- str_segment_field_name is null
    gdf_find_nearest_stream = gdf_stream_merge_crossing[gdf_stream_merge_crossing[str_segment_field_name ].isna()]
    
    # --- when a major axis line does not intersect a stream line
    # if no match found, search for the nearest stream and populate str_segment_field_name and search distance

    for index, row in gdf_find_nearest_stream.iterrows():
        # get the the nearest stream in gdf_stream_in_aoi_prj
        pd_dist_to_nearest_stream = gdf_hand_stream_in_aoi_input_prj.distance(row['geometry']).sort_values()
        
        # distance to nearest stream
        flt_nearest_stream_dist = pd_dist_to_nearest_stream.iloc[0]
        
        # index in gdf_stream_in_aoi_prj of nearest stream
        int_nearest_stream_index = gdf_hand_stream_in_aoi_input_prj.distance(row['geometry']).sort_values().index[0]
        
        # append gdf_mjr_axis_ln with the 'feature_id', 'order_' and 'dist_river' of the nearest stream line
        
        gdf_find_nearest_stream.at[index, str_segment_field_name ] = gdf_hand_stream_in_aoi_input_prj.loc[int_nearest_stream_index][str_segment_field_name]
        gdf_find_nearest_stream.at[index, 'order__left'] = gdf_hand_stream_in_aoi_input_prj.loc[int_nearest_stream_index]['order_']
        gdf_find_nearest_stream.at[index, 'dst_new_rv'] = round(flt_nearest_stream_dist, 2)
        gdf_find_nearest_stream.at[index, 'feature_id_right'] = gdf_hand_stream_in_aoi_input_prj.loc[int_nearest_stream_index]['feature_id']
        
    # combine the gdf_find_nearest_stream and gdf_crosses_stream
    gdf_mjr_axis_ln_attributed = pd.concat([gdf_find_nearest_stream, gdf_crosses_stream])
    
    # convert 'feature_id' to integer
    gdf_mjr_axis_ln_attributed['feature_id_right'] = gdf_mjr_axis_ln_attributed['feature_id_right'].astype(int)
    
    # ----add the ranting curve to the geodataframe ----
    int_count = 0
    l = len(gdf_mjr_axis_ln_attributed)
    str_prefix = "Fetch Rating " + str(int_count) + ' of ' + str(l)
    fn_print_progress_bar(0, l, prefix = str_prefix , suffix = 'Complete', length = 29)
    
    
    gdf_mjr_axis_ln_attributed['hand_r'] = ''

    for index, row in gdf_mjr_axis_ln_attributed.iterrows():
        # -- update progress bar --
        time.sleep(0.05)
        int_count += 1
        str_prefix = "Fetch Rating " + str(int_count) + ' of ' + str(l)
        fn_print_progress_bar(int_count, l, prefix = str_prefix , suffix = 'Complete', length = 29)
        
        if row['feature_id'] == row['feature_id_right']:
            # get the 'hard_r' rating curve for the str_segment_field_name
            
            str_list_of_tuples = fn_hydro_id_rating(str_segment_field_name,
                                                    int(row[str_segment_field_name]),
                                                    str_hydro_table_csv)
            
            row['hand_r'] = str_list_of_tuples
            gdf_mjr_axis_ln_attributed.at[index, 'hand_r'] = str_list_of_tuples
        else:
            # the feature_id from the NWM and the redelineation don't match
            # don't pull a hand_r rating curve
            pass
    # --------
    print('Saving output...')
    
    # ------- Exporting the revised attributed major axis lines
    str_path_xs_folder = os.path.join(str_input_dir, '08_cross_sections')
            
    # create the output directory if it does not exist
    os.makedirs(str_path_xs_folder, exist_ok=True)
    
    str_major_axis_xs_file = os.path.join(str_path_xs_folder, '08_08_mjr_axis_xs_w_feature_id_nbi_low_hull_rating.gpkg')
    
    # export the geopackage
    gdf_mjr_axis_ln_attributed.to_file(str_major_axis_xs_file, driver='GPKG')
    
    # export the geojson
    str_major_axis_xs_file = os.path.join(str_path_xs_folder, '08_08_mjr_axis_xs_w_feature_id_nbi_low_hull_rating.geojson')
    gdf_mjr_axis_ln_attributed.to_file(str_major_axis_xs_file, driver='GeoJSON')
    
    # export the stream segments
    str_stream_segments_file = os.path.join(str_path_xs_folder, '08_09_stream_segements.geojson')
    gdf_hand_stream_in_aoi_input_prj.to_file(str_stream_segments_file, driver='GeoJSON')
    # ----
    
    print('+-----------------------------------------------------------------+')
# .....................................................


# ----------------------------------------------------
def fn_fetch_hand_rating_curves(str_input_dir, str_input_hand_data_dir, str_segment_field_name):
    
    # --- build file paths to the required input folders ---
    str_major_axis_lines = os.path.join(str_input_dir, '08_cross_sections', '08_07_mjr_axis_xs_w_feature_id_nbi_low_hull.gpkg')
    
    str_path_to_aoi_folder = os.path.join(str_input_dir, '00_input_shapefile')
    str_aoi_ar = ''
    
    # find a shapefile in the str_path_to_aoi_folder and get list
    list_shapefiles = fn_filelist(str_path_to_aoi_folder, ('.SHP', '.shp'))
    if len(list_shapefiles) > 0:
        str_aoi_ar = list_shapefiles[0]
    
    str_hand_stream_ln = os.path.join(str_input_hand_data_dir, 'demDerived_reaches_split_filtered_addedAttributes_crosswalked.gpkg')
    str_hydro_table_csv = os.path.join(str_input_hand_data_dir, 'hydroTable_rp_bf_lmtdischarge_cda.csv')
    
    list_input_files = [str_major_axis_lines, str_aoi_ar, str_hand_stream_ln, str_hydro_table_csv]
    
    # --- check to see if all the required input files exist ---
    list_files_exist = []
    for str_file in list_input_files:
        list_files_exist.append(os.path.isfile(str_file))
    
    if all(list_files_exist):
        # all input files were found
        list_input_files.append(str_input_dir)
        
        fn_determine_segment_id(list_input_files, str_segment_field_name)
    else:
        int_item = 0
        for item in list_files_exist:
            if not item:
                print(" ERROR: Input file not found: " + list_input_files[int_item])
            int_item += 1

# ----------------------------------------------------


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    # segment id name - like HydroID or FatSgtID
    # representing the subdivision of the NWM streams
    str_segment_field_name = 'FATSGTID'
    
    parser = argparse.ArgumentParser(description='============== FETCH HAND SYNTHETIC RATING CURVES =================')
    
    parser.add_argument('-i',
                        dest = "str_input_dir",
                        help=r'REQUIRED: input directory of processed data for area of interest [contains 00 to 08 folders]: Example: C:\bridge_data\folder_location',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    # TODO - Note the hard coded path below - 2022.11.15
    
    parser.add_argument('-r',
                        dest = "str_input_hand_data_dir",
                        help=r'OPTIONAL: input directory to HAND data: Example: G:\X-ORNL-HAND\aus_txdot_hand_20221116',
                        required=False,
                        default=r'G:\X-ORNL-HAND\aus_txdot_hand_20221116',
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-s',
                        dest = "str_segment_field_name",
                        help=r'OPTIONAL: name of stream segment in HAND input data: Example: HydroID',
                        required=False,
                        default = str_segment_field_name,
                        metavar='STR',
                        type=str)
    
    args = vars(parser.parse_args())
    
    str_input_dir = args['str_input_dir']
    str_input_hand_data_dir = args['str_input_hand_data_dir']
    str_segment_field_name = args['str_segment_field_name']
    

    
    print(" ")
    print("+=================================================================+")
    print("|              FETCH HAND SYNTHETIC RATING CURVES                 |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) PATH TO INPUT FOLDERS: " + str_input_dir)
    print("  ---[h]   Optional: PATH TO HAND INPUT: " + str_input_hand_data_dir )
    print("  ---[s]   Optional: STREAM SEGMENT NAME: " + str_segment_field_name )
    print("===================================================================")

    fn_fetch_hand_rating_curves(str_input_dir, str_input_hand_data_dir, str_segment_field_name)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~