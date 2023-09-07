# From the '08_01_mjr_axis_xs.gpkg' created with 'attribute_major_axis.py'
# assign the National Water Model Feature ID to each bridge
#
# Created by: Andy Carter, PE
# Created - 2022.11.08
# Last revised - 2023.007.27
#
# tx-bridge - sub-process of the 8th processing script
# Uses the 'pdal' conda environment

# ************************************************************
import argparse

import geopandas as gpd
import pandas as pd
import os
import xarray as xr
from shapely.geometry import Point, mapping
import uuid
import numpy as np
import json
import configparser

import warnings

import time
import datetime
# ************************************************************


# ````````````````````````````````````````
def fn_json_from_ini(str_ini_path):
    # Read the INI file
    config = configparser.ConfigParser()
    config.read(str_ini_path)
    
    # Convert to a dictionary
    config_dict = {section: dict(config[section]) for section in config.sections()}
    
    # Convert to JSON
    json_data = json.dumps(config_dict, indent=4)
    
    return(json_data)
# ````````````````````````````````````````



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

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def fn_determine_feature_id(list_input_files):
    
    print("+-----------------------------------------------------------------+")
    print('Assigning National Water Model Stream ID to major axis lines...')
    
    nwm_prj = "ESRI:102039"

    str_mjr_axis_ln_shp = list_input_files[0]
    str_aoi_ar_shp = list_input_files[1]
    str_nwm_flowline_geopkg_path = list_input_files[2]
    str_netcdf_path = list_input_files[3]
    str_input_dir = list_input_files[4]


    gdf_aoi_ar = gpd.read_file(str_aoi_ar_shp)
    
    # convert area of interest to nwm projection
    gdf_aoi_ar_nwm_prj = gdf_aoi_ar.to_crs(nwm_prj)
    
    gdf_mjr_axis_ln = gpd.read_file(str_mjr_axis_ln_shp)
    
    # get string of major axis projection
    local_prj = str(gdf_mjr_axis_ln.crs)
    
    # assign a uuid to every major axis line
    gdf_mjr_axis_ln['uuid'] = [str(uuid.uuid4()) for _ in range(len(gdf_mjr_axis_ln.index))]
    
    # Union of aoi polygons - creates shapely polygon
    shp_aoi_union = gdf_aoi_ar_nwm_prj.geometry.unary_union
    
    # Create dataframe of the bounding coordiantes
    tuple_aoi_extents = shp_aoi_union.bounds
    
    # Read Geopackage with bounding box filter
    gdf_stream_nwm_prj = gpd.read_file(str_nwm_flowline_geopkg_path,
                                       bbox=tuple_aoi_extents)
    
    # rename ID to feature_id
    gdf_stream_nwm_prj = gdf_stream_nwm_prj.rename(columns={"ID": "feature_id"})
    
    # Load the netCDF file to pandas dataframe - 15 seconds
    print("+-----------------------------------------------------------------+")
    print('Loading the National Water Model Lookup Table ~ 15 sec')
    ds = xr.open_dataset(str_netcdf_path)
    df_all_nwm_streams = ds.to_dataframe()
    print("+-----------------------------------------------------------------+")
    
    # left join the stream geodataframe with the recurrance dataFrame
    # to get the feature_id on the stream features
    gdf_stream_nwm_prj = gdf_stream_nwm_prj.merge(df_all_nwm_streams,
                                                  on='feature_id',
                                                  how='left')
    
    # clip the streams to the area of interest
    # should be same crs... cleaning up to supress crs mismatch error
    gdf_stream_nwm_prj = gdf_stream_nwm_prj.to_crs(gdf_aoi_ar_nwm_prj.crs)
    
    gdf_stream_in_aoi_nwm_prj = gpd.overlay(
        gdf_stream_nwm_prj, gdf_aoi_ar_nwm_prj, how='intersection')
    
    # reproject to major hull crs
    gdf_stream_in_aoi_input_prj = gdf_stream_in_aoi_nwm_prj.to_crs(gdf_mjr_axis_ln.crs)
    
    # filter to only selected coloumns
    gdf_stream_in_aoi_prj = gdf_stream_in_aoi_input_prj[['feature_id','order_', 'geometry']]
    
    # set of points where the major axis lines intersect the streams
    points = gdf_stream_in_aoi_prj.unary_union.intersection(gdf_mjr_axis_ln.unary_union)
    
    b_have_points = False
    
    if points.geom_type == 'MultiPoint' or points.geom_type == 'Point':
        gs_multipoints = gpd.GeoSeries(points)
        gs_points = gs_multipoints.explode(index_parts=False)
        
        gdf_points = gpd.GeoDataFrame(geometry=gpd.GeoSeries(gs_points))
        gdf_points.crs = gdf_mjr_axis_ln.crs
        
        # need to reindex the returned geoDataFrame
        gdf_points = gdf_points.reset_index(drop=True)
        
        b_have_points = True
        
    # Need to supress warnings
    warnings.simplefilter(action='ignore', category=FutureWarning)
    
    # buffer to get stream attributes on points
    gdf_stream_in_aoi_prj_buffer = gdf_stream_in_aoi_prj.copy()
    gdf_stream_in_aoi_prj_buffer['geometry'] = gdf_stream_in_aoi_prj_buffer.geometry.buffer(0.1)
    
    if b_have_points:
        
        print('Determining nearest feature ID...')
        # TODO - if there are no points (gdf_points) this returns an error: 2022.12.29
        # Spatial join of the points and buffered stream
        gdf_intersection_points_feature_id = gpd.sjoin(
            gdf_points,
            gdf_stream_in_aoi_prj_buffer,
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
    
        # A major axis line could cross more than one stream
        # each uuid should be unique... if not delete all but the highest stream order 'order_'
        
        # determine major axis lines with two or more stream points
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
                max_index = gdf_matching['order_'].idxmax()
        
                list_index_to_keep.append(max_index)
        
            # list of all the gdf_duplicates indecies
            list_all_index = gdf_duplicates.index.tolist()
        
            # create a list of indecies to remove from gdf_intersection_points_major_axis
            list_index_to_drop = np.setdiff1d(list_all_index,list_index_to_keep).tolist()
            
            # remove the duplicates on gdf_intersection_points_major_axis 
            gdf_intersection_points_major_axis = gdf_intersection_points_major_axis.drop(index=list_index_to_drop)
            
            #print(str(len(list_index_to_drop)) + " duplicate stream crossings removed.")
    
        # create a dist_river coloumn in gdf_intersection_points_major_axis and set to '0'
        gdf_intersection_points_major_axis['dist_river'] = 0
        
        # left join the major axis lines with points to get feature_id and order on lines
    
        # left join the stream geodataframe with the recurrance dataFrame
        # to get the feature_id on the stream features
        gdf_stream_merge_crossing = gdf_mjr_axis_ln.merge(gdf_intersection_points_major_axis[['feature_id', 'order_', 'dist_river','uuid']],
                                                   on='uuid',
                                                   how='left')
    
        # dataframe where a 'feature_id' was assigned -- 'feature_id' is not null
        gdf_crosses_stream = gdf_stream_merge_crossing[gdf_stream_merge_crossing['feature_id'].notna()]
        
        # dataframe where a 'feature_id' was not assigned -- 'feature_id' is null
        gdf_find_nearest_stream = gdf_stream_merge_crossing[gdf_stream_merge_crossing['feature_id'].isna()]
    
    
        # if no match found, search for the nearest stream and populate feature_id and search distance
    
        for index, row in gdf_find_nearest_stream.iterrows():
            # get the the nearest stream in gdf_stream_in_aoi_prj
            pd_dist_to_nearest_stream = gdf_stream_in_aoi_prj.distance(row['geometry']).sort_values()
            
            # distance to nearest stream
            flt_nearest_stream_dist = pd_dist_to_nearest_stream.iloc[0]
            
            # index in gdf_stream_in_aoi_prj of nearest stream
            int_nearest_stream_index = gdf_stream_in_aoi_prj.distance(row['geometry']).sort_values().index[0]
            
            # append gdf_mjr_axis_ln with the 'feature_id', 'order_' and 'dist_river' of the nearest stream line
            
            gdf_find_nearest_stream.at[index, 'feature_id'] = gdf_stream_in_aoi_prj.loc[int_nearest_stream_index]['feature_id']
            gdf_find_nearest_stream.at[index, 'order_'] = gdf_stream_in_aoi_prj.loc[int_nearest_stream_index]['order_']
            gdf_find_nearest_stream.at[index, 'dist_river'] = flt_nearest_stream_dist
            
        # combine the gdf_find_nearest_stream and gdf_crosses_stream
        gdf_mjr_axis_ln_attributed = pd.concat([gdf_find_nearest_stream, gdf_crosses_stream])

    else:
        # no intersecting points were found
        print('No Intersection of stream and Major axis lines found...')
        print('Determining nearest feature ID...')
        gdf_mjr_axis_ln_attributed = gdf_mjr_axis_ln.copy()
        
        # create empty coloumns
        gdf_mjr_axis_ln_attributed['feature_id'] = ''
        gdf_mjr_axis_ln_attributed['order_'] = '' 
        gdf_mjr_axis_ln_attributed['dist_river'] = ''
        
        for index, row in gdf_mjr_axis_ln_attributed.iterrows():
            # get the the nearest stream in gdf_stream_in_aoi_prj
            pd_dist_to_nearest_stream = gdf_stream_in_aoi_prj.distance(row['geometry']).sort_values()
            
            # distance to nearest stream
            flt_nearest_stream_dist = pd_dist_to_nearest_stream.iloc[0]
            
            # index in gdf_stream_in_aoi_prj of nearest stream
            int_nearest_stream_index = gdf_stream_in_aoi_prj.distance(row['geometry']).sort_values().index[0]
            
            # append gdf_mjr_axis_ln with the 'feature_id', 'order_' and 'dist_river' of the nearest stream line
            
            gdf_mjr_axis_ln_attributed.at[index, 'feature_id'] = gdf_stream_in_aoi_prj.loc[int_nearest_stream_index]['feature_id']
            gdf_mjr_axis_ln_attributed.at[index, 'order_'] = gdf_stream_in_aoi_prj.loc[int_nearest_stream_index]['order_']
            gdf_mjr_axis_ln_attributed.at[index, 'dist_river'] = flt_nearest_stream_dist
            
    
    
    gdf_mjr_axis_ln_attributed["feature_id"] = gdf_mjr_axis_ln_attributed["feature_id"].astype(int)
    gdf_mjr_axis_ln_attributed["order_"] = gdf_mjr_axis_ln_attributed["order_"].astype(int)
    gdf_mjr_axis_ln_attributed["hull_len"] = gdf_mjr_axis_ln_attributed["hull_len"].astype(float)
    gdf_mjr_axis_ln_attributed["avg_width"] = gdf_mjr_axis_ln_attributed["avg_width"].astype(float)
    
    decimals = 2    
    gdf_mjr_axis_ln_attributed['dist_river'] = gdf_mjr_axis_ln_attributed['dist_river'].apply(lambda x: round(x, decimals))
    gdf_mjr_axis_ln_attributed['hull_len'] = gdf_mjr_axis_ln_attributed['hull_len'].apply(lambda x: round(x, decimals))
    gdf_mjr_axis_ln_attributed['avg_width'] = gdf_mjr_axis_ln_attributed['avg_width'].apply(lambda x: round(x, decimals))
    
    
    # ------- Exporting the revised attributed major axis lines
    str_path_xs_folder = os.path.join(str_input_dir, '08_cross_sections')
            
    # create the output directory if it does not exist
    os.makedirs(str_path_xs_folder, exist_ok=True)
    
    str_major_axis_xs_file = os.path.join(str_path_xs_folder, '08_02_mjr_axis_xs_w_feature_id.gpkg')
    
    # export the geopackage
    gdf_mjr_axis_ln_attributed.to_file(str_major_axis_xs_file, driver='GPKG')
    
    
    str_nwm_streams_file = os.path.join(str_path_xs_folder, '08_03_nwm_streams.geojson')
    gdf_stream_in_aoi_input_prj.to_file(str_nwm_streams_file,driver="GeoJSON")
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        
# --------------------------------------------------------
def fn_assign_feature_id_to_mjr_axis(str_input_dir,dict_global_config_data):
    
    """
    xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

    Args:
        str_input_dir: path that contains the processed input data 
        folders such as ... 00_input_shapefile ... to ... 07_major_axis_names
        
        dict_global_config_data: dictionary of Global INI file

    Returns:
        geojson file of the attributed major axis lines
    """
    

    # --- build file paths to the required input folders ---
    str_path_to_mjr_axis_gpkg = os.path.join(str_input_dir, '08_cross_sections', '08_01_mjr_axis_xs.gpkg')
    
    str_path_to_aoi_folder = os.path.join(str_input_dir, '00_input_shapefile')
    str_aoi_shapefile_path = ''
    
    # find a shapefile in the str_path_to_aoi_folder and get list
    list_shapefiles = fn_filelist(str_path_to_aoi_folder, ('.SHP', '.shp'))
    if len(list_shapefiles) > 0:
        str_aoi_shapefile_path = list_shapefiles[0]
        
    str_nwm_flowline_geopkg_path = dict_global_config_data['global_input_files']['str_nwm_flowlines_gpkg']
    str_netcdf_path = dict_global_config_data['global_input_files']['str_nwm_lookup_netcdf']
    
    list_input_files = [str_path_to_mjr_axis_gpkg, str_aoi_shapefile_path, str_nwm_flowline_geopkg_path, str_netcdf_path]
    
    
    # --- check to see if all the required input files exist ---
    list_files_exist = []
    for str_file in list_input_files:
        list_files_exist.append(os.path.isfile(str_file))
    
    if all(list_files_exist):
        # all input files were found
        list_input_files.append(str_input_dir)
        fn_determine_feature_id(list_input_files)
        
        pass
    else:
        int_item = 0
        for item in list_files_exist:
            if not item:
                print(" ERROR: Input file not found: " + list_input_files[int_item])
            int_item += 1
    
# --------------------------------------------------------

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='==== ASSIGN NATIONAL WATER MODEL STREAM IDS TO MAJOR AXIS LINES ===')
    
    parser.add_argument('-i',
                        dest = "str_input_dir",
                        help=r'REQUIRED: input directory of processed data for area of interest [contains 00 to 08 folders]: Example: C:\bridge_data\folder_location',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-g',
                        dest = "str_global_config_ini_path",
                        help=r'OPTIONAL: global variable initialization file: Example: C:\Users\civil\dev\tx-bridge\src\config_global.ini',
                        required=False,
                        default=r'C:\Users\civil\dev\tx-bridge\src\config_global.ini',
                        metavar='FILE',
                        type=str)
    
    args = vars(parser.parse_args())
    
    str_input_dir = args['str_input_dir']
    str_global_config_ini_path = args['str_global_config_ini_path']

    print(" ")
    print("+=================================================================+")
    print("|  ASSIGN NATIONAL WATER MODEL STREAM IDS TO MAJOR AXIS LINES     |")
    print("|                 FROM USER SUPPLIED DATASETS                     |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) PATH TO INPUT FOLDERS: " + str_input_dir)
    print("  ---[g]   Optional: GLOBAL CONFIG INI: " + str_global_config_ini_path)
    
    print("===================================================================")

    # convert the INI to a dictionary
    dict_global_config_data = json.loads(fn_json_from_ini(str_global_config_ini_path))

    fn_assign_feature_id_to_mjr_axis(str_input_dir,dict_global_config_data)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
 #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~