# Given an input polygon shapefile, get a shapefile of the bridge lines
# from OpenStreetMaps
#
# Created by: Andy Carter, PE
# Created - 2022.01.19
# Last revised - 2022.02.03

# ************************************************************
import argparse

import osmnx as ox
import geopandas as gpd
import pandas as pd
import shapely.geometry

from multiprocessing.pool import ThreadPool
from time import sleep

import os
import time
import datetime
# ************************************************************


# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
def is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist" % arg)
    else:
        # File exists so return the directory
        return arg
        return open(arg, 'r')  # return an open file handle
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
def fn_has_list(x):
    # from a dataframe, return a data series of the fields that have
    # a list in them (True / False)
    return any(isinstance(i, list) for i in x)
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~   
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
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~   

# -------------------------------------------------------
def fn_fetch_osm_data(list_input_data):
    
    wgs = "epsg:4326"
    # input is a list of lists with four values. 
    # ------
    bbox_current = list_input_data[0]
    str_output = list_input_data[1]
    source_crs = list_input_data[2]
    geom_current_wgs = list_input_data[3]
    
    # ------

    try:
        # added try call to skip polygons where nothing is returned
        G = ox.graph_from_polygon(bbox_current,
                          simplify=True,
                          retain_all=True,
                          custom_filter='["bridge"~"yes"]')

        # convert the graph to a projected graph
        P = ox.projection.project_graph(G, to_crs=source_crs)
        
        # convert osmnx graph to geodataframes
        gdf_bridge_nodes, gdf_bridge_edges = ox.graph_to_gdfs(P)
    
        gdf_out = gdf_bridge_edges
        
        # flatten the index
        gdf_out.reset_index(inplace=True)
        
        # returns a data series of all the fields in geodataframe that contain lists
        ds_mask = gdf_out.apply(fn_has_list)
        
        list_str_keys_with_lists = []
        
        # get a list of the keys in ds_mask that are True
        for label, content in ds_mask.items():
            if content == True:
                list_str_keys_with_lists.append(label)
        
        # recast the fields with lists as strings
        for str_field_name in list_str_keys_with_lists:
            gdf_out[str_field_name] = gdf_out[str_field_name].astype(str)
            
        # remove duplicate osmid
        # the same bridge in opposite directions is frequently returned
        gdf_out = gdf_out.drop_duplicates(subset=['osmid'])
        
        # TODO - Clip the bridge vector data to the input polygon - 2023.01.20
        gdf_current_wgs = gpd.GeoDataFrame(index=[0],
                                          crs=wgs,
                                          geometry=[geom_current_wgs])
        
        gdf_current_local_crs = gdf_current_wgs.to_crs(source_crs)
        
        
        gdf_out = gpd.overlay(gdf_out,gdf_current_local_crs, how='intersection',
                              keep_geom_type=True)
        
        if len(gdf_out) > 0:
            gdf_out.to_file(str_output)
        else:
            # no bridges found
            pass
    except:
        pass
        # nothing found in the requested polygon
        # skip it
        
    return(str_output)
# -------------------------------------------------------  


# --------------------------------------------------------
def fn_get_osm_bridge_lines_from_shp(str_input_path,str_output_dir,str_field_name ):
    
    """
    Fetch the OSM road and rail linework from first polygon in the user supplied polygon

    Args:
        str_input_path: path to the requested polygon shapefile
        str_output_dir: path to write the output shapefile
        str_field_name: input shapefile field name with unique vaules for output shapefile(s) naming

    Returns:
        geodataframe of transporation bridge lines (edges)
    """
    
    print(" ")
    print("+=================================================================+")
    print("|            OPENSTREETMAP BRIDGE LINES FROM POLYGON              |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) INPUT SHAPEFILE PATH: " + str_input_path)
    print("  ---(o) OUTPUT DIRECTORY: " + str_output_dir)
    print("  ---[f]   Optional: Naming Field: " + str(str_field_name)) 
    print("===================================================================")
    
    # create the output directory if it does not exist
    os.makedirs(str_output_dir, exist_ok=True)

    # option to turn off the SettingWithCopyWarning
    pd.set_option('mode.chained_assignment', None)

    wgs = "epsg:4326"

    # read the "area of interest" shapefile in to geopandas dataframe
    gdf_aoi_prj = gpd.read_file(str_input_path)
    
    # convert aoi to wgs
    gdf_aoi_wgs = gdf_aoi_prj.to_crs(wgs)
    
    # get the crs of the user supplied shapefile
    source_crs = gdf_aoi_prj.crs
    
    # determine if the naming field can be used in for the output shapefiles
    b_have_valid_label_field = False
    
    # determine if the requested naming field is in the input shapefile
    if str_field_name in gdf_aoi_wgs.columns:
        
        if len(gdf_aoi_wgs) < 2:
            # no need to convert to series if there is just one polygon
            b_have_valid_label_field = True
            
        else:
            # create a dataframe of just the requested field
            gdf_just_req_field = pd.DataFrame(gdf_aoi_wgs, columns = [str_field_name])
    
            # convert the dataframe to a series
            df_just_req_field_series = gdf_just_req_field.squeeze()
    
            # determine if the naming field is unique
            if df_just_req_field_series.is_unique:
                b_have_valid_label_field = True
            else:
                print('No unique values found.  Naming will be sequential.')
    

    # get geoDataFrame of the boundary of the input shapefile polygons
    gdf_bounds = gdf_aoi_wgs.bounds
    
    list_bboxes_all_polys = []
    list_output_filepath = []
    list_crs = []
    list_geom_wgs = []
    
    int_count = 0
    
    for index, row in gdf_aoi_wgs.iterrows():
        # convert the pandas first row to list of bounding points
        list_bbox = gdf_bounds.loc[index, :].values.tolist()
        
        # get the geometry of the requested polygon
        list_geom_wgs.append(row['geometry'])
        
        # convert list to tuple
        tup_bbox = tuple(list_bbox)
        
        # shapely geom of bbox from tuple
        bbox_polygon = shapely.geometry.box(*tup_bbox, ccw=True)
        
        list_bboxes_all_polys.append(bbox_polygon)
        
        if b_have_valid_label_field:
            str_current_name = row[str_field_name]
        else:
            str_current_name = str(int_count)
            
        str_folder_output = os.path.join(str_output_dir, str_current_name)
        
        if not os.path.exists(str_folder_output):
            os.mkdir(str_folder_output)
        
        str_output = os.path.join(str_folder_output, 'osm_bridge_ln_' + str_current_name + '.shp')
        
        list_output_filepath.append(str_output)
        list_crs.append(source_crs)
    
        int_count += 1
    
    # create lsits of lists for MultiThreading
    list_of_lists = list(zip(list_bboxes_all_polys, list_output_filepath, list_crs, list_geom_wgs))
    
    print('NOTE: Large areas may take several minutes')
    print('Getting OSM Bridge lines...')
    int_count = 0
    
    # **************************
    # Multi-threaded download
    # Multiple requested to OSM per polygon
    # on first pass ... each county could take up to 5 minutes
    
    # Initial call to print 0% progress
    l = len(list_bboxes_all_polys)
    print(' ')

    str_prefix = "Polygon " + str(1) + " of " + str(len(list_bboxes_all_polys))
    fn_print_progress_bar(0, l, prefix = str_prefix , suffix = 'Complete', length = 36)
    
    i = 0
    results = ThreadPool(10).imap_unordered(fn_fetch_osm_data,
                                            list_of_lists)

    for str_requested_tile in results:
        time.sleep(0.1)
        i += 1
        str_prefix = "Polygon " + str(i) + " of " + str(len(list_bboxes_all_polys))
        fn_print_progress_bar(i, l, prefix = str_prefix, suffix = 'Complete', length = 36)
    # **************************
    
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='========= OPENSTREETMAP BRIDGE LINES FROM POLYGON =========')
    
    parser.add_argument('-i',
                        dest = "str_input_path",
                        help=r'REQUIRED: path to the input shapefile (polygon) Example: C:\test\cloud_harvest\00_aoi_shapefile\huc_12_aoi_2277.shp',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))

    parser.add_argument('-o',
                        dest = "str_output_dir",
                        help=r'REQUIRED: directory to write line shapefile: Example: C:\test\cloud_harvest\03_osm_trans_lines',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-f',
                        dest = "str_field_name",
                        help='OPTIONAL: unique field from input shapefile',
                        required=False,
                        default='NONE',
                        metavar='STRING',
                        type=str)
    
    args = vars(parser.parse_args())
    
    str_input_path = args['str_input_path']
    str_output_dir = args['str_output_dir']
    str_field_name = args['str_field_name']
    
    fn_get_osm_bridge_lines_from_shp(str_input_path,
                                     str_output_dir,
                                     str_field_name)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~