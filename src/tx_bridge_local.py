# This is the main orchestration script for "tx-bridge".  It attempts to convert
# get polygon footprints of bridges from USGS classified entwine point clouds
# within the limits of the requested input polygons.  For each polygon an alignment
# down the bridge is determined from roadway linework.  Along this alignment, a
# profile of both the bridge deck and the "bare earth" below the bridge is determined.
# From these data, a 'bridge envelope' can be determined.
#
# Revised 2023-07-25 - MAC - utilizes locally provided files only
# removed all connections to internet and web services
#
# This script needs other scripts to complete the process
# (1) find_point_clouds_by_class.py
# (2) polygonize_point_groups.py
# (3) get_osm_lines_from_shape.py
# (4) determine_major_axis.py
# (5) create_hull_dem.py
# (6) flip_major_axis.py
# (7) assign_osm_names_major_axis.py
# (8) attribute_major_axis.py

# Created by: Andy Carter, PE
# Last revised - 2023.09.05
#
# Main code for tx-bridge
# Uses the 'tx-bridge' conda environment
#
from find_point_clouds_by_class_from_copc import fn_point_clouds_by_class_from_copc
from polygonize_point_groups import fn_polygonize_point_groups
from fix_convex_hulls import fn_fix_convex_hulls
from get_osm_lines_from_file import  fn_get_osm_lines_from_file
from determine_major_axis import fn_determine_major_axis
from create_hull_dem_from_copc import fn_create_hull_dem_from_copc
from flip_major_axis_local import fn_flip_major_axis
from assign_osm_names_major_axis_local import fn_assign_osm_names_major_axis
from attribute_major_axis_local_mp import fn_attribute_mjr_axis

import json
import configparser
import argparse
import os
import geopandas as gpd

import time
import datetime


# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
def is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist" % arg)
    else:
        # File exists so return the directory
        return arg
        return open(arg, 'r')  # return an open file handle
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


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


# --------------------------------------------------------
def fn_run_tx_bridge(str_input_json,
                     int_start_step,
                     str_global_ini_path):
    
    # mannualy setting the step to start computations
    int_step = int_start_step
    
    flt_start_run_tx_bridge = time.time()
    
    # create json of global configuration file
    dict_global_config_data = json.loads(fn_json_from_ini(str_global_ini_path))
    
    # Parse the run's JSON data
    with open(str_input_json) as f:
        json_run_data = json.load(f)
    
    str_input_shp_path_arg = json_run_data["str_aoi_shp_filepath"]
    str_aoi_name = json_run_data["str_aoi_name"]
    str_out_arg = json_run_data["str_output_folder"]
    b_is_feet = json_run_data["b_is_feet"]
    str_cog_dem_path = json_run_data["str_cog_dem_path"] 
    
    str_input_copc_file = json_run_data["copc_point_cloud"]["copc_filepath"]
    int_class = json_run_data["copc_point_cloud"]["copc_class"]
    str_copc_name = json_run_data["copc_point_cloud"]["copc_name"]
    str_copc_short_name = json_run_data["copc_point_cloud"]["copc_short_name"]
    int_copc_date = json_run_data["copc_point_cloud"]["copc_date"]

    print(" ")
    print("+=================================================================+")
    print("|         CREATE BRIDGE DECK DATA FROM COPC POINT CLOUDS          |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")
    
    print("  ---(i) INPUT JSON: " + str_input_json)
    print("  ---[s]   Optional: Starting step: " + str(int_start_step))
    print("  ---[g]   Optional: Global INI file: " + str_global_ini_path)
    print("  ")
    print("    --- Input shapefile: " + str(str_input_shp_path_arg))
    print("    --- Area of interest name: " + str(str_aoi_name))
    print("    --- Output folder: " + str(str_out_arg ))
    print("    --- Vertical in feet: " + str(b_is_feet))
    print("    --- COG DEM: " + str_cog_dem_path)
    print("+-----------------------------------------------------------------+")
    print("    --- COPC file path: " + str_input_copc_file)
    print("    --- COPC point classification: " + str(int_class))
    print("    --- COPC name: " + str_copc_name)
    print("    --- COPC short name: " + str_copc_short_name)
    print("    --- COPC date: " + str(int_copc_date))
    print("===================================================================")
    print(" ")
    
    if not os.path.exists(str_out_arg):
        os.mkdir(str_out_arg)
    
    # ---- Step 0: Save the input shapefile ----
    if int_step <= 0:
        str_input_shapefile_dir = os.path.join(str_out_arg, "00_input_shapefile")
        if not os.path.exists(str_input_shapefile_dir):
            os.mkdir(str_input_shapefile_dir)
            
        # find a shapefile in the str_path_to_aoi_folder and get list
        list_shapefiles = fn_filelist(str_input_shapefile_dir, ('.SHP', '.shp'))
        
        # if there is no shapefile in this directory, then save out the first polygon
        if len(list_shapefiles) <= 0:
            # load the polygon geodataframe
            gdf_polygons = gpd.read_file(str_input_shp_path_arg)
            
            # select the first polygon
            gdf_single_poly = gdf_polygons.iloc[[0]]
            
            # single polygon shapefile folder
            str_single_shape_file = os.path.join(str_input_shapefile_dir, 'input_polygon_ar.shp')
            gdf_single_poly.to_file(str_single_shape_file)
    # ------------------------------------------------------------------
    
    
    # ---- Step 1: find and extract point clouds by classification ----
    # distance to buffer the input polygon (meters)
    int_buffer = int(dict_global_config_data["01_points_by_class"]["int_buffer"])

    # height and width of entwine tile (meters)
    int_tile = int(dict_global_config_data ["01_points_by_class"]["int_tile"])
    
    # requested point cloud tile overlay (meters)
    int_overlap = int(dict_global_config_data ["01_points_by_class"]["int_overlap"])
    
    # create a folder for las point clouds
    str_las_from_copc_dir = os.path.join(str_out_arg, "01_las_from_copc") 
    if not os.path.exists(str_las_from_copc_dir):
        os.mkdir(str_las_from_copc_dir)
    
    # run the first script (find_point_clouds_by_class)
    if int_step <= 1:
        fn_point_clouds_by_class_from_copc(str_input_shp_path_arg,
                                           str_las_from_copc_dir,
                                           str_input_copc_file,
                                           int_class,
                                           int_buffer,
                                           int_tile,
                                           int_overlap)
        
    # ------------------------------------------------------------------ 
    
    # ---- Step 2: create polygons of point cloud groupings ----
    # DBSCAN - distance from point to be in neighboorhood in centimeters
    flt_epsilon = float(dict_global_config_data["02_polygonize_clusters"]["flt_epsilon"])
    
    # DBSCAN - points within epsilon radius to anoint a core point
    int_min_samples = int(dict_global_config_data["02_polygonize_clusters"]["int_min_samples"])
    
    # create a folder hull polygons
    str_hull_shp_dir = os.path.join(str_out_arg, "02_shapefile_of_hulls") 
    if not os.path.exists(str_hull_shp_dir):
        os.mkdir(str_hull_shp_dir)
        
    str_bridge_polygons_file = 'class_' + str(int_class) + '_ar_3857.gpkg'
    str_bridge_polygons_path = os.path.join(str_hull_shp_dir, str_bridge_polygons_file)
    
    if int_step <= 2:
        b_clouds_found = fn_polygonize_point_groups(str_las_from_copc_dir,
                                                    str_hull_shp_dir,
                                                    int_class,
                                                    flt_epsilon,
                                                    int_min_samples)
        
        if b_clouds_found:
            # revise the convex hulls that are too 'fat'
            fn_fix_convex_hulls(str_bridge_polygons_path,
                                str_input_json,
                                dict_global_config_data)
        
    # ------------------------------------------------------------------
    
    if int_step > 2:
        b_clouds_found = True
    
    if b_clouds_found:
        # classified point clouds found
        # do the other steps
        
        # ---- Step 3: get OpenStreetMap Linework for roads, railraods, etc ----
        str_osm_global_shp_path = dict_global_config_data["global_input_files"]["str_osm_gpkg"]
        
        # create a folder for OpenStreetMap linework
        str_osm_lines_shp_dir = os.path.join(str_out_arg, "03_osm_trans_lines") 
        if not os.path.exists(str_osm_lines_shp_dir):
            os.mkdir(str_osm_lines_shp_dir)
            
        if int_step <= 3:
            fn_get_osm_lines_from_file(str_input_shp_path_arg,
                                       str_osm_global_shp_path,
                                       str_osm_lines_shp_dir)
        # ------------------------------------------------------------------
        
        # ---- Step 4: determine the major axis for each polygon ----
        # distance to extend major axis beyond hull (project aoi units)
        flt_buffer_hull = float(dict_global_config_data["04_determine_mjr_axis"]["flt_buffer_hull"])
        
        
        str_bridge_polygons_file = 'class_' + str(int_class) + '_ar_3857.gpkg'
        str_bridge_polygons_path = os.path.join(str_hull_shp_dir, str_bridge_polygons_file)
        
        str_trans_line_path = os.path.join(str_osm_lines_shp_dir, 'osm_trans_ln.shp')
        
        # create a folder for major axis lines
        str_mjr_axis_shp_dir = os.path.join(str_out_arg, "04_major_axis_lines") 
        if not os.path.exists(str_mjr_axis_shp_dir):
            os.mkdir(str_mjr_axis_shp_dir)
            
        if int_step <= 4:
            fn_determine_major_axis(str_bridge_polygons_path,
                                    str_trans_line_path,
                                    str_mjr_axis_shp_dir,
                                    flt_buffer_hull)
        # ------------------------------------------------------------------
        
        # ---- Step 5: create DEM raster for each hull ----

        # create a folder for major axis lines
        str_deck_dem_dir = os.path.join(str_out_arg, "05_bridge_deck_dems") 
        if not os.path.exists(str_deck_dem_dir):
            os.mkdir(str_deck_dem_dir)
            
        if int_step <= 5:

            fn_create_hull_dem_from_copc(str_bridge_polygons_path,
                                         str_input_json,
                                         str_global_ini_path)
            '''
            # delete the extra dems
            fn_delete_files(str_deck_dem_dir)
            '''
            
        # --------------------------------------------------
        
        # ---- Step 6: flip major axis (left to right downstream) ----
        str_nhd_stream_gpkg = dict_global_config_data["global_input_files"]["str_nhd_stream_gpkg"]
        
        # distance to buffer major axis
        flt_mjr_axis = float(dict_global_config_data["06_flip_mjr_axis"]["flt_mjr_axis"])
        
        str_major_axis_ln_path = os.path.join(str_mjr_axis_shp_dir, 'mjr_axis_ln.shp')
        
        # create a folder for major axis lines
        str_flip_axis_dir = os.path.join(str_out_arg, "06_flipped_major_axis") 
        if not os.path.exists(str_flip_axis_dir):
            os.mkdir(str_flip_axis_dir)
            
        if int_step <= 6:
            fn_flip_major_axis(str_major_axis_ln_path,
                               str_flip_axis_dir,
                               flt_mjr_axis,
                               str_nhd_stream_gpkg)
        # --------------------------------------------------
        
        # ---- Step 7: assign names to major axis lines ----
        # ratio distance to create a point on major axis 
        flt_perct_on_line = float(dict_global_config_data["07_assign_names"]["flt_perct_on_line"])
        
        # distance to search around mjr axis' points for nearest osm line
        flt_offset = float(dict_global_config_data["07_assign_names"]["flt_offset"])
        
        str_mjr_axis_shp_path = os.path.join(str_flip_axis_dir, 'flip_mjr_axis_ln.shp')
        
        # create a folder for major axis with names lines
        str_mjr_axis_names_dir = os.path.join(str_out_arg, "07_major_axis_names") 
        if not os.path.exists(str_mjr_axis_names_dir):
            os.mkdir(str_mjr_axis_names_dir)
            
        if int_step <= 7:
            fn_assign_osm_names_major_axis(str_trans_line_path,
                                           str_mjr_axis_shp_path,
                                           str_mjr_axis_names_dir,
                                           flt_perct_on_line,
                                           flt_offset)
        # --------------------------------------------------
        
        # ---- Step 8: extract deck profile (plot and tabular) ----
        #flt_mjr_axis = 4 # distance to buffer major axis - lambert units - meters
        
        # create a folder for deck profile plots and tables
        str_deck_profiles_dir = os.path.join(str_out_arg, "08_cross_sections") 
        if not os.path.exists(str_deck_profiles_dir):
            os.mkdir(str_deck_profiles_dir)
        
        if int_step <= 8:
            fn_attribute_mjr_axis(str_out_arg,int_class,str_cog_dem_path,dict_global_config_data,str_input_json)

        # --------------------------------------------------
    
    else:
        print('--- NO POINT CLOUDS FOUND ---')
    
    flt_end_run_run_tx_bridge = time.time()
    flt_time_pass_tx_bridge = (flt_end_run_run_tx_bridge - flt_start_run_tx_bridge) // 1
    time_pass_tx_bridge = datetime.timedelta(seconds=flt_time_pass_tx_bridge)
    
    print('Total Compute Time: ' + str(time_pass_tx_bridge))
# --------------------------------------------------------


# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ 

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='======= CREATE BRIDGE DECK DATA FROM COPC POINT CLOUDS ========')
    
    
    parser.add_argument('-i',
                        dest = "str_input_json",
                        help=r'REQUIRED: path to the input coniguration json Example: D:\bridge_local_test\config.json',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))

    parser.add_argument('-s',
                        dest = "int_start_step",
                        help='OPTIONAL: starting computational step: Default=0',
                        required=False,
                        default=0,
                        metavar='INTEGER',
                        type=int)
    
    parser.add_argument('-g',
                        dest = "str_global_ini_path",
                        help=r'OPTIONAL: path to the input coniguration json Example: C:\Users\civil\dev\tx-bridge\src\config_global.ini',
                        required=False,
                        default=r'C:\Users\civil\dev\tx-bridge\src\config_global.ini',
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))
    
    args = vars(parser.parse_args())
    
    str_input_json = args['str_input_json']
    int_start_step = args['int_start_step']
    str_global_ini_path = args['str_global_ini_path']
    
    fn_run_tx_bridge(str_input_json,
                     int_start_step,
                     str_global_ini_path)