# This is the main orchestration script for "tx-bridge".  It attempts to convert
# get polygon footprints of bridges from USGS classified entwine point clouds
# within the limits of the requested input polygons.  For each polygon an alignment
# down the bridge is determined from roadway linework.  Along this alignment, a
# profile of both the bridge deck and the "bare earth" below the bridge is determined.
# From these data, a 'bridge envelope' can be determined.
#
# This script needs other scripts to complete the process
# (1) find_point_clouds_by_class.py
# (2) polygonize_point_groups.py
# (3) get_osm_lines_from_shape.py
# (4) determine_major_axis.py
# (5) create_hull_dem.py
# (6) flip_major_axis.py
# (7) assign_osm_names_major_axis.py
# (8) extract_deck_profile.py
#
# TODO - Not yet created - 2022.06.16
# (9) build_object_model_output.py
# (10) conmposite_terrain.py
#
# Created by: Andy Carter, PE
# Last revised - 2022.06.17
#
# Main code for tx-bridge
# Uses the 'tx-bridge' conda environment
#
from find_point_clouds_by_class import fn_point_clouds_by_class
from polygonize_point_groups import fn_polygonize_point_groups
from get_osm_lines_from_shp import fn_get_osm_lines_from_shp
from determine_major_axis import fn_determine_major_axis
from create_hull_dem import fn_create_hull_dems
from flip_major_axis import fn_flip_major_axis
from assign_osm_names_major_axis import fn_assign_osm_names_major_axis
from extract_deck_profile import fn_extract_deck_profile
from get_usgs_dem_from_shape import fn_get_usgs_dem_from_shape
# fn_get_usgs_dem_from_shape(str_input_path,str_output_dir,int_res,int_buffer,int_tile,b_is_feet,str_field_name)

import argparse
import os

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


# --------------------------------------------------------
def fn_run_tx_bridge(str_input_shp_path_arg,
                     str_out_arg,
                     int_class,
                     b_is_feet):
    
    # mannualy setting the step to start computations
    int_step = 9
    
    flt_start_run_tx_bridge = time.time()
    
    print(" ")
    print("+=================================================================+")
    print("|       CREATE BRIDGE DECK DATA FROM ENTWINE POINT CLOUDS         |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")
    
    print("  ---(i) INPUT SHAPEFILE: " + str(str_input_shp_path_arg))
    print("  ---(o) OUTPUT DIRECTORY: " + str(str_out_arg))   
    print("  ---[c]   Optional: Point Classification: " + str(int_class))
    print("  ---[v]   Optional: Vertical in feet: " + str(b_is_feet))

    print("===================================================================")
    print(" ")
    
    # ---- Step 1: find and download point clouds by classification ----
    int_buffer = 300 # distance to buffer the input polygon (meters)
    int_tile = 2000 # height and width of entwine tile (meters)
    int_overlap = 50 # requested point cloud tile overlay (meters)
    
    # create a folder for las point clouds
    str_las_from_entwine_dir = os.path.join(str_out_arg, "01_las_from_entwine") 
    if not os.path.exists(str_las_from_entwine_dir):
        os.mkdir(str_las_from_entwine_dir)
    
    # run the first script (find_point_clouds_by_class)
    if int_step <= 1:
        fn_point_clouds_by_class(str_input_shp_path_arg,
                                 str_las_from_entwine_dir,
                                 int_class,
                                 int_buffer,
                                 int_tile,
                                 int_overlap)
    # ------------------------------------------------------------------ 
    
    # ---- Step 2: create polygons of point cloud groupings ----
    flt_epsilon = 250 # DBSCAN - distance from point to be in neighboorhood in centimeters
    int_min_samples = 4 # DBSCAN - points within epsilon radius to anoint a core point
    
    # create a folder hull polygons
    str_hull_shp_dir = os.path.join(str_out_arg, "02_shapefile_of_hulls") 
    if not os.path.exists(str_hull_shp_dir):
        os.mkdir(str_hull_shp_dir)
        
    if int_step <= 2:
        fn_polygonize_point_groups(str_las_from_entwine_dir,
                                   str_hull_shp_dir,
                                   int_class,
                                   flt_epsilon,
                                   int_min_samples)
    # ------------------------------------------------------------------
    
    # ---- Step 3: get OpenStreetMap Linework for roads, railraods, etc ----
    b_simplify_graph = True # simplify the network
    b_get_drive_service = True # get the road lines
    b_get_railroad = True # get the rialroad lines
    
    # TODO - add buffer distance as input paramter - 20220617
    
    # create a folder for OpenStreetMap linework
    str_osm_lines_shp_dir = os.path.join(str_out_arg, "03_osm_trans_lines") 
    if not os.path.exists(str_osm_lines_shp_dir):
        os.mkdir(str_osm_lines_shp_dir)
        
    if int_step <= 3:
        fn_get_osm_lines_from_shp(str_input_shp_path_arg,
                                  str_osm_lines_shp_dir,
                                  b_simplify_graph,
                                  b_get_drive_service,
                                  b_get_railroad)
    # ------------------------------------------------------------------
    
    # ---- Step 4: determine the major axis for each polygon ----
    flt_buffer_hull = 30 # distance to extend major axis beyond hull (meters)
    
    
    str_bridge_polygons_file = str(int_class) + '_ar_3857.shp'
    str_bridge_polygons_path = os.path.join(str_hull_shp_dir, str_bridge_polygons_file)
    
    #str_trans_line_path = str_osm_lines_shp_dir + '\\' + 'osm_trans_ln.shp'
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
    flt_dem_resolution = 0.3 # resolution of dem in meters
    
    # create a folder for major axis lines
    str_deck_dem_dir = os.path.join(str_out_arg, "05_bridge_deck_dems") 
    if not os.path.exists(str_deck_dem_dir):
        os.mkdir(str_deck_dem_dir)
        
    if int_step <= 5:
        fn_create_hull_dems(str_bridge_polygons_path,
                            str_deck_dem_dir,
                            flt_dem_resolution,
                            b_is_feet)
    # --------------------------------------------------
    
    # ---- Step 6: flip major axis (left to right downstream) ----
    flt_mjr_axis = 0.3 # distance to buffer major axis
    #str_major_axis_ln_path = str_mjr_axis_shp_dir + '//' + 'mjr_axis_ln.shp'
    str_major_axis_ln_path = os.path.join(str_mjr_axis_shp_dir, 'mjr_axis_ln.shp')
    
    # create a folder for major axis lines
    str_flip_axis_dir = os.path.join(str_out_arg, "06_flipped_major_axis") 
    if not os.path.exists(str_flip_axis_dir):
        os.mkdir(str_flip_axis_dir)
        
    if int_step <= 6:
        fn_flip_major_axis(str_major_axis_ln_path,
                           str_flip_axis_dir,
                           flt_mjr_axis)
    # --------------------------------------------------
    
    # ---- Step 7: assign names to major axis lines ----
    flt_perct_on_line = 0.35 # ratio distance to create a point on major axis    
    flt_offset  = 0.01 # distance to search around mjr axis' points for nearest osm line
    
    #str_mjr_axis_shp_path = str_flip_axis_dir + '\\' + 'flip_mjr_axis_ln.shp'
    str_mjr_axis_shp_path = os.path.join(str_flip_axis_dir, 'flip_mjr_axis_ln.shp')
    
    # create a folder for major axis with names lines
    str_mjr_axis_names_dir = os.path.join(str_out_arg, "07_major_axis_names") 
    if not os.path.exists(str_mjr_axis_names_dir):
        os.mkdir(str_mjr_axis_names_dir)
        
    if int_step <= 7:
        fn_assign_osm_names_major_axis(str_input_shp_path_arg,
                                       str_mjr_axis_shp_path,
                                       str_mjr_axis_names_dir,
                                       flt_perct_on_line,
                                       flt_offset)
    # --------------------------------------------------
    
    # ---- Step 8: extract deck profile (plot and tabular) ----
    flt_mjr_axis = 4 # distance to buffer major axis - lambert units - meters
    int_resolution = 1 # requested resolution in lambert units - meters
    flt_xs_sample_interval = 1 # interval to sample points along a line for cross section - crs units
    
    # create a folder for deck profile plots and tables
    str_deck_profiles_dir = os.path.join(str_out_arg, "08_deck_profiles") 
    if not os.path.exists(str_deck_profiles_dir):
        os.mkdir(str_deck_profiles_dir)
     
    '''
    if int_step <= 8:
        fn_extract_deck_profile(str_mjr_axis_shp_path,
                                str_deck_dem_dir,
                                str_deck_profiles_dir,
                                b_is_feet,
                                flt_mjr_axis,
                                int_resolution,
                                flt_xs_sample_interval)
    '''
    # --------------------------------------------------
    
    # ---- Step 9: get bare earth terrain for each aoi polygon ----
    int_res = 3 # resolution of the bare earth dem
    int_buffer = 300 # buffer for each polygon (meters)
    int_tile = 1500 # size of bare earth tile to request
    str_field_name = ''
    
    # create a folder for deck profile plots and tables
    str_bare_earth_dem_dir = os.path.join(str_out_arg, "09_bare_earth_dem") 
    if not os.path.exists(str_bare_earth_dem_dir):
        os.mkdir(str_bare_earth_dem_dir)
        
    # TODO - deletion error - 2022.06.17
    if int_step <= 9:
        fn_get_usgs_dem_from_shape(str_input_shp_path_arg,
                                   str_bare_earth_dem_dir,
                                   int_res,
                                   int_buffer,
                                   int_tile,
                                   b_is_feet,
                                   str_field_name)
    # --------------------------------------------------
    
    # ---- Step 10: merge the dems (bridge and bare earth) ----
    
    int_output_res = 3 # resolution of the aggregated dem (meters)
    # create a folder for healed dems (bridge deck and bare earth)
    str_healed_dem_dir = os.path.join(str_out_arg, "10_healed_dem") 
    if not os.path.exists(str_healed_dem_dir):
        os.mkdir(str_healed_dem_dir)
        
    # run the first script (find_point_clouds_by_class)
    '''
    if int_step <= 10:
        fn_heal_dems(str_input_shp_path_arg,
                     str_healed_dem_dir,
                     str_bridge_polygons_path,
                     str_deck_dem_dir
                     int_output_res)
    '''
    # --------------------------------------------------
    
    flt_end_run_run_tx_bridge = time.time()
    flt_time_pass_tx_bridge = (flt_end_run_run_tx_bridge - flt_start_run_tx_bridge) // 1
    time_pass_tx_bridge = datetime.timedelta(seconds=flt_time_pass_tx_bridge)
    
    print('Total Compute Time: ' + str(time_pass_tx_bridge))
# --------------------------------------------------------


# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ 

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='======= CREATE BRIDGE DECK DATA FROM ENTWINE POINT CLOUDS ========')
    
    # inputs
    # -- input shapefile of the area of iterest (polygon)
    # -- output directory (Example: C:\test\bridge_output\)
    
    parser.add_argument('-i',
                        dest = "str_input_shp_path_arg",
                        help=r'REQUIRED: path to the input shapefile (polygons) Example: C:\test\cloud_harvest\huc_12_aoi_2277.shp',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))

    parser.add_argument('-o',
                        dest = "str_out_arg",
                        help=r'REQUIRED: path to write all the outputs: Example C:\test\bridge_output_folder',
                        required=True,
                        metavar='DIR',
                        type=str)

    parser.add_argument('-c',
                        dest = "int_class",
                        help='OPTIONAL: desired point cloud classification: Default=17 (bridge)',
                        required=False,
                        default=17,
                        metavar='INTEGER',
                        type=int)
    
    parser.add_argument('-v',
                        dest = "b_is_feet",
                        help='OPTIONAL: create vertical data in feet: Default=True',
                        required=False,
                        default=True,
                        metavar='T/F',
                        type=str2bool)
    
    args = vars(parser.parse_args())
    
    str_input_shp_path_arg = args['str_input_shp_path_arg']
    str_out_arg = args['str_out_arg']
    int_class = args['int_class']
    b_is_feet = args['b_is_feet']
    
    fn_run_tx_bridge(str_input_shp_path_arg,
                     str_out_arg,
                     int_class,
                     b_is_feet)