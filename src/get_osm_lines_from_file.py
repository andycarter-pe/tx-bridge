# Given an input polygon shapefile, get a shapefile of the transportation
# alignments (lines) from OpenStreetMaps (local shapefiles).
#
# Created by: Andy Carter, PE
# Created - 2023.02.28
#
# tx-bridge - third processing script
# Uses the 'pdal' conda environment

# ************************************************************
import argparse
import geopandas as gpd
import pandas as pd
import shapely.geometry

import osmnx as ox
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


# --------------------------------------------------------
def fn_get_osm_lines_from_file(str_input_path,
                              str_osm_line_shpaefile_path,
                              str_output_dir):
    
    
    # distance in crs to buffer aoi polygon
    flt_buffer = 2000
    
    
    """
    Fetch the OSM road and rail linework from first polygon in the user supplied polygon

    Args:
        str_input_path: path to the requested polygon shapefile
        str_output_dir: path to write the output shapefile

    Returns:
        geodataframe of transportation lines (edges)
    """
    
    print(" ")
    print("+=================================================================+")
    print("|         OPENSTREETMAP TRANSPORTATION LINES FROM POLYGON         |")
    print("|            USING USER PROVIDED OSM OVERALL SHAPEFILE            |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) INPUT AREA OF INTEREST SHAPEFILE PATH: " + str_input_path)
    print("  ---(s) INPUT OSM SHAPEFILE PATH: " + str_osm_line_shpaefile_path)
    print("  ---(o) OUTPUT DIRECTORY: " + str_output_dir)
    print("===================================================================")
    
    # create the output directory if it does not exist
    os.makedirs(str_output_dir, exist_ok=True)

    # option to turn off the SettingWithCopyWarning
    pd.set_option('mode.chained_assignment', None)

    wgs = "epsg:4326"
    
    # read the "area of interest" shapefile in to geopandas dataframe
    gdf_aoi_prj = gpd.read_file(str_input_path)
    
    # buffer the gdf_aoi_prj
    gdf_aoi_prj = gdf_aoi_prj.buffer(flt_buffer)
    
    # convert aoi to wgs
    gdf_aoi_wgs = gdf_aoi_prj.to_crs(wgs)
    
    # get the crs of the user supplied shapefile
    source_crs = gdf_aoi_prj.crs
    
    # get geoDataFrame of the boundary of the input shapefile
    gdf_bounds = gdf_aoi_wgs.bounds
    
    # convert the pandas first row to list of bounding points
    list_bbox = gdf_bounds.loc[0, :].values.tolist()
    
    # convert list to tuple
    tup_bbox = tuple(list_bbox)
    
    print("Clipping OpenStreetMap Transportation line file... ~ 1 minute")
    
    # read bounding line data within the bounding box
    gdf_bb = gpd.read_file(str_osm_line_shpaefile_path,bbox=tup_bbox)
    
    print("Reprojecting OSM line data back to source crs ...")
    
    # convert road data to source crs
    gdf_bb_prj = gdf_bb.to_crs(source_crs)
    
    print('Writing clipped transportation file ... ~ 1 minute')
    
    str_file_shp_to_write = os.path.join(str_output_dir, 'osm_trans_ln.shp')
    gdf_bb_prj.to_file(str_file_shp_to_write)
# --------------------------------------------------------


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='========= OPENSTREETMAP TRANSPORTATION LINES FROM POLYGON =========')
    
    parser.add_argument('-i',
                        dest = "str_input_path",
                        help=r'REQUIRED: path to the input shapefile (polygon) Example: D:\globus_transfer\merge_output\aoi_south_central_ar_3857.shp',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))
    
    parser.add_argument('-s',
                        dest = "str_osm_line_shpaefile_path",
                        help=r'REQUIRED: path to the OSM transportation shapefile (line) Example: D:\osm_download\texas_osm_transport_dissolve_ln_4326.shp',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))

    parser.add_argument('-o',
                        dest = "str_output_dir",
                        help=r'REQUIRED: directory to write line shapefile: Example: D:\osm_download\clipped_output',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    args = vars(parser.parse_args())
    
    str_input_path = args['str_input_path']
    str_osm_line_shpaefile_path = args['str_osm_line_shpaefile_path']
    str_output_dir = args['str_output_dir']

    fn_get_osm_lines_from_file(str_input_path,
                               str_osm_line_shpaefile_path,
                               str_output_dir)

    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
