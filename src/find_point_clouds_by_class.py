# Given a user defined polygon shapefile, this script breaks the polygon into
# tiles and seaches for point clouds by user supplied classification. For
# example - classification 17 is for points classified as bridge.
#
# Created by: Andy Carter, PE
# Created - 2022.04.26
# Last revised - 2022.04.26
#
# tx-bridge - first processing script
# Uses the 'pdal' conda environment

# ************************************************************
import os

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon
import pdal
import json

import argparse

import time
import datetime
import warnings

from multiprocessing.pool import ThreadPool

import multiprocessing as mp
import tqdm
from time import sleep

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


# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
def fn_create_tiles_gdf (str_aoi_shp_path,
                         int_buffer,
                         int_tile_x,
                         int_tile_y,
                         int_overlap):
    
    
    # define the "lambert" espg
    str_lambert = "epsg:3857"
    
    # read the "area of interest" shapefile in to geopandas dataframe
    gdf_aoi_prj = gpd.read_file(str_aoi_shp_path)
    
    # convert the input shapefile to lambert
    gdf_aoi_lambert = gdf_aoi_prj.to_crs(str_lambert)
    
    #buffer the polygons in the input shapefile
    gdf_aoi_lambert['geometry'] = gdf_aoi_lambert.geometry.buffer(int_buffer)
    
    
    for index_gdf_int, row_gdf_int in gdf_aoi_lambert.iterrows():
    
        # the geometry from the requested polygon as wellKnownText
        boundary_geom_WKT = gdf_aoi_lambert['geometry'][index_gdf_int]  # to WellKnownText
    
        # create geodataframe of just the current row
        gdf_aoi_lambert_current = gpd.GeoDataFrame(gdf_aoi_lambert.iloc[[index_gdf_int]])
    
        # reset the index
        gdf_aoi_lambert_current = gdf_aoi_lambert_current.reset_index(drop=True)
        
        # get the geodataframe coordiante system
        gdf_aoi_lambert_current = gdf_aoi_lambert_current.set_crs(str_lambert)
    
        # the bounding box of the requested lambert polygon
        b = boundary_geom_WKT.bounds
    
        # convert the bounding coordinates to integers
        list_int_b = []
        for i in b:
            list_int_b.append(int(i//1))
    
        # determine the width and height of the requested polygon
        flt_delta_x = list_int_b[2] - list_int_b[0]
        flt_delta_y = list_int_b[3] - list_int_b[1]
    
        # determine the number of tiles in the x and y direction
        int_tiles_in_x = (flt_delta_x // (int_tile_x - int_overlap)) + 1
        int_tiles_in_y = (flt_delta_y // (int_tile_y - int_overlap)) + 1
    
        list_tile_name = []
        list_geometry = []
    
        list_point_x = []
        list_point_y = []
    
        for value_x in range(int_tiles_in_x):
            list_point_x = []
            int_current_start_x = (value_x * (int_tile_x - int_overlap)) + list_int_b[0]
            list_point_x = [int_current_start_x, 
                            int_current_start_x + int_tile_x, 
                            int_current_start_x + int_tile_x, 
                            int_current_start_x,
                            int_current_start_x]
    
            for value_y in range(int_tiles_in_y):
                list_point_y = []
                int_current_start_y = (value_y * (int_tile_y - int_overlap)) + list_int_b[1] 
                list_point_y = [int_current_start_y, 
                                int_current_start_y,
                                int_current_start_y + int_tile_y,
                                int_current_start_y + int_tile_y,
                                int_current_start_y]
    
                polygon_geom = Polygon(zip(list_point_x, list_point_y))
                list_geometry.append(polygon_geom)
    
                str_time_name = str(value_x) + '_' + str(value_y)
                list_tile_name.append(str_time_name)
    
    # create a pandas dataframe
    df = pd.DataFrame({'tile_name': list_tile_name, 'geometry': list_geometry})
    
    # convert the pandas dataframe to a geopandas dataframe
    gdf_tiles = gpd.GeoDataFrame(df, geometry='geometry')
    
    # set the tile footprint crs
    gdf_tiles = gdf_tiles.set_crs(str_lambert)
    
    # intersect the tiles and the requested polygon
    gdf_intersected_tiles = gpd.overlay(gdf_tiles, gdf_aoi_lambert_current, how='intersection')
    
    # get a unique list of the intersected tiles
    arr_tiles_intersect = gdf_intersected_tiles['tile_name'].unique()
    
    # convert the array to a list
    list_tiles_intersect = arr_tiles_intersect.tolist()
    
    # new geodataframe of the tiles intersected (but not clipped)
    gdf_tiles_intersect_only = gdf_tiles[gdf_tiles['tile_name'].isin(list_tiles_intersect)]
    
    # reset the remaining tile index
    gdf_tiles_intersect_only = gdf_tiles_intersect_only.reset_index(drop=True)
    
    return gdf_tiles_intersect_only
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


# -------------------------------------------------------------------
def fn_determine_ept_source_per_tile(gdf_tiles):
    
    str_hobu_footprints = r'https://raw.githubusercontent.com/hobu/usgs-lidar/master/boundaries/boundaries.topojson'

    # Get EPT limits from github repository
    gdf_entwine_footprints = gpd.read_file(str_hobu_footprints)
    
    str_lambert = "epsg:3857"
    str_wgs = "epsg:4326"
    
    # Set the entwine footprint CRS
    gdf_entwine_footprints = gdf_entwine_footprints.set_crs(str_wgs)

    # Convert the footprints to lambert
    gdf_entwine_footprints = gdf_entwine_footprints.to_crs(str_lambert)
    
    list_ept_tiles = []
    
    # get a point cloud for the 'selected' polygon
    for index, row in gdf_tiles.iterrows():
        gdf_current_poly = gdf_tiles.iloc[index:index + 1]
    
        # clip the footprints to the requested boundary
        gdf_entwine_footprints_clip = gpd.overlay(gdf_entwine_footprints, gdf_current_poly, how='intersection')
    
        # Set the EPT url to the first point cloud found
        # TODO - what if there are no ept sources - 2022.04.26
        # TODO - what is there are more than one ept - find best - 2022.04.26
        ept_source = gdf_entwine_footprints_clip.loc[0, 'url']
        
        list_ept_tiles.append(ept_source)
    
    # add the list to geodataframe
    gdf_tiles['ept_source'] = list_ept_tiles
    
    return(gdf_tiles)
# -------------------------------------------------------------------


# ===================================================================
def fn_get_las_tiles(gdf_current_tile):
    
    # 'tile_name'
    str_tile_name = gdf_current_tile.iloc[0]['tile_name']
    ept_source = gdf_current_tile.iloc[0]['ept_source']
    INT_CLASS = gdf_current_tile.iloc[0]['class']
    
    STR_OUTPUT_PATH = r'C:\test\cloud_harvest\cloud_output\\'
    
    b = gdf_current_tile.iloc[0]['geometry'].bounds #the bounding box of the requested lambert polygon

    str_classification = "Classification[" + str(INT_CLASS) + ":" + str(INT_CLASS) + "]"
    
    # TODO - This is hardcoded - fix this!!! - 2022.04.26
    # ################
    str_las = STR_OUTPUT_PATH + str_tile_name + '_class_' + str(INT_CLASS) + '.las'
    # ################

    
    #if n_points > 0:
        # get and save the point cloud with the requested classificaton

    pipeline_class_las = {
    "pipeline": [
        {   
            'bounds':str(([b[0], b[2]],[b[1], b[3]])),
            "filename":ept_source,
            "type":"readers.ept",
            "tag":"readdata"
        },
        {   
            "type":"filters.range",
            "limits": str_classification,
            "tag":"class_points"
        },
        {
            "filename": str_las,
            "inputs": [ "class_points" ],
            "type": "writers.las"
        }
    ]}
    #execute the pdal pipeline
    pipeline = pdal.Pipeline(json.dumps(pipeline_class_las))
    n_points = pipeline.execute()
    
    sleep(0.01) # this allows the tqdm progress bar to update
    
    if n_points > 0:
        return(str_las)
    else:
        pass
        # need to delete this file
        
# ===================================================================



def fn_point_clouds_by_class(str_input_path,
                             str_output_dir,
                             int_class,
                             int_buffer,
                             int_tile,
                             int_overlap):
    
    # supress all warnings
    warnings.filterwarnings("ignore", category=UserWarning )
    
    print(" ")
    print("+=================================================================+")
    print("|          POINT CLOUDS BY CLASSIFICATION FROM SHAPEFILE          |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    
    print("  ---(i) INPUT PATH: " + str_input_path)
    print("  ---(o) OUTPUT PATH: " + str_output_dir)
    print("  ---[c]   Optional: CLASSIFICATION: " + str(int_class) )
    print("  ---[b]   Optional: BUFFER: " + str(int_buffer) + " meters") 
    print("  ---[t]   Optional: TILE SIZE: " + str(int_tile) + " meters") 
    print("  ---[m]   Optional: TILE OVERLAP: " + str(int_overlap) + " meters") 
    print("===================================================================")


    gdf_tiles = fn_create_tiles_gdf(str_input_path,
                                    int_buffer,
                                    int_tile,
                                    int_tile,
                                    int_overlap)

    print('Determining Entwine paths: ' + str(len(gdf_tiles)) + ' tiles')
    
    
    # append tiles with the entwine path
    gdf_tiles_ept = fn_determine_ept_source_per_tile(gdf_tiles)
    
    # append the dataframe with the classification
    gdf_tiles_ept['class'] = int_class
    
    # creating a list of geodataframes (just one row each) for multithreading the pdal requests
    list_of_gdf_tiles = []

    for index, row in gdf_tiles.iterrows():
        gdf_single_row = gdf_tiles.loc[[index]]
        list_of_gdf_tiles.append(gdf_single_row)
    
    
    print("+-----------------------------------------------------------------+")
    l = len(gdf_tiles)
    p = mp.Pool(processes = (mp.cpu_count() - 1))
        
    list_return_values = list(tqdm.tqdm(p.imap(fn_get_las_tiles, list_of_gdf_tiles),
                                        total = l,
                                        desc='Get LAS Points',
                                        bar_format = "{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
                                        ncols=65))
    p.close()
    p.join()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='========== POINT CLOUDS BY CLASSIFICATION FROM SHAPEFILE ==========')
    
    parser.add_argument('-i',
                        dest = "str_input_path",
                        help=r'REQUIRED: path to the input shapefile (polygons) Example: C:\test\cloud_harvest\huc_12_aoi_2277.shp',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))

    parser.add_argument('-o',
                        dest = "str_output_dir",
                        help=r'REQUIRED: directory to write DEM files Example: C:\test\cloud_harvest\cloud_output',
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

    parser.add_argument('-b',
                        dest = "int_buffer",
                        help='OPTIONAL: buffer for each polygon (meters): Default=300',
                        required=False,
                        default=300,
                        metavar='INTEGER',
                        type=int)
    
    parser.add_argument('-t',
                        dest = "int_tile",
                        help='OPTIONAL: requested tile dimensions (meters): Default=2000',
                        required=False,
                        default=2000,
                        metavar='INTEGER',
                        type=int)
    
    parser.add_argument('-m',
                        dest = "int_overlap",
                        help='OPTIONAL: requested tile overlap distance (meters): Default=50',
                        required=False,
                        default=50,
                        metavar='INTEGER',
                        type=int)
    
    args = vars(parser.parse_args())
    
    str_input_path = args['str_input_path']
    str_output_dir = args['str_output_dir']
    int_class = args['int_class']
    int_buffer = args['int_buffer']
    int_tile = args['int_tile']
    int_overlap = args['int_overlap']

    fn_point_clouds_by_class(str_input_path,
                             str_output_dir,
                             int_class,
                             int_buffer,
                             int_tile,
                             int_overlap)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~