# Given a rtree created from the 05_create_rtree_from_dem_dir script,
# this script creates (1) a geopackage of the tile layout in the 'most common'
# crs of the input DEMS, (2) a geopackage of the tile index in EPSG:3857 and
# (3) a geopackage of the tile index with links to the orginal DEM locations
# in EPSG 3857.  This was created to clean up 05 issues.  In the future, the
# ammended 05_create_rtree_from_dem_dir should handle this processing without
# this script.
# -- for TACC tx-bridge input staging (step 5)
#
# Created by: Andy Carter, PE
# Created - 2023.08.18
# Last revised - 2023.08.28
# # Uses the 'tx-bridge' conda environment

# ************************************************************
import argparse
import os

import rtree

from shapely.geometry import box
from shapely.geometry import Polygon

import geopandas as gpd
import pandas as pd

import time
import datetime
# ************************************************************

# --------------------------------------------------------
def fn_reproject_rtree_file(str_input_rtree_filepath,
                            str_out_directory):


    print(" ")
    print("+=================================================================+")
    print("|              REPROJECT RTREE INDEX TO EPSG:3857                 |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) INPUT RTREE FILEPATH: " + str(str_input_rtree_filepath))
    print("  ---(o) OUTPUT DIRECTORY: " + str(str_out_directory))

    print("===================================================================")
    print(" ")

    if not os.path.exists(str_out_directory):
        os.makedirs(str_out_directory)
        
    rtree_file = str_input_rtree_filepath
    
    # initialize variables for minimum and maximum coordinates
    minx, miny, maxx, maxy = float('inf'), float('inf'), float('-inf'), float('-inf')
    
    # create a shapely polygon from the bounding box coordinates
    shp_polygon = box(minx, miny, maxx, maxy)
    
    # Initialize lists to store information
    list_intersecting_dem_paths = []
    list_bbox_tup = []
    list_str_crs = []
    
    # Open the R-tree index
    itree = rtree.index.Index(rtree_file)
    
    # Find intersecting objects (DEM files) in the R-tree index based on the bounding box
    hits = itree.intersection(shp_polygon.bounds, objects='raw')
    
    for int_count, tup_bounds, filepath, str_crs in hits:
        list_intersecting_dem_paths.append(filepath)
        list_bbox_tup.append(tup_bounds)
        list_str_crs.append(str_crs)
    itree.close()
    
    # Create a Pandas DataFrame from the lists
    data = {'dem_paths': list_intersecting_dem_paths,
            'bbox': list_bbox_tup,
            'crs': list_str_crs}
    
    df = pd.DataFrame(data)
    
    # Split the DataFrame into multiple DataFrames based on unique values in "crs"
    dfs_dict = {crs: group for crs, group in df.groupby('crs')}
    
    # Convert the dictionary values (DataFrames) to a list
    list_of_dfs = list(dfs_dict.values())
    
    list_gdfs_original_crs = []
    
    for df_by_crs in list_of_dfs:
        # create a list from the list_bbox_tup
        list_bbox_tup = df_by_crs['bbox'].tolist()
    
        # Create a list of Polygon geometries from the bounding box tuples
        list_polygons_per_df = [Polygon([(minx, miny),
                                  (maxx, miny),
                                  (maxx, maxy),
                                  (minx, maxy)]) for minx, miny, maxx, maxy in list_bbox_tup]
    
    
        list_dem_paths_per_df = df_by_crs['dem_paths'].tolist()
        list_str_crs_per_df = df_by_crs['crs'].tolist()
    
        # Create a GeoDataFrame from the list of polygons
        gdf_single_crs = gpd.GeoDataFrame({'geometry': list_polygons_per_df,
                                           'dem_paths': list_dem_paths_per_df,
                                           'crs_strings': list_str_crs_per_df},
                                          crs=list_str_crs_per_df[0])
    
        list_gdfs_original_crs.append(gdf_single_crs)
        
    int_crs_count = 0 
    
    print('Creating ' + str(len(list_gdfs_original_crs)) + " geopackage in original crs...")
    
    for gdf_item in list_gdfs_original_crs:
        # create a geodataframe for each crs in collection
        str_gpkg_name = 'dem_tiles_ar_source_' + str(int_crs_count) + ".gpkg"
        
        str_gpkg_filepath = os.path.join(str_out_directory,str_gpkg_name)
        gdf_item.to_file(str_gpkg_filepath, driver="GPKG")
        
        int_crs_count += 1
    
    # ------ reproject all the gdfs per crs
    print('Saving tile geopackage in EPSG:3857...')
    str_new_crs = 'EPSG:3857'
        
    list_gdfs_in_3857 = []
    # reproject all of the gdfs to a single crs
    # and merge the gdfs into a single gdf
    for gdf_item in list_gdfs_original_crs:
        
        # Reproject the GeoDataFrame to the new CRS
        gdf_item_3857 = gdf_item.to_crs(str_new_crs)
        list_gdfs_in_3857.append(gdf_item_3857)
        
    # Merge the list of GeoDataFrames into a single GeoDataFrame
    gdf_merge_3857 = pd.concat(list_gdfs_in_3857, ignore_index=True)
    
    str_gpkg_name = 'dem_tiles_ar_3857.gpkg'
    str_gpkg_filepath = os.path.join(str_out_directory,str_gpkg_name)
    
    gdf_merge_3857.to_file(str_gpkg_filepath, driver="GPKG")
    
    # ---- create rtree in the new crs
    print('Making Rtree in EPSG:3857...')
    # compute a bounding box
    gdf_merge_3857['bbox'] = gdf_merge_3857['geometry'].bounds.apply(lambda row: (row['minx'],
                                                                                  row['miny'],
                                                                                  row['maxx'],
                                                                                  row['maxy']), axis=1)
    #create an rtree of the reprojection
    rtree_file_3857 = os.path.join(str_out_directory,'rtree_file_3857')
    itree = rtree.index.Index(rtree_file_3857)
    int_count = 0
    
    for index, row in gdf_merge_3857.iterrows():
        tup_bbox = tuple(row['bbox'])
    
        # add the polygon to the rtree index with pathname to dems
        itree.insert(int_count, tup_bbox, obj = (int_count,
                                                 tup_bbox,
                                                 row['dem_paths'],
                                                 row['crs_strings']))
    
    int_count += 1
    itree.close()
# --------------------------------------------------------


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()

    parser = argparse.ArgumentParser(description='=========== REPROJECT DEM RTREE INDEX TO EPSG:3857 ============')

    # NOTE --- dont add the '.idx' or '.dat' to the input file path of rtree
    parser.add_argument('-i',
                        dest = "str_input_rtree_filepath",
                        help=r'REQUIRED: Filepath to RTREE: Example: D:\rtree\008\rtree_file',
                        required=True,
                        metavar='DIR',
                        type=str)

    parser.add_argument('-o',
                        dest = "str_out_directory",
                        help=r'REQUIRED: path to write all the outputs: Example D:\folder_containing_dems\06_rtreeindex',
                        required=True,
                        metavar='DIR',
                        type=str)
    

    args = vars(parser.parse_args())

    str_input_rtree_filepath = args['str_input_rtree_filepath']
    str_out_directory = args['str_out_directory']

    fn_reproject_rtree_file(str_input_rtree_filepath,
                                     str_out_directory)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)

    print('Compute Time: ' + str(time_pass))
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~