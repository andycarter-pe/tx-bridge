# Given a directory containing LAS files that have just the requested 
# classification, create polygons of the convex hull of each point
# grouping.
#
# Created by: Andy Carter, PE
# Created - 2022.04.27
# Last revised - 2022.07.19
#
# tx-bridge - second processing script
# Uses the 'pdal' conda environment


# ************************************************************
import argparse
from sklearn.cluster import DBSCAN # for point clustering
import pandas as pd
import geopandas as gpd

import os

import multiprocessing as mp
import tqdm
from time import sleep

import time
import datetime

import pylas # to read in the point cloud
# ************************************************************


# ------------------------------------------------------------
def fn_return_xyc(point):
    
    """
    Get a list of paramaters for a single LAS point

    Args:
        point: single las point from pylas
        
    Returns:
        list of parameters of las point
    """
    
    x = point[0]
    y = point[1]
    c = point[5] # classification
    return [x, y, c]
# ------------------------------------------------------------


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def fn_get_hull_polygons(dict_params):

    """
    Get polygon of clusters of point clouds data by classification.
    Uses the DBSCAN method from skikit-learn.

    Args:

        -- Getting these values from dictionary - for multiprocessing
        str_las_path: path to point cloud las
        int_lidar_class: classification which hulls will be generated
        flt_epsilon: DBSCAN epsilon - radial distance from point to be in neighboorhood in centimeters
        int_min_samples: DBSCAN - points within epsilon radius to anoint a core point
        
    Returns:

        gdf_bridge_hulls: geopandas geodataframe of point cloud cluster hulls
    """
    
    str_las_path = dict_params.get('str_las_path')
    int_lidar_class = dict_params.get('int_lidar_class')
    flt_epsilon = dict_params.get('flt_epsilon')
    int_min_samples = dict_params.get('int_min_samples')
    

    str_lambert = "epsg:3857"

    # read in the point cloud with pylas
    pcloud = pylas.read(str_las_path)

    # create a list of points from the points cloud
    points = [fn_return_xyc(i) for i in pcloud]

    # crete list of lists of points with desired classification
    list_selected_pts = [point for point in points if point[2] == int_lidar_class]

    # DBSCAN clustering
    sk_clustering = DBSCAN(eps = flt_epsilon, min_samples = int_min_samples).fit(list_selected_pts)

    # convert the clustering label to list - positive values are 'valid' clusters
    list_clustering = sk_clustering.labels_.tolist()

    # get index of the last valid cluster
    int_last_valid_cluster_index = max(list_clustering)

    # list to name the data coloumns
    list_col_val = ['x','y','class']

    # create pandas dataframe of point data
    df = pd.DataFrame(points, columns = list_col_val)
    df['cluster'] = list_clustering

    # TODO - 20220415 - Not sure if this is consistent - integer value returned - need to convert to float
    df['x'] = df['x']/100
    df['y'] = df['y']/100

    # create geodataframe of points pandas data
    gdf = gpd.GeoDataFrame(df, geometry = gpd.points_from_xy(df.x, df.y))

    # set the coordinate zone of the points geodataframe
    gdf = gdf.set_crs(str_lambert)

    # create an Empty DataFrame object
    gdf_bridge_hulls = gpd.GeoDataFrame(columns=['las_path', 'geometry'], geometry='geometry')

    # set the coordinate zone of the points geodataframe
    gdf_bridge_hulls = gdf_bridge_hulls.set_crs(str_lambert)

    for i in range(int_last_valid_cluster_index + 1):

        # filter a geodataframe to just the points in a specific cluster
        gdf_bridge = gdf[gdf.cluster==i]

        # convert this grouping's points into a single multipoint
        shp_multipoint_bridge =  gdf_bridge.unary_union

        # create a convex hull of all of the bridge points
        shp_poly_bridge_hull = shp_multipoint_bridge.convex_hull

        # crete a dictionary of this polygon
        dict_bridge_hull = {'las_path': str_las_path,
                            'geometry':  shp_poly_bridge_hull}

        # append this polygon to the geodataframe
        gdf_bridge_hulls = gdf_bridge_hulls.append(dict_bridge_hull, ignore_index = True)
    
    sleep(0.01) # this allows the tqdm progress bar to update
    
    return gdf_bridge_hulls
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# `````````````````````````````````````````````````````````````
def fn_polygonize_point_groups(str_las_input_directory, str_output_dir, int_class, flt_epsilon, int_min_samples):

    print(" ")
    print("+=================================================================+")
    print("|         POLYGONIZE POINT CLOUD GROUPS BY CLASSIFICATION         |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    
    print("  ---(i) INPUT DIRECTORY: " + str_las_input_directory)
    print("  ---(o) OUTPUT DIRECTORY: " + str_output_dir)
    print("  ---[c]   Optional: CLASSIFICATION: " + str(int_class) )
    print("  ---[e]   Optional: DBSCAN EPSILON: " + str(flt_epsilon) + " centimeters") 
    print("  ---[m]   Optional: DBSCAN MIN SAMPLES: " + str(int_min_samples) ) 
    print("===================================================================")
    
    str_lambert = "epsg:3857"
    
    # create the output directory if it does not exist
    os.makedirs(str_output_dir, exist_ok=True)
    
    list_files = []

    #TODO - Do we really want a walk and not just files in this folder? - 2022.04.27
    for root, dirs, files in os.walk(str_las_input_directory):
        for file in files:
            if file.endswith(".las") or file.endswith(".LAS"):
                # Note the case sensitive issue
                str_file_path = os.path.join(root, file)
                list_files.append(str_file_path)
    
    # get list of just the las files with points
    list_files_with_points = []
    
    for str_las_file_path in list_files:
        # read in the point cloud with pylas
        pcloud = pylas.read(str_las_file_path)
        points = [fn_return_xyc(i) for i in pcloud]
        if len(points) > 0:
            list_files_with_points.append(str_las_file_path)
    
    list_gdf_hulls = []
    
    list_of_dict = []
    
    for i in list_files_with_points:
        dict_params = {'str_las_path': i,
                       'int_lidar_class': int_class,
                       'flt_epsilon': flt_epsilon,
                       'int_min_samples': int_min_samples}
        list_of_dict.append(dict_params)
        
    
    l = len(list_files_with_points)
    p = mp.Pool(processes = (mp.cpu_count() - 1))
        
    list_gdf_hulls = list(tqdm.tqdm(p.imap(fn_get_hull_polygons, list_of_dict),
                                        total = l,
                                        desc='Processing LAS',
                                        bar_format = "{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
                                        ncols=65))
    p.close()
    p.join()
    
    # combine all the returned geodataframes
    gdf_hulls = pd.concat(list_gdf_hulls, ignore_index=True)
    
    # set a projection
    gdf_hulls = gdf_hulls.set_crs(str_lambert)
    
    # merging the overlapping polygons - polygons span multiple tiles
    # create a union of all the hulls
    gdf_hulls_merge = gdf_hulls.unary_union
    
    # convert the union hulls to geodataframe
    gdf_merge_polygons = gpd.GeoDataFrame([polygon for polygon in gdf_hulls_merge]).set_geometry(0)
    
    gdf_merge_polygons.rename_geometry('geometry', inplace=True)
    
    gdf_merge_polygons = gdf_merge_polygons.set_crs(str_lambert)
    
    # add a interim bridge id number to determine intersecting tiles
    gdf_merge_polygons.insert(0, 'temp_id', range(0, 0 + len(gdf_merge_polygons)))
    
    # intersect the polygons
    gdf_intersection = gdf_merge_polygons.overlay(gdf_hulls, how='intersection')
    
    list_clouds_per_poly = []
    
    for i in range(0, 0 + len(gdf_merge_polygons)):
        # get all the rows that match the temp_id
        gdf_current_poly = gdf_intersection.loc[gdf_intersection['temp_id'] == i]
        
        #TODO - need to check if no tiles returned - 2022.04.27
        
        # convert the coloumn to list
        list_tiles = gdf_current_poly['las_path'].tolist()
        
        list_clouds_per_poly.append(list_tiles)
        
    # add the 'list_clouds_per_poly' as new coloumn to gdf_merge_polygons
    gdf_merge_polygons['las_paths'] = list_clouds_per_poly
    
    # delete the 'temp_id' coloumn
    del gdf_merge_polygons['temp_id']
    
    # stringify list
    # TODO - 2022.07.21 - what is the list_clouds_per_poly is too long to fit into a field?
    gdf_merge_polygons['las_paths'] = gdf_merge_polygons['las_paths'].astype(str)
    
    str_file_shp_to_write = os.path.join(str_output_dir, 'class_' + str(int_class) +'_ar_3857.shp')
    gdf_merge_polygons.to_file(str_file_shp_to_write)
    
    # the geopackage does not truncate the 'las_path' field name converted from list
    str_file_gpkg_to_write = os.path.join(str_output_dir, 'class_' + str(int_class) +'_ar_3857.gpkg')
    gdf_merge_polygons.to_file(str_file_gpkg_to_write, driver='GPKG')
    print("+-----------------------------------------------------------------+")
    
# `````````````````````````````````````````````````````````````


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='========= POLYGONIZE POINT CLOUD GROUPS BY CLASSIFICATION =========')
    
    parser.add_argument('-i',
                        dest = "str_las_input_directory",
                        help=r'REQUIRED: directory containing LAS Example: C:\test\cloud_harvest\cloud_output',
                        required=True,
                        metavar='DIR',
                        type=str)

    parser.add_argument('-o',
                        dest = "str_output_dir",
                        help=r'REQUIRED: directory to write polygon shapefile: Example: C:\test\cloud_harvest\hull_polygons',
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
    
    parser.add_argument('-e',
                        dest = "flt_epsilon",
                        help='OPTIONAL: DBSCAN epsilon - distance from point to be in neighboorhood in centimeters: Default=250',
                        required=False,
                        default=250,
                        metavar='FLOAT',
                        type=float)
    
    parser.add_argument('-m',
                        dest = "int_min_samples",
                        help='OPTIONAL: DBSCAN - points within epsilon radius to anoint a core point: Default=4',
                        required=False,
                        default=4,
                        metavar='INTEGER',
                        type=int)   


    args = vars(parser.parse_args())
    
    str_las_input_directory = args['str_las_input_directory']
    str_output_dir = args['str_output_dir']
    int_class = args['int_class']
    flt_epsilon = args['flt_epsilon']
    int_min_samples = args['int_min_samples']

    fn_polygonize_point_groups(str_las_input_directory,
                               str_output_dir,
                               int_class,
                               flt_epsilon,
                               int_min_samples)
    
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~