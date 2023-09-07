# From the '08_06_mjr_axis_xs_w_feature_id_nbi_low.gpkg' 
# (1) add a latitiude and longitude of the centerpoint of the 'major axis line'
# (2) add the polygon of the 'bridge hull' as wkt
#
# Created by: Andy Carter, PE
# Created - 2022.11.11
# Last revised - 2022.11.11
#
# tx-bridge - sub-process of the 8th processing script
# Uses the 'pdal' conda environment

# ************************************************************

# ************************************************************
import argparse

import geopandas as gpd

import pathlib
from pathlib import Path

import os

import time
import datetime
# ************************************************************

# ----------------------------------------------------
def fn_add_hull_geometry(str_input_dir, int_class):
    
    
    print('Tranfering bridge hull geometry...')
    
    # --- build file paths to the required input folders ---
    str_path_to_mjr_axis_gpkg = os.path.join(str_input_dir, '08_cross_sections', '08_06_mjr_axis_xs_w_feature_id_nbi_low.gpkg')

    if os.path.exists(str_path_to_mjr_axis_gpkg):
        # file is found
        gdf_mjr_axis_ln = gpd.read_file(str_path_to_mjr_axis_gpkg)
        
        # create a new coloumn
        gdf_mjr_axis_ln['hull_wkt'] = ''
        
        # get list of the unique 'file_path' in gdf_mjr_axis_ln
        list_unique_filepath = gdf_mjr_axis_ln.file_path.unique().tolist()
        
        for str_filepath_unique in list_unique_filepath:
            # geodataframe of major axis in a unique file
            gdf_majr_axis_ln_filepath = gdf_mjr_axis_ln.loc[gdf_mjr_axis_ln['file_path'] == str_filepath_unique]
            
            # build path to hull shapefile
            path, file = os.path.split(str_filepath_unique)
            list_path_parts = list(Path(path).parts)
            
            # change the folder name to the '02_shapefile_hulls'
            list_path_parts[-1] = '02_shapefile_of_hulls'
            str_root_path = pathlib.Path(*list_path_parts)
            
            # build the filename
            str_filename = "class_" + str(int_class) + "_ar_3857.shp"
            
            # combine the header and the filename
            str_filepath_hulls = os.path.join(str_root_path, str_filename)
            
            if os.path.exists(str_filepath_hulls):
                gdf_hulls_per_file = gpd.read_file(str_filepath_hulls)
                
                # reproject gdf_hulls_per_file to gdf_mjr_axis_ln crs
                gdf_hulls_per_file_local_prj = gdf_hulls_per_file.to_crs(gdf_mjr_axis_ln.crs)
                
                for index, row in gdf_majr_axis_ln_filepath.iterrows():
                    int_hull_index = int(row['hull_idx'])
                    geom = gdf_hulls_per_file_local_prj.loc[int_hull_index]['geometry']
                    str_hull_geom = str(geom.wkt)
                    
                    # append the hull_wkt
                    gdf_mjr_axis_ln.at[index, 'hull_wkt'] = str_hull_geom
            else:
                print("  ERROR: Required File Not Found: " + str_filepath_hulls)
        
        print('+-----------------------------------------------------------------+')
        # ------- Exporting the revised attributed major axis lines
        str_path_xs_folder = os.path.join(str_input_dir, '08_cross_sections')
                
        # create the output directory if it does not exist
        os.makedirs(str_path_xs_folder, exist_ok=True)
        
        str_major_axis_xs_file = os.path.join(str_path_xs_folder, '08_07_mjr_axis_xs_w_feature_id_nbi_low_hull.gpkg')
        
        # export the geopackage
        gdf_mjr_axis_ln.to_file(str_major_axis_xs_file, driver='GPKG')
        
        # ----
                
    else:
        print("  ERROR: Required File Not Found: " + str_path_to_mjr_axis_gpkg)
# ----------------------------------------------------



#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='========== ADD BRIDGE HULL GEOMETRY AS WELL KNOWN TEXT ============')
    
    parser.add_argument('-i',
                        dest = "str_input_dir",
                        help=r'REQUIRED: input directory of processed data for area of interest [contains 00 to 08 folders]: Example: C:\bridge_data\folder_location',
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
    
    args = vars(parser.parse_args())
    
    str_input_dir = args['str_input_dir']
    int_class = args['int_class']
    
    print(" ")
    print("+=================================================================+")
    print("|          ADD BRIDGE HULL GEOMETRY AS WELL KNOWN TEXT            |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) PATH TO INPUT FOLDERS: " + str_input_dir)
    print("  ---[c]   Optional: CLASSIFICATION: " + str(int_class) )
    print("===================================================================")

    fn_add_hull_geometry(str_input_dir, int_class)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
