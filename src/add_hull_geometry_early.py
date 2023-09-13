# Add the polygon of the 'bridge hull' as wkt
#
# Created by: Andy Carter, PE
# Created - 2022.11.11
# Last revised - 2023.09.05
#
# tx-bridge - sub-process of the 8th processing script
# Uses the 'pdal' conda environment

# ************************************************************

# ************************************************************
import geopandas as gpd
import pandas as pd

import pathlib
from pathlib import Path

import os

# ************************************************************

# ----------------------------------------------------
def fn_add_hull_geometry_early(gdf_appended_ln_w_hull_id, int_class):
    
    # option to turn off the SettingWithCopyWarning
    pd.set_option('mode.chained_assignment', None)
    
    print('Early add of bridge geometry...')
    
    gdf_mjr_axis_ln = gdf_appended_ln_w_hull_id
    
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
            pass
    
    print('+-----------------------------------------------------------------+')
    
    return(gdf_mjr_axis_ln)
# ----------------------------------------------------