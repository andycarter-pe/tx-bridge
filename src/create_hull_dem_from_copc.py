# From the bridge hulls and the requested entwine point cloud las files,
# build a bridge deck dem for each polygon.
#
# Created by: Andy Carter, PE
# Created - 2023.08.02
# Last revised - 2023.08.02
#
# tx-bridge - fourth processing script
# Uses the 'pdal' conda environment

# ************************************************************
import argparse
import configparser
import geopandas as gpd
import json
import pdal
import rioxarray as rxr
import os
import tqdm

from shapely.geometry import MultiPolygon, Polygon
from shapely.wkt import loads

import multiprocessing as mp
from multiprocessing import Pool

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


# ---------------------------------------------
def fn_create_hull_dem_from_copc(str_bridge_polygons_path,
                                 str_input_json,
                                 str_global_config_ini_path):
    
    """
    Create DEMS of the hulls from the classified point clouds

    Returns:
        nothing
    """
    
    print(" ")
    print("+=================================================================+")
    print("|     CREATE DEM FOR BRIDGE DECKS FROM COPC AND POLYGON HULLS     |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) INPUT HULL SHAPEFILE PATH: " + str_bridge_polygons_path)
    print("  ---(l) JSON CONFIG FOR RUN FILEPATH: " + str_input_json)
    print("  ---[g]   Optional: GLOBAL INI FILEPATH: " + str_global_config_ini_path )
    print("===================================================================")
    
    #get the input values from the INI
    # convert the INI to a dictionary
    dict_global_config_data = json.loads(fn_json_from_ini(str_global_config_ini_path))
    
    flt_dem_resolution = float(dict_global_config_data['05_create_hull_dems']['flt_dem_resolution'])

    # parse the run's configuration file
    with open(str_input_json) as f:
        json_run_data = json.load(f)
    
    # variables from run configuration json
    str_output_folder = json_run_data["str_output_folder"]
    b_is_feet = json_run_data["b_is_feet"]
    str_input_copc_file = json_run_data["copc_point_cloud"]["copc_filepath"]

    # the desired output location
    str_dem_output_folder = os.path.join(str_output_folder, '05_bridge_deck_dems')
    
    # create the output directory if it does not exist
    os.makedirs(str_dem_output_folder, exist_ok=True)
    
    # read the bridge polygons
    gdf_bridge_ar = gpd.read_file(str_bridge_polygons_path)
    

    #append the dataframe with in input parameters
    gdf_bridge_ar['copc_path'] = str_input_copc_file
    gdf_bridge_ar['is_feet'] = b_is_feet
    gdf_bridge_ar['dem_res'] = flt_dem_resolution
    gdf_bridge_ar['output_folder'] = str_dem_output_folder
    
    l = len(gdf_bridge_ar)
    
    list_of_single_row_gdfs = [gpd.GeoDataFrame([row], crs=gdf_bridge_ar.crs) for _, row in gdf_bridge_ar.iterrows()]
    
    # create a pool of processors
    # TODO -2023.08.02 - overwrite with the smaller of
    # avialble processors and user supplied num of processors
    num_processors = (mp.cpu_count() - 1)
    pool = Pool(processes = num_processors)
    
    list_str_dems_created = list(tqdm.tqdm(pool.imap(fn_create_single_hull_dem, list_of_single_row_gdfs),
                                           total = l,
                                           desc='Create DEMs',
                                           bar_format = "{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
                                           ncols=67 ))

    pool.close()
    pool.join()
    
# ---------------------------------------------   
    
    

# ..............................................
def fn_create_single_hull_dem(gdf_singlerow):
    
    # given a geodataframe containing a single row (has geometry and index)
    int_row_index = gdf_singlerow.index[0]
    geometry_first_row = gdf_singlerow.geometry.loc[int_row_index]
    str_geom_wkt = geometry_first_row.wkt
    
    # get the variables from the single dataframe
    str_copc_filepath = gdf_singlerow.iloc[0]['copc_path']
    b_is_feet = gdf_singlerow.iloc[0]['is_feet']
    flt_dem_resolution = gdf_singlerow.iloc[0]['dem_res']
    str_output_dir = gdf_singlerow.iloc[0]['output_folder']
    
    # create a file name
    str_bridge_dem = os.path.join(str_output_dir, str(int_row_index) + '_bridge_deck_dem.tif')

    pipeline_class_las = {
        "pipeline": [
            {   
                'polygon':str_geom_wkt,
                "filename":str_copc_filepath,
                "type":"readers.copc",
                "tag":"readdata"
            },
            {
                "filename": str_bridge_dem,
                "gdalopts": "tiled=yes,     compress=deflate",
                "inputs": "readdata",
                "nodata": -9999,
                "output_type": "idw",
                "resolution": flt_dem_resolution,
                "type": "writers.gdal"
            }
        ]}

    #execute the pdal pipeline
    pipeline = pdal.Pipeline(json.dumps(pipeline_class_las))
    n_points = pipeline.execute()

    with rxr.open_rasterio(str_bridge_dem, masked=True) as bridge_dem:
        # read the DEM as a "Rioxarray"

        # clip the DEM from points to the polygon limits
        clipped = bridge_dem.rio.clip(gdf_singlerow.geometry, drop=True, invert=False)

        # fill in the missing pixels
        filled = clipped.rio.interpolate_na()

        # clip the filled-in data to the polygon boundary
        clipped2 = filled.rio.clip(gdf_singlerow.geometry, drop=True, invert=False)

        # convert vertical values to meters
        if b_is_feet:
            # scale the raster from meters to feet
            clipped2 = clipped2 * 3.28084

            # write out the raster
            bridge_dem_out = str_bridge_dem[:-4] + '_vert_ft.tif'
            clipped2.rio.to_raster(bridge_dem_out, compress='LZW', dtype="float32")

        else:
            # write out the raster
            bridge_dem_out = str_bridge_dem[:-4] + '_vert_m.tif'
            clipped2.rio.to_raster(bridge_dem_out, compress='LZW', dtype="float32")
        
    # TODO - 2023.08.02 - MAC - delete the DEM created with PDAL as intermediate file
    return(str_bridge_dem)
# ..............................................

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='======== BUILD BRIDGE DECK DEMS FROM BRIDGE HULL POLYGONS ========')

    
    parser.add_argument('-i',
                        dest = "str_bridge_polygons_path",
                        help=r'REQUIRED: path to bridge hull polygon shapefile Example: C:\test\cloud_harvest\02_shapefile_of_hulls\class_17_ar_3857.shp',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))

    parser.add_argument('-l',
                        dest = "str_input_json",
                        help=r'REQUIRED: path to the input coniguration json Example: D:\bridge_local_test\config.json',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))
    
    parser.add_argument('-g',
                        dest = "str_global_config_ini_path",
                        help=r'OPTIONAL: global variable initialization file: Example: C:\Users\civil\dev\tx-bridge\src\config_global.ini',
                        required=False,
                        default=r'C:\Users\civil\dev\tx-bridge\src\config_global.ini',
                        metavar='FILE',
                        type=str)
    
    args = vars(parser.parse_args())
    
    str_bridge_polygons_path = args['str_bridge_polygons_path']
    str_input_json =args['str_input_json']
    str_global_config_ini_path = args['str_global_config_ini_path']

    fn_create_hull_dem_from_copc(str_bridge_polygons_path,
                                 str_input_json,
                                 str_global_config_ini_path)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~