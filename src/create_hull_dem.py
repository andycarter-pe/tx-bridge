# From the bridge hulls and the requested entwine point cloud las files,
# build a bridge deck dem for each polygon.
#
# Created by: Andy Carter, PE
# Created - 2022.05.16
# Last revised - 2022.05.16
#
# tx-bridge - fourth processing script
# Uses the 'pdal' conda environment


# ************************************************************
import argparse
import geopandas as gpd
import json
import pdal
import rioxarray as rxr
import os
import tqdm

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


# ````````````````````````````````````````````````````````
def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')
# ````````````````````````````````````````````````````````

# -------------------------------------------
def fn_delete_files(str_output_dir):
    
    list_files = []
    
    for root, dirs, files in os.walk(str_output_dir):
        for file in files:
            if file.endswith(".tif") or file.endswith(".TIF"):
                # Note the case sensitive issue
                str_file_path = os.path.join(root, file)
                list_files.append(str_file_path)
    
    for str_file_path in list_files:
        if str_file_path[-7:-4] == 'dem':
            os.remove(str_file_path)
# -------------------------------------------

# --------------------------------------------------------
def fn_create_hull_dems(str_bridge_polygons_path,str_output_dir,flt_dem_resolution,b_is_feet):
    
    """
    Create DEMS of the hulls from the classified point clouds

    Args:
        str_bridge_polygons_path: path convex hull polygons
        str_output_dir: where to write the road deck dems
        flt_dem_resolution: resolution of the DEM to create
        b_is_feet: T/F create data in vertical feet

    Returns:
        nothing
    """
    
    print(" ")
    print("+=================================================================+")
    print("|      CREATE DEM FOR BRIDGE DECKS FROM LAS AND POLYGON HULL      |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) INPUT CONVEX HULL SHAPEFILE PATH: " + str_bridge_polygons_path)
    print("  ---(o) OUTPUT DIRECTORY: " + str_output_dir)
    print("  ---[r]   Optional: RESOLUTION OF DEMS TO CREATE: " + str(flt_dem_resolution) )
    print("  ---[v]   Optional: VERTICAL DATA IN FEET: " + str(b_is_feet) )
    print("===================================================================")
    
    # create the output directory if it does not exist
    os.makedirs(str_output_dir, exist_ok=True)

    # read the bridge polygons
    gdf_bridge_ar = gpd.read_file(str_bridge_polygons_path)
    
    # TODO - Multiprocessing of this loop for speed - 20220516
    for index, row in tqdm.tqdm(gdf_bridge_ar.iterrows(),
                            total = gdf_bridge_ar.shape[0],
                            desc='Create DEMs',
                            bar_format = "{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
                            ncols=65):
    
        list_clouds = eval(gdf_bridge_ar.iloc[index]['las_paths'])
        
        # create a file name
        str_bridge_dem = os.path.join(str_output_dir, str(index) + '_bridge_deck_dem.tif')
        
        # create a pdal pipeline dictionaries - specific tile request
    
        # pdal readers for a single tile
        dict_read_single_las = {
            "type":"readers.las",
            "filename":list_clouds[0],
            "tag": "merge_las"
        }
    
        # pdal writers to create DEM
        dict_create = {
                "filename": str_bridge_dem,
                "gdalopts": "tiled=yes,     compress=deflate",
                "inputs": [ "merge_las" ],
                "nodata": -9999,
                "output_type": "idw",
                "resolution": flt_dem_resolution,
                "type": "writers.gdal"
            }
    
        # pdal merge multiple LAS tiles
        dict_merge = {
                "type" : "filters.merge",
                "tag": "merge_las"
        }
        
        list_pipeline = []
        
        # create a pdal pipeline for just one tile
        if len(list_clouds) == 1:
            list_pipeline = [dict_read_single_las, dict_create]
    
        # create pdal pipeline to merge multiple tiles
        if len(list_clouds) > 1:
            for i in list_clouds:
                # create pdal readers for each tile
                dict_current = {
                    "type":"readers.las",
                    "filename":i
                }
                list_pipeline.append(dict_current)
            list_pipeline.append(dict_merge)
            list_pipeline.append(dict_create)
        
        # create pipeline dictionary
        # pdal pipelines are dictionaries with a list of dictionaries
        dict_pipeline_merge = {
            "pipeline": list_pipeline
        }
        
        #execute the pdal pipeline
        pipeline = pdal.Pipeline(json.dumps(dict_pipeline_merge))
        n_points = pipeline.execute()
        
        if n_points > 0:
            #bridges were found
    
            with rxr.open_rasterio(str_bridge_dem, masked=True) as bridge_dem:
                # read the DEM as a "Rioxarray"
                #bridge_dem = rxr.open_rasterio(str_bridge_dem, masked=True).squeeze()
                
                # get a geodataframe of just one row
                gdf_singlerow = gdf_bridge_ar.iloc[[index],:]
        
                # TODO - processing error - 2022.12.03 - MAC
                # clip the DEM from points to the polygon limits
                #clipped = bridge_dem.rio.clip(gdf_singlerow.geometry,
                #                              gdf_singlerow.geometry.crs,
                #                              drop=True, invert=False)
                
                clipped = bridge_dem.rio.clip(gdf_singlerow.geometry,
                              drop=True, invert=False)
        
                # fill in the missing pixels
                filled = clipped.rio.interpolate_na()
        
                # clip the filled-in data to the polygon boundary
                # TODO - processing error - 2022.12.03 - MAC
                #clipped2 = filled.rio.clip(gdf_singlerow.geometry,
                #                           gdf_bridge_ar.geometry.crs,
                #                           drop=True, invert=False)
                
                clipped2 = filled.rio.clip(gdf_singlerow.geometry,
                                          drop=True, invert=False)
                
        
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
                
    fn_delete_files(str_output_dir)
# --------------------------------------------------------
    

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='======== BUILD BRIDGE DECK DEMS FROM BRIDGE HULL POLYGONS =========')

    
    parser.add_argument('-i',
                        dest = "str_bridge_polygons_path",
                        help=r'REQUIRED: path to bridge hull polygon shapefile Example: C:\test\cloud_harvest\02_shapefile_of_hulls\class_17_ar_3857.shp',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))

    parser.add_argument('-o',
                        dest = "str_output_dir",
                        help=r'REQUIRED: directory to write deck dems: Example: C:\test\cloud_harvest\05_bridge_deck_dems',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-r',
                        dest = "flt_dem_resolution",
                        help='OPTIONAL: resolution of the bridge deck dem: Default=0.3',
                        required=False,
                        default=0.3,
                        metavar='FLOAT',
                        type=float)
    
    parser.add_argument('-v',
                        dest = "b_is_feet",
                        help='OPTIONAL: create vertical data in feet: Default=True',
                        required=False,
                        default=True,
                        metavar='T/F',
                        type=str2bool)
    
    args = vars(parser.parse_args())
    
    str_bridge_polygons_path = args['str_bridge_polygons_path']
    str_output_dir = args['str_output_dir']
    flt_dem_resolution = args['flt_dem_resolution']
    b_is_feet = args['b_is_feet']
    
    fn_create_hull_dems(str_bridge_polygons_path,
                        str_output_dir,
                        flt_dem_resolution,
                        b_is_feet)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~