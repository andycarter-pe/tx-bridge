# User provides file path to a geotiff that covers the entire area of
# interest in addition to directory that contains geotiffs that cover areas
# of the overall raster.  The smaller rasters are aligned and scaled to match
# the overall raster.  These smaller rasters are compositied into the overall
# raster.
#
# This was written for the tx-bridge suite so that the high resolution bridge
# deck data can be added into the watershed's bare earth terrain to create
# a 'healed' terrain.
#
# Created by: Andy Carter, PE
# Created - 2022.07.18
# Last revised - 2022.07.18
#
# tx-bridge - tenth (10) processing script
# Uses the 'pdal' conda environment


# ************************************************************
import warnings
warnings.filterwarnings('ignore')

import argparse

import xarray
from rioxarray.merge import merge_arrays

import os

from tqdm import tqdm

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

# `````````````````````````````````````````````````````````````
def fn_composite_terrain(str_input_path,str_small_dem_dir_path,str_output_dir,str_output_dem_name):

    print(" ")
    print("+=================================================================+")
    print("|                    COMPOSITE RASTER TERRAINS                    |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    
    print("  ---(i) INPUT OVERALL RASTER: " + str_input_path)
    print("  ---(d) INPUT SMALL RASTER DIRECTORY: " + str_small_dem_dir_path)
    print("  ---(o) OUTPUT DIRECTORY: " + str_output_dir)
    print("  ---[c]   Optional: OUTPUT DEM NAME: " + str_output_dem_name )
    print("===================================================================")
    
    # create the output directory if it does not exist
    os.makedirs(str_output_dir, exist_ok=True)
    
    # determine all the "small" raster in a given directory
    list_files = []
    
    #TODO - Do we really want a walk and not just files in this folder? - 2022.017.18
    for root, dirs, files in os.walk(str_small_dem_dir_path):
        for file in files:
            if file.endswith(".tif") or file.endswith(".TIF"):
                str_file_path = os.path.join(root, file)
                list_files.append(str_file_path)
    
    if len(list_files) > 0:
        
        # open the base dem
        xds_base_to_match = xarray.open_rasterio(str_input_path)
        list_xds_rasters = []
        
        # loop through all the rasters 
        # TODO - 2022.07.18 - this throws an error on the first pass
        # 'Warning 1: +init=epsg:XXXX syntax is deprecated.'
    
        for str_current_small_raster in tqdm(list_files, 
                                             desc='Composite',
                                             bar_format = "{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
                                             ncols=65):

            # open the smaller raster to allign
            xds = xarray.open_rasterio(str_current_small_raster)

            # reproject and resample the smaller raster to the larger reference
            xds_repr_match = xds.rio.reproject_match(xds_base_to_match)
 
            list_xds_rasters.append(xds_repr_match)
 
        list_xds_rasters.append(xds_base_to_match)

        print('Merging all rasters...')
        
        #merge the xarray raster
        xds_merge = merge_arrays(list_xds_rasters)
        
        # write out the raster
        if str_output_dem_name == '':
            # get the filename from input path
            head, tail = os.path.split(str_input_path)
            str_filename_from_path = tail
            
            # split the filename
            f_name, f_ext = os.path.splitext(str_filename_from_path)
            str_filename = f_name + '_healed' + f_ext
        else:
            str_filename = str_output_dem_name + '.tif'
        str_out_dem = os.path.join(str_output_dir, str_filename )
        
        xds_merge.rio.to_raster(str_out_dem)
    else:
        print('No tifs found for compositing.')
 
# `````````````````````````````````````````````````````````````


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='==================== COMPOSITE RASTER TERRAINS ====================')
    
    parser.add_argument('-i',
                        dest = "str_input_path",
                        help=r'REQUIRED: path to the overall raster (geotiff) Example: C:\test\09_bare_earth_dem\120903010206.tif',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))
    
    parser.add_argument('-d',
                        dest = "str_small_dem_dir_path",
                        help=r'REQUIRED: directory with rasters to composite: C:\test\11_healed_dem',
                        required=True,
                        metavar='DIR',
                        type=str)

    parser.add_argument('-o',
                        dest = "str_output_dir",
                        help=r'REQUIRED: directory to write composite DEM Example: C:\test\11_healed_dem',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-f',
                        dest = "str_output_dem_name",
                        help='OPTIONAL: name of output file (omit .tif)',
                        required=False,
                        default='',
                        metavar='STRING',
                        type=str)   
    
    args = vars(parser.parse_args())
    
    str_input_path = args['str_input_path']
    str_small_dem_dir_path = args['str_small_dem_dir_path']
    str_output_dir = args['str_output_dir']
    str_output_dem_name = args['str_output_dem_name']


    fn_composite_terrain(str_input_path,
                         str_small_dem_dir_path,
                         str_output_dir,
                         str_output_dem_name)

    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~