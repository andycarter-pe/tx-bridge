# Running a 'tx_bridge' for an entire shapefile of polygons
#
# Created by: Andy Carter, PE
# Last revised - 2022.07.19
#
# Uses the 'tx-bridge' conda environment

# ********************************************
from tx_bridge import fn_run_tx_bridge

import argparse
import os
import geopandas as gpd

import time
import datetime
# ********************************************


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


# ----------------------------------------------
def fn_run_multiple_tx_bridge(str_input_shp_path_arg,
                              str_out_arg,
                              int_class,
                              b_is_feet,
                              str_field_name):
    

    flt_start_run_tx_bridge = time.time()
    
    print(" ")
    print("+=================================================================+")
    print("|          RUN TX_BRIDGE FOR ALL POLYGONS IN SHAPEFILE            |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")
    
    print("  ---(i) INPUT SHAPEFILE: " + str(str_input_shp_path_arg))
    print("  ---(o) OUTPUT DIRECTORY: " + str(str_out_arg))   
    print("  ---[c]   Optional: Point Classification: " + str(int_class))
    print("  ---[v]   Optional: Vertical in feet: " + str(b_is_feet))
    print("  ---[f]   Optional: Naming Field: " + str(str_field_name))

    print("===================================================================")
    print(" ")
    
    if not os.path.exists(str_out_arg):
        os.mkdir(str_out_arg)
    
    # load the polygon geodataframe
    gdf_polygons = gpd.read_file(str_input_shp_path_arg)
    
    for index, row in gdf_polygons.iterrows():
        gdf_single_poly = gdf_polygons.loc[[index]]
        
        str_field_label = gdf_single_poly.iloc[0][str_field_name]
        print(str_field_label)
        str_sub_folder = os.path.join(str_out_arg, str_field_label)
        
        if not os.path.exists(str_sub_folder):
            os.mkdir(str_sub_folder)
            
        # single polygon shapefile folder
        str_single_shape_folder = os.path.join(str_sub_folder, '00_input_shapefile')
        
        if not os.path.exists(str_single_shape_folder):
            os.mkdir(str_single_shape_folder)
            
        # single polygon shapefile folder
        str_single_shape_file = os.path.join(str_single_shape_folder, str_field_label + '_ar.shp')
        gdf_single_poly.to_file(str_single_shape_file)
        
        int_start_step = 1
        
        fn_run_tx_bridge(str_single_shape_file,
                         str_sub_folder,
                         int_class,
                         b_is_feet,
                         int_start_step)
        
    flt_end_run_run_tx_bridge = time.time()
    flt_time_pass_tx_bridge = (flt_end_run_run_tx_bridge - flt_start_run_tx_bridge) // 1
    time_pass_tx_bridge = datetime.timedelta(seconds=flt_time_pass_tx_bridge)
    
    print('Total Compute Time: ' + str(time_pass_tx_bridge))
# ----------------------------------------------

# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ 
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='========== RUN TX_BRIDGE FOR ALL POLYGONS IN SHAPEFILE ===========')
    

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
    
    parser.add_argument('-f',
                        dest = "str_field_name",
                        help='OPTIONAL: unique field from input shapefile',
                        required=False,
                        default='',
                        metavar='STRING',
                        type=str)
    
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
    str_field_name = args['str_field_name']
    
    int_start_step = 1
    
    fn_run_multiple_tx_bridge(str_input_shp_path_arg,
                              str_out_arg,
                              int_class,
                              b_is_feet,
                              str_field_name)