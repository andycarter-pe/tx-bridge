# merge all the las files in a single directory
# -- for TACC Lidar collection bridge processing (step 3)
# Created by: Andy Carter, PE
# Created - 2023.02.17
#
# Uses the 'tx-bridge' conda environment

# ************************************************************
import argparse

import pdal
import json

import os

import time
import datetime
from time import sleep
from datetime import date
# ************************************************************


# -----------------------------------------
def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')
# -----------------------------------------


# ................................
def fn_merge_las(str_input_directory,
                 str_out_directory,
                 str_output_filename,
                 b_create_copc):
    
    print(" ")
    print("+=================================================================+")
    print("|             MERGE ALL LAS POINT CLOUDS IN DIRECTORY             |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) INPUT LAS DIRECTORY: " + str_input_directory)
    print("  ---(o) OUTPUT DIRECTORY: " + str_out_directory)
    print("  ---(f) OUTPUT FILENAME: " + str_output_filename)
    print("  ---[c]   Optional: Create CoPC: " + str(b_create_copc) )
    print("===================================================================")
    
    
    # --- merging the las files (assumed in same projection) ---
    # if merge file already exists, don't create it
    str_merge_las_path = os.path.join(str_out_directory, str_output_filename + '.las')
    
    if os.path.exists(str_merge_las_path):
        print('Merged las already exists')
    else:
        # create a list of all 'las' in a directory
        list_las_files = []
        # r=root, d=directories, f = files
        for r, d, f in os.walk(str_input_directory):
            for file in f:
                if file.endswith(".las"):
                    list_las_files.append(os.path.join(r, file))
                
        # last path in list is where pdal will write the merged output
        str_merge_las_path = os.path.join(str_out_directory, str_output_filename + '.las')
        list_las_files.append(str_merge_las_path)
        
        #execute the pdal pipeline
        print('Merging LAS files....')
        pipeline = pdal.Pipeline(json.dumps(list_las_files))
        n_points = pipeline.execute()
    # ------
    
    
    # --- creating CoPC of merged LAS ---
    str_copc_out = str_merge_las_path[:-4] + '.copc.laz'
    
    if os.path.exists(str_copc_out):
        print('CoPC already exists')
    else:
        # if asked... create the COPC as output
        if b_create_copc:
            
            pipeline_create_copc = {
                "pipeline": [
                    {   
                        "filename":str_merge_las_path,
                        "type":"readers.las",
                        "tag":"readdata"
                    },
                    {
                        "filename": str_copc_out,
                        "inputs": [ "readdata" ],
                        "type": "writers.copc"
                    }
                ]}
            #execute the pdal pipeline
            pipeline = pdal.Pipeline(json.dumps(pipeline_create_copc))
            n_points = pipeline.execute()
    # ------
# ................................

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()    
    parser = argparse.ArgumentParser(description='=========== MERGE ALL LAS POINT CLOUDS IN DIRECTORY ==========')


    parser.add_argument('-i',
                        dest = "str_input_directory",
                        help=r'REQUIRED: path to directory containing LAS: Example: D:\working\test_dump',
                        required=True,
                        metavar='DIR',
                        type=str)

    parser.add_argument('-o',
                        dest = "str_out_directory",
                        help=r'REQUIRED: path to write merged outputs: Example D:\working\merge_output_folder',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-f',
                        dest = "str_output_filename",
                        help=r'REQUIRED: name of file to create (no extension): Example bridge_dallas_collection',
                        required=True,
                        metavar='DIR',
                        type=str)

    parser.add_argument('-c',
                        dest = "b_create_copc",
                        help='OPTIONAL: create cloud optimized point cloud: Default=True',
                        required=False,
                        default=True,
                        metavar='T/F',
                        type=str2bool)
    
    args = vars(parser.parse_args())
    
    str_input_directory = args['str_input_directory']
    str_out_directory = args['str_out_directory']
    str_output_filename = args['str_output_filename']
    b_create_copc = args['b_create_copc']
    
    fn_merge_las(str_input_directory,
                 str_out_directory,
                 str_output_filename,
                 b_create_copc)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~