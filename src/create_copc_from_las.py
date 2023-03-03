# create a cloud optimized point cloud from a single las
# -- for TACC Lidar collection bridge processing
# Created by: Andy Carter, PE
# Created - 2023.02.27
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


# ................................
def fn_create_copc(str_input_file,
                   str_out_directory,
                   str_output_filename):
    
    print(" ")
    print("+=================================================================+")
    print("|                CREATE COPC FROM LAS POINT CLOUD                 |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) INPUT LAS DIRECTORY: " + str_input_file)
    print("  ---(o) OUTPUT DIRECTORY: " + str_out_directory)
    print("  ---[f]   Optional output filename: " + str_output_filename)
    print("===================================================================")
    

    if os.path.exists(str_input_file):
        
        if not os.path.exists(str_out_directory):
            os.makedirs(str_out_directory)
            
        if str_output_filename == 'none':
            # get the filename from the str_input_file
            head, tail = os.path.split(str_input_file)
            str_file_to_create = tail[:-4] + '.copc.laz'
        else:
            str_file_to_create = str_output_filename + '.copc.laz'
        
        str_copc_out = os.path.join(str_out_directory, str_file_to_create)
        print('Creating COPC: ' + str_copc_out)
        
        pipeline_create_copc = {
        "pipeline": [
            {   
                "filename":str_input_file,
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
    
    else:
        print(' Input file: ' + str_input_file + ' - not found')
    # ------
# ................................

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()    
    parser = argparse.ArgumentParser(description='==============  CREATE COPC FROM LAS POINT CLOUD  ============')


    parser.add_argument('-i',
                        dest = "str_input_file",
                        help=r'REQUIRED: path to LAS: Example: D:\working\test_dump\input_data.las',
                        required=True,
                        metavar='FILE',
                        type=str)

    parser.add_argument('-o',
                        dest = "str_out_directory",
                        help=r'REQUIRED: path to write merged outputs: Example D:\working\merge_output_folder',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-f',
                        dest = "str_output_filename",
                        help=r'Optional: name of file to create (no extension): Example bridge_dallas_collection',
                        required=False,
                        default='none',
                        metavar='STRING',
                        type=str)


    args = vars(parser.parse_args())
    
    str_input_file = args['str_input_file']
    str_out_directory = args['str_out_directory']
    str_output_filename = args['str_output_filename']
    
    fn_create_copc(str_input_file,
                   str_out_directory,
                   str_output_filename)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~