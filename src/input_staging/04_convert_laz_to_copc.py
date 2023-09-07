# convert a single laz to a cloud optimized point cloud
# 04 -- for TACC Lidar collection bridge processing (step 4)
# Created by: Andy Carter, PE
# Created - 2023.08.15
# Last revised - 2023.08.15
#
# Uses the 'tx-bridge' conda environment

# ************************************************************
import argparse

import pdal
import json

import os

import time
import datetime
from datetime import date
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


# ..........................................................
def fn_create_copc(str_input_laz_filepath,
                   str_output_dir):

    """
    Convert a single LAZ to a COPC.LAZ

    """
    print(" ")
    print("+=================================================================+")
    print("|               CONVERT A SINGLE LAZ TO A COPC.LAZ                |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) INPUT LAS DIRECTORY: " + str_input_laz_filepath)
    print("  ---(o) OUTPUT DIRECTORY: " + str_output_dir)
    print("===================================================================")


    if not os.path.exists(str_output_dir):
        os.makedirs(str_output_dir)

    str_filename = os.path.basename(str_input_laz_filepath)
    
    str_output_copc = os.path.join(str_output_dir, str_filename[:-3] + "copc.laz")
    
    dict_pipeline_create_copc = {
    "pipeline": [
        {
            "filename":str_input_laz_filepath,
            "type":"readers.las",
            "tag":"readdata"
        },
        {
            "type":"writers.copc",
            "filename":str_output_copc
        }
    ]}

    print('Creating COPC...')
    pipeline = pdal.Pipeline(json.dumps(dict_pipeline_create_copc))
    n_points = pipeline.execute()
# ..........................................................


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()

    parser = argparse.ArgumentParser(description='============== CONVERT A SINGLE LAZ TO A COPC.LAZ =================')

    parser.add_argument('-i',
                        dest = "str_input_laz_filepath",
                        help=r'REQUIRED: filepath to laz point cloud: Example: D:\globus_transfer\003_br_st1_merge.laz',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))
    
    parser.add_argument('-o',
                        dest = "str_output_dir",
                        help=r'REQUIRED: directory to write created copc file: Example: D:\globus_transfer\copc_output_folder',
                        required=True,
                        metavar='DIR',
                        type=str)

    args = vars(parser.parse_args())

    str_input_laz_filepath = args['str_input_laz_filepath']
    str_output_dir = args['str_output_dir']

    fn_create_copc(str_input_laz_filepath,
                   str_output_dir)
                   
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)

    print('Compute Time: ' + str(time_pass))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~