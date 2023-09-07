# reproject all las files in a given directory to a user suplied coordinate reference system
# 02 -- for TACC Lidar collection bridge processing (step 2)
# Created by: Andy Carter, PE
# Created - 2023.02.17
# Last revised - 2023.08.15
#
# Uses the 'tx-bridge' conda environment

# ************************************************************
import argparse

import pdal
import json
import tqdm

import os
import multiprocessing as mp

import time
import datetime
from time import sleep
from datetime import date
# ************************************************************


# **********************************
def fn_reproject_single_las(dict_current_pipeline):
    #execute the pdal pipeline
    try:
        pipeline = pdal.Pipeline(json.dumps(dict_current_pipeline))
        n_points = pipeline.execute()
    except:
        print('File with issues: ' + dict_current_pipeline['pipeline'][0]['filename'])

    sleep(0.01) # this allows the tqdm progress bar to update

    return 1
# **********************************

# ..........................................................
def fn_reproject_las_in_dir(str_input_las_dir,
                            str_output_dir,
                            str_projection,
                            int_cores):


    """
    Convert all las in file to the same projection

    """

    print(" ")
    print("+=================================================================+")
    print("|       REPROJECT ALL POINT CLOUD LAS IN A GIVEN DIRECTORY        |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) INPUT LAS DIRECTORY: " + str_input_las_dir)
    print("  ---(o) OUTPUT DIRECTORY: " + str_output_dir)
    print("  ---[p]   Optional: Re-projected CRS: " + str(str_projection) )
    print("  ---[n]   Optional: Number of Cores: " + str(int_cores))
    print("===================================================================")

    # create a list of all 'las' in a directory
    #print(" ")
    #print("Determining files to process...")
    list_las_files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(str_input_las_dir):
        for file in f:
            if file.endswith(".las"):
                list_las_files.append(os.path.join(r, file))

    # create pdal pipeline dictionaries for each file
    #print(" ")
    #print("Creating PDAL pipelines...")

    list_pipeline_dict = []

    for item in list_las_files:
        str_basename = os.path.basename(item)

        str_input_file = item
        str_output_file = os.path.join(str_output_dir, str_basename)

        pipeline_reproject_las = {
            "pipeline": [
                {
                    "filename":str_input_file,
                    "type":"readers.las",
                    "tag":"readdata"
                },
                {
                    "type":"filters.reprojection",
                    "out_srs":"EPSG:3857+5703",
                    "tag":"reproj_points"
                },
                {
                    "filename": str_output_file,
                    "inputs": [ "reproj_points" ],
                    "type": "writers.las"
                }
            ]}
        list_pipeline_dict.append(pipeline_reproject_las)

    # kick off a multiprocessing las creation
    print('Begin multi-procressing of re-projection....')
    print("+-----------------------------------------------------------------+")

    if int_cores == 0 or int_cores >= mp.cpu_count():
        p = mp.Pool(processes = (mp.cpu_count() - 1))
    else:
        p = mp.Pool(processes = int_cores)

    l = len(list_pipeline_dict)

    list_return_values = list(tqdm.tqdm(p.imap(fn_reproject_single_las, list_pipeline_dict),
                                        total = l,
                                        desc='Re-projecting',
                                        bar_format = "{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
                                        ncols=65))

    p.close()
    p.join()
# ..........................................................

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()

    parser = argparse.ArgumentParser(description='====== REPROJECT ALL POINT CLOUD LAS IN A GIVEN DIRECTORY =========')

    parser.add_argument('-i',
                        dest = "str_input_las_dir",
                        help=r'REQUIRED: directory containing las files: Example: D:\globus_transfer\austin_bexar_bridge',
                        required=True,
                        metavar='DIR',
                        type=str)

    parser.add_argument('-o',
                        dest = "str_output_dir",
                        help=r'REQUIRED: directory to write reprojected las file: Example: D:\globus_transfer\austin_bexar_bridge_reproject',
                        required=True,
                        metavar='DIR',
                        type=str)

    parser.add_argument('-p',
                        dest = "str_projection",
                        help='OPTIONAL: EPSG projection: Default: EPSG:3857+5703',
                        required=False,
                        default='EPSG:3857+5703',
                        metavar='STRING',
                        type=str)

    parser.add_argument('-n',
                    dest = "int_cores",
                    help='OPTIONAL: number of cores: Default=0 (will deploy all cores, less one for overhead)',
                    required=False,
                    default=0,
                    metavar='INTEGER',
                    type=int)

    args = vars(parser.parse_args())

    str_input_las_dir = args['str_input_las_dir']
    str_output_dir = args['str_output_dir']
    str_projection = args['str_projection']
    int_cores = args['int_cores']


    fn_reproject_las_in_dir(str_input_las_dir,
                            str_output_dir,
                            str_projection,
                            int_cores)


    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)

    print('Compute Time: ' + str(time_pass))