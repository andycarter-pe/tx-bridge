# Given an input directory of laz files, extract and save cooresponding bridge
# deck classification points (las) for each tile
# -- for TACC Lidar collection bridge processing (step 1)
#
# Created by: Andy Carter, PE
# Created - 2022.01.31
# Last revised - 2022.02.17
# # Uses the 'tx-bridge' conda environment

# ************************************************************
import argparse
import os
import pdal
import json

import multiprocessing as mp
from multiprocessing import Pool
import tqdm
from time import sleep

import time
import datetime
# ************************************************************

 
# **********************************
def fn_create_las(dict_current_pipeline):
    #execute the pdal pipeline
    try:
        pipeline = pdal.Pipeline(json.dumps(dict_current_pipeline))
        n_points = pipeline.execute()
    except:
        print('File with issues: ' + dict_current_pipeline['pipeline'][0]['filename'])
        
    sleep(0.01) # this allows the tqdm progress bar to update
        
    return 1
# **********************************


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def fn_list_not_yet_processed(str_input_directory,
                              str_out_directory,
                              int_class):
    
    # get a list of all the .laz files in the input directory

    list_valid_name_to_process = []
    if os.path.exists(str_input_directory):
        files = os.listdir(str_input_directory)
        list_file_names = [f for f in files if os.path.isfile(str_input_directory+'/'+f)] #Filtering only the files.

        for file in list_file_names:
            if file.endswith(".laz") or file.endswith(".LAZ"):
                list_valid_name_to_process.append(file[:-4])

    # get a list of all the .las file in the output directory
    str_expected_end = '_class_' + str(int_class) + '.las'

    list_already_processed_output = []
    if os.path.exists(str_out_directory):
        files_output = os.listdir(str_out_directory)
        list_output_file_names = [f for f in files_output if os.path.isfile(str_out_directory+'/'+f)] #Filtering only the files.

        for file in list_output_file_names:
            if file.endswith(str_expected_end):
                list_already_processed_output.append(file[:len(str_expected_end)*-1])

    # get items in 'list_valid_name_to_process' that aren't in 'list_already_processed_output'
    list_to_process = list(set(list_valid_name_to_process).difference(set(list_already_processed_output)))

    return(list_to_process)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# --------------------------------------------------------
def fn_extract_classification(str_input_directory,
                              str_out_directory,
                              int_class,
                              int_cores):
    

    flt_start_extract_classification = time.time()
    
    print(" ")
    print("+=================================================================+")
    print("|    EXTRACT POINT CLOUD CLASSIFICATION FROM DIRECTORY OF LAZ     |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")
    
    print("  ---(i) INPUT DIRECTORY: " + str(str_input_directory))
    print("  ---(o) OUTPUT DIRECTORY: " + str(str_out_directory))   
    print("  ---[c]   Optional: Point Classification: " + str(int_class))
    print("  ---[n]   Optional: Number of Cores: " + str(int_cores))

    print("===================================================================")
    print(" ")
    
    list_to_process = fn_list_not_yet_processed(str_input_directory, str_out_directory, int_class)
    
    if len(list_to_process) > 0:
    
        if not os.path.exists(str_out_directory):
            os.mkdir(str_out_directory)
    
        list_pipeline_dict = []
        str_classification = "Classification[" + str(int_class) + ":" + str(int_class) + "]"
    
        for item in list_to_process:
            str_input_filepath_name = os.path.join(str_input_directory, item + ".laz")
            str_output_filepath_name = os.path.join(str_out_directory, item + '_class_' + str(int_class) + '.las')
    
            dict_pipeline_current_las = {
                    "pipeline": [
                        {   
                            "filename":str_input_filepath_name,
                            "type":"readers.las",
                            "tag":"readdata"
                        },
                        {   
                            "type":"filters.range",
                            "limits": str_classification,
                            "tag":"class_points"
                        },
                        {
                            "filename": str_output_filepath_name,
                            "inputs": [ "class_points" ],
                            "type": "writers.las"
                        }
                    ]}
    
            list_pipeline_dict.append(dict_pipeline_current_las)

        # kick off a multiprocessing las creation
        print('Begin multi-procressing....')
        print("+-----------------------------------------------------------------+")
        
        if int_cores == 0 or int_cores >= mp.cpu_count():
            p = mp.Pool(processes = (mp.cpu_count() - 1))
        else:
            p = mp.Pool(processes = int_cores)
            
        l = len(list_to_process)
        
        list_return_values = list(tqdm.tqdm(p.imap(fn_create_las, list_pipeline_dict),
                                            total = l,
                                            desc='Extract Points',
                                            bar_format = "{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
                                            ncols=65))
        
        p.close()
        p.join()
        
            
    else:
        print('No laz files to process')
        
    # -----------------------------------------
    flt_end_extract_classification = time.time()
    flt_time_extract_classification = (flt_end_extract_classification - flt_start_extract_classification) // 1
    time_pass_extract_classification = datetime.timedelta(seconds=flt_time_extract_classification)
    
    print('Total Compute Time: ' + str(time_pass_extract_classification))

# --------------------------------------------------------   

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


if __name__ == '__main__':

    
    parser = argparse.ArgumentParser(description='===== EXTRACT LAZ POINTS BY CLASSIFICATION FROM DIRECTORY =====')


    parser.add_argument('-i',
                        dest = "str_input_directory",
                        help=r'REQUIRED: path to directory containing LAZ: Example: D:\entwine_build_test_20230124\30097-C6\stratmap-2021-28cm-50cm-bexar-travis\lpc',
                        required=True,
                        metavar='DIR',
                        type=str)

    parser.add_argument('-o',
                        dest = "str_out_directory",
                        help=r'REQUIRED: path to write all the outputs: Example D:\test\stratmap-2021-28cm-50cm-bexar-travis',
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
    
    parser.add_argument('-n',
                        dest = "int_cores",
                        help='OPTIONAL: number of cores: Default=0 (will deploy all cores, less one for overhead)',
                        required=False,
                        default=0,
                        metavar='INTEGER',
                        type=int)

    args = vars(parser.parse_args())
    
    str_input_directory = args['str_input_directory']
    str_out_directory = args['str_out_directory']
    int_class = args['int_class']
    int_cores = args['int_cores']
    
    fn_extract_classification(str_input_directory,
                              str_out_directory,
                              int_class,
                              int_cores)
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~