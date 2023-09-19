# From the '08_08_mjr_axis_xs_w_feature_id_nbi_low_hull_rating.gpkg' 
# Plot cross sections of each of the bridges.
#
# Created by: Andy Carter, PE
# Created - 2022.11.18
# Last revised - 2022.12.06
#
# tx-bridge - sub-process of the 8th processing script
# Uses the 'pdal' conda environment
# ************************************************************


# ************************************************************
import argparse

import geopandas as gpd
import pandas as pd
import ast # converting sting of list to list
import difflib # compare two string and score

import matplotlib.pyplot as plt
import matplotlib.ticker as tick

import multiprocessing as mp
from multiprocessing import Pool

import os
import tqdm

import time
import datetime
from datetime import date
# ************************************************************


# ----------------------------------------------  
# Print iterations progress
def fn_print_progress_bar (iteration,
                           total,
                           prefix = '', suffix = '',
                           decimals = 0,
                           length = 100, fill = '█',
                           printEnd = "\r"):
    """
    from: https://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()
# ----------------------------------------------  


# **********************************************
def fn_clean_envelopes(gdf_mjr_axis_envelopes):
    # removes rows where number of ordinates in the 
    # sta, ground_elv, deck_elev and low_ch_elv are not
    # the same length - bad processing
    
    list_invalid_index = []
    
    for index, row in gdf_mjr_axis_envelopes.iterrows():
        int_sta = len(ast.literal_eval(gdf_mjr_axis_envelopes.iloc[index]['sta']))
        int_ground = len(ast.literal_eval(gdf_mjr_axis_envelopes.iloc[index]['ground_elv']))
        int_deck = len(ast.literal_eval(gdf_mjr_axis_envelopes.iloc[index]['deck_elev']))
        int_low_ch_elv = len(ast.literal_eval(gdf_mjr_axis_envelopes.iloc[index]['low_ch_elv']))
        
        if (int_sta + int_ground + int_deck + int_low_ch_elv)/4 != int_sta:
            list_invalid_index.append(index)
            
    if len(list_invalid_index) > 0:
        gdf_mjr_axis_envelopes = gdf_mjr_axis_envelopes.drop(list_invalid_index)
    
    return(gdf_mjr_axis_envelopes)
# **********************************************


# ----------------------------------------------
def fn_plot_single_xs(gdf_singlerow):

    # plot a single cross section (one row in provided geodataframe)
    
    # *****
    # constants
    b_is_feet = True # vertical units are in feet
    int_padding = 1 # vertical padding below the lowest elevation when plotting

    flt_comid_snap_dist = 200 # hard coded distance for valid COMID
    flt_min_conveyance_area = 1.0 # minimum allowable conveyance area
    
    str_no_nbi_label = 'None Found' # hard coded value to plot when no nbi found
    str_no_comid = 'None within distance'
    
    # get variables from the provided row
    str_bridge_thickness = gdf_singlerow.iloc[0]['nbi_thick']
    str_nbi_asset = gdf_singlerow.iloc[0]['nbi_asset']

    # determine if there is a valid comid
    b_valid_comid = False
    str_comid_dist =  gdf_singlerow.iloc[0]['dist_river']
    if str_comid_dist != '':
        flt_comid_dist = float(str_comid_dist)
        # if stream close enough set as True
        if flt_comid_dist <= flt_comid_snap_dist:
            b_valid_comid = True

    # was an nbi found
    b_valid_nbi_thickness = False
    if str_bridge_thickness == '':
        pass
    else:
        b_valid_nbi_thickness = True

    # convert strings back to lists
    list_station = ast.literal_eval(gdf_singlerow.iloc[0]['sta'])
    list_ground_elv = ast.literal_eval(gdf_singlerow.iloc[0]['ground_elv'])
    list_deck_elev = ast.literal_eval(gdf_singlerow.iloc[0]['deck_elev'])
    list_low_chord = ast.literal_eval(gdf_singlerow.iloc[0]['low_ch_elv'])

    flt_conveyance_area = float(gdf_singlerow.iloc[0]['convey_ar'])

    # create a pandas dataframe of lists
    df_bridge = pd.DataFrame(list(zip(list_station,
                                      list_ground_elv,
                                      list_deck_elev,
                                      list_low_chord)),columns =['sta', 'ground_elv', 'deck_elev', 'low_chord'])

    df_bridge["max_ground_low_chord"] = df_bridge[["ground_elv", "low_chord"]].max(axis=1)

    # for now - using the average gound elevation as the water surface
    df_bridge["wsel"] = sum(list_ground_elv) / len(list_ground_elv)
    df_bridge["max_wsel_ground"] = df_bridge[["ground_elv", "wsel"]].max(axis=1)

    # create lists of max_wsel_ground and max_ground_low_chord
    list_max_wsel_ground = df_bridge['max_wsel_ground'].tolist()
    list_max_ground_low_chord = df_bridge['max_ground_low_chord'].tolist()

    # ------- lat / Long -------
    str_lon = str(gdf_singlerow.iloc[0]['longitude'])
    str_lat = str(gdf_singlerow.iloc[0]['latitude'])
    str_coords = '(' + str_lat + ',' + str_lon + ')'

    # -------Get the title text -----
    str_nhd_name = str(gdf_singlerow.iloc[0]['nhd_name'])
    str_road_name = str(gdf_singlerow.iloc[0]['name'])
    str_road_ref_name = str(gdf_singlerow.iloc[0]['ref'])

    b_have_road_name = False
    b_have_ref_name = False

    str_title_label = ''
    
    # --- converted strings may be 'nan'
    if str_road_name == 'nan':
        str_road_name = None
        
    if str_road_ref_name == 'nan':
        str_road_ref_name = None
        
    if str_nhd_name == 'nan':
        str_nhd_name = None
    # --- converted strings may be 'nan'

    if str_road_name != None:
        str_title_label = str_road_name
        b_have_road_name = True

    if str_road_ref_name != None:
        b_have_ref_name = True
        if b_have_road_name:
            # have a road name and a reference name
            # check to see how similar the two strings are
            seq=difflib.SequenceMatcher(a=str_road_name, b=str_road_ref_name)
            flt_name_match_score = seq.ratio()
            if flt_name_match_score < 0.9:
                # name and ref are different enough
                str_title_label += ' (' + str_road_ref_name + ')'
        else:
            str_title_label = str_road_ref_name

    if str_nhd_name != None:
        if str_nhd_name[:2] != '99':
            if b_have_road_name or b_have_ref_name:
                str_title_label += ' @ ' + str_nhd_name
            else:
                str_title_label = str_nhd_name


    # ---- Labels for the plot ----
    fig = plt.figure(figsize=(8,4), dpi = 300)
    fig.patch.set_facecolor('gainsboro')

    fig.suptitle(str_title_label, fontsize=14, fontweight='bold')

    ax = plt.gca()
    today = date.today()

    # positions of the first text (lower right)
    flt_y_location = 0.04
    flt_y_delta = 0.03
    int_font_size = 4

    # ------
    # date created label
    ax.text(0.98, flt_y_location, 'Created: ' + str(today),
                verticalalignment='bottom',
                horizontalalignment='right',
                backgroundcolor='w',
                transform=ax.transAxes,
                fontsize=int_font_size,
                style='italic')
    flt_y_location += flt_y_delta
    # ------

    # ------
    # created by label
    ax.text(0.98, flt_y_location, 'Created by: University of Texas',
            verticalalignment='bottom',
            horizontalalignment='right',
            backgroundcolor='w',
            transform=ax.transAxes, fontsize=int_font_size, style='italic')
    flt_y_location += flt_y_delta
    # ------

    # ------
    # location lat/long label
    ax.text(0.98, flt_y_location, 'Lat/Long: '+ str_coords,
            verticalalignment='bottom',
            horizontalalignment='right',
            backgroundcolor='w',
            transform=ax.transAxes, fontsize=int_font_size, style='italic')
    flt_y_location += flt_y_delta
    # ------

    # ------
    # location COMID b_valid_comid
    if b_valid_comid:
        str_comid = str(gdf_singlerow.iloc[0]['feature_id'])
        str_color = 'k'
    else:
        str_comid = str_no_comid
        str_color = 'r'

    ax.text(0.98, flt_y_location, 'NWM COMID: '+ str_comid,
            verticalalignment='bottom',
            horizontalalignment='right',
            backgroundcolor='w',
            color=str_color,
            transform=ax.transAxes, fontsize=int_font_size, style='italic')
    flt_y_location += flt_y_delta
    # ------

    # ------
    # deck thickness label
    if b_valid_nbi_thickness:
        str_color = 'k'
        str_thickness = str(gdf_singlerow.iloc[0]['nbi_thick'])
    else:
        str_color = 'r'
        str_thickness = 'None - Default Value'

    ax.text(0.98, flt_y_location, 'Est. deck thickness: '+ str_thickness,
            verticalalignment='bottom',
            horizontalalignment='right',
            color=str_color,
            backgroundcolor='w',
            transform=ax.transAxes, fontsize=int_font_size, style='italic')
    flt_y_location += flt_y_delta
    # ------

    # ------
    if str_nbi_asset != '':
        str_color = 'k'
    else:
        str_color = 'r'
        str_nbi_asset = str_no_nbi_label

    # nbi asset number label
    ax.text(0.98, flt_y_location, 'NBI: '+ str_nbi_asset,
            verticalalignment='bottom',
            horizontalalignment='right',
            backgroundcolor='w',
            color=str_color,
            transform=ax.transAxes, fontsize=int_font_size, style='italic')
    flt_y_location += flt_y_delta


    # ------

    # ---- Labels for the plot ----
    if b_valid_comid:
        # create WSEL line if there is a valid COMID
        if flt_conveyance_area > flt_min_conveyance_area:
            # and the conyenace area under the low chord is greater than zero
            plt.plot(list_station, list_max_wsel_ground, label = "wsel", linewidth=2, linestyle="-", c="b")  # creates the line

    if b_valid_nbi_thickness:
        plt.plot(list_station, list_max_ground_low_chord, label = "low_chord", linewidth=1, linestyle="-", c="k")  # creates the line
    else:
        plt.plot(list_station, list_max_ground_low_chord, label = "low_chord", linewidth=1, linestyle="-", c="r")  # creates the line

    plt.plot(list_station, list_deck_elev, label = "high_chord", linewidth=1, linestyle="-", c="k")  # creates the line
    plt.plot(list_station, list_ground_elv, label = "ground", linewidth=2, linestyle="-", c="k")  # creates the line

    # bridge deck fill
    if b_valid_nbi_thickness:
        plt.fill_between(list_station, list_deck_elev, list_max_ground_low_chord, color='grey', alpha=0.4)
    else:
        plt.fill_between(list_station, list_deck_elev, list_max_ground_low_chord, color='red', alpha=0.4)

    if b_valid_comid:
        # wsel fill if there is a valid COMID
        if flt_conveyance_area > 1:
            plt.fill_between(list_station, list_max_wsel_ground, list_ground_elv, color='cyan', alpha=0.25)

    if b_is_feet:
        plt.ylabel('Elevation (ft)')
        plt.xlabel('Station (ft)')
    else:
        plt.ylabel('Elevation (m)')
        plt.xlabel('Station (m)')

    # --- hatching areas below gound profile
    list_lowest = [min(list_ground_elv) - int_padding ] * len(list_ground_elv)
    plt.fill_between(list_station, list_ground_elv, list_lowest, color='saddlebrown', alpha=0.20)

    plt.grid(True)


    # Save the plot
    str_xs_file_name = gdf_singlerow.iloc[0]['uuid'] + '.png'
    
    
    str_xs_folder = gdf_singlerow.iloc[0]['xs_folder']
    str_xs_plot_filepath = os.path.join(str_xs_folder, str_xs_file_name)

    plt.savefig(str_xs_plot_filepath,bbox_inches="tight")

    plt.cla()
    plt.close('all')
    
    return(1)
# ----------------------------------------------


# ..................................................
def fn_process_all_cross_sections(list_input_files, str_flow_csv_filename):
    

    str_majr_axis_filename = list_input_files[0]
    str_input_dir = list_input_files[1]
    
    # create a sub-folder to store the cross sections
    str_xs_folder = os.path.join(str_input_dir, '08_cross_sections', '08_10_cross_section_plots')
    
    if not os.path.exists(str_xs_folder):
        os.makedirs(str_xs_folder, exist_ok=True)
        
    gdf_mjr_axis_envelopes = gpd.read_file(str_majr_axis_filename)
    
    
    # drop bridge envelopes from plotting where the length of ordered pairs
    # of the sta, ground, deck and low choord are not equal
    gdf_mjr_axis_envelopes = fn_clean_envelopes(gdf_mjr_axis_envelopes)
    
    # ----plotting cross sections ----
    l = len(gdf_mjr_axis_envelopes)
    
    # create a list of geodataframes where each item contains one row
    gdf_mjr_axis_envelopes['xs_folder'] = str_xs_folder
        
    # create a list of geodataframes where each item contains one row
    list_of_single_row_gdfs = [gpd.GeoDataFrame([row], crs=gdf_mjr_axis_envelopes.crs) for _, row in gdf_mjr_axis_envelopes.iterrows()]
    
    num_processors = (mp.cpu_count() - 1)
    try:
        # problems on HPC - memory allocation
        # setting cores to 16 max
        if num_processors > 16:
            num_processors = 16
            
        pool = Pool(processes = num_processors)
        
        list_ints = list(tqdm.tqdm(pool.imap(fn_plot_single_xs, list_of_single_row_gdfs),
                                   total = l,
                                   desc='Plot XS',
                                   bar_format = "{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
                                   ncols=67 ))
    
        pool.close()
        pool.join()
        
    except OSError:
        # there is a memory allocation... run in a serial loop
        # 
        
        int_count = 0
        str_prefix = "Plotting XS " + str(int_count) + ' of ' + str(l)
        fn_print_progress_bar(0, l, prefix = str_prefix , suffix = 'Complete', length = 29)
        
        for gdf_single_row in list_of_single_row_gdfs:
            # -- update progress bar --
            time.sleep(0.02)
            int_count += 1
            str_prefix = "Plotting XS " + str(int_count) + ' of ' + str(l)
            fn_print_progress_bar(int_count, l, prefix = str_prefix , suffix = 'Complete', length = 29)
            
            int_return_val = fn_plot_single_xs(gdf_single_row)
    # --------------
    
# ..................................................


# ----------------------------------------------------
def fn_plot_cross_sections(str_input_dir, str_majr_axis_filename, str_flow_csv_filename):
    
    # --- build file paths to the required input folders ---
    str_major_axis_lines = os.path.join(str_input_dir, '08_cross_sections', str_majr_axis_filename)
    
    list_input_files = [str_major_axis_lines]
    
    # --- check to see if all the required input files exist ---
    list_files_exist = []
    for str_file in list_input_files:
        list_files_exist.append(os.path.isfile(str_file))
    
    if all(list_files_exist):
        # all input files were found
        list_input_files.append(str_input_dir)
        
        fn_process_all_cross_sections(list_input_files, str_flow_csv_filename)
    else:
        int_item = 0
        for item in list_files_exist:
            if not item:
                print(" ERROR: Input file not found: " + list_input_files[int_item])
            int_item += 1

# ----------------------------------------------------


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='============= PLOT MAJOR AXIS LINE CROSS SECTIONS =================')
    
    parser.add_argument('-i',
                        dest = "str_input_dir",
                        help=r'REQUIRED: input directory of processed data for area of interest [contains 00 to 08 folders]: Example: C:\bridge_data\folder_location',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-n',
                        dest = "str_majr_axis_filename",
                        help=r'OPTIONAL: name of the major axis file name: Example: 08_08_mjr_axis_xs_w_feature_id_nbi_low_hull_rating.gpkg',
                        required=False,
                        default=r'08_08_mjr_axis_xs_w_feature_id_nbi_low_hull_rating.gpkg',
                        metavar='STRING',
                        type=str)
    
    parser.add_argument('-w',
                    dest = "str_flow_csv_filename",
                    help=r'OPTIONAL: flow determined for each segment: Example: C:\bridge_data\folder_location\module1_stage_flow_15Oct2022_4pm_predicted_peak_flow.csv',
                    required=False,
                    default=r'NONE',
                    metavar='STRING',
                    type=str)
    
    args = vars(parser.parse_args())
    
    str_input_dir = args['str_input_dir']
    str_majr_axis_filename = args['str_majr_axis_filename']
    str_flow_csv_filename = args['str_flow_csv_filename']

    print(" ")
    print("+=================================================================+")
    print("|             PLOT MAJOR AXIS LINE CROSS SECTIONS                 |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) PATH TO INPUT FOLDERS: " + str_input_dir)
    print("  ---[n]   Optional: MAJOR AXIS FILE NAME: " + str_majr_axis_filename )
    print("  ---[w]   Optional: FLOW PER SEGMENT FILE: " + str_flow_csv_filename )
    print("===================================================================")

    # TODO - check to see if str_flow_csv_filename exists
    
    fn_plot_cross_sections(str_input_dir,str_majr_axis_filename,str_flow_csv_filename)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~