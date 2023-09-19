# Create a KML of the bridge data
#
# Created by: Andy Carter, PE
# Created - 2023.07.28
# Last revised - 2023.09.19
#
# tx-bridge - sub-process of the 8th processing script
# ************************************************************

# ************************************************************
import argparse

import simplekml
from simplekml import Kml

import geopandas as gpd
import os

from shapely.geometry import Polygon, MultiPolygon

import time
import datetime
from datetime import datetime

import json
import configparser

import warnings
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


# ........................................
def fn_generate_kml(str_input_dir,str_input_json,dict_global_config_data):
    
    # Need to supress warnings
    warnings.simplefilter(action='ignore', category=Warning)
    
    print('Computing KML file...')
    # minimum complation score to be considered a 'good match'
    flt_min_conflation_score = 0.65
    
    # parse the run's configuration file
    with open(str_input_json) as f:
        json_run_data = json.load(f)
    
    
    # variables from run configuration json
    str_input_shp_path_arg = json_run_data["str_aoi_shp_filepath"]
    str_aoi_name = json_run_data["str_aoi_name"]
    str_cog_dem_path = json_run_data["str_cog_dem_path"] 
    
    str_input_copc_file = json_run_data["copc_point_cloud"]["copc_filepath"]
    int_class = json_run_data["copc_point_cloud"]["copc_class"]
    str_copc_name = json_run_data["copc_point_cloud"]["copc_name"]
    str_copc_short_name = json_run_data["copc_point_cloud"]["copc_short_name"]
    int_copc_date = json_run_data["copc_point_cloud"]["copc_date"]
        
    # todays date as string
    dt_today = datetime.today()
    str_today = dt_today.strftime('%Y%m%d')
    
    # variables from the global ini
    str_marker_green_filepath = dict_global_config_data['global_input_files']['str_marker_green_filepath']
    str_marker_yellow_filepath = dict_global_config_data['global_input_files']['str_marker_yellow_filepath']
    str_marker_white_filepath = dict_global_config_data['global_input_files']['str_marker_white_filepath']
    str_marker_purple_filepath = dict_global_config_data['global_input_files']['str_marker_purple_filepath']
    
    
    # create file names
    str_kml_name = str_aoi_name + '_' + str_copc_short_name + '_' + str_today
    str_kmz_name = str_kml_name + '.kmz'
    
    #
    str_root_folder = str_input_dir
    str_aoi = os.path.join('00_input_shapefile','input_polygon_ar.shp')
    
    # ---- create paths to needed files
    str_aoi_path = os.path.join(str_root_folder, str_aoi)
    str_mjr_axis_path = os.path.join(str_root_folder,'08_cross_sections','08_08_mjr_axis_xs_w_feature_id_nbi_low_hull_rating.gpkg')
    str_stream_path = os.path.join(str_root_folder, '08_cross_sections','08_03_nwm_streams.geojson' )
    str_stream_segments = os.path.join(str_root_folder, '08_cross_sections','08_09_stream_segements.geojson')
    str_hull_file = 'class_' + str(int_class) + '_ar_3857.gpkg'
    str_bridge_hull_path = os.path.join(str_root_folder,'02_shapefile_of_hulls', str_hull_file)
    str_image_path_header = os.path.join(str_root_folder, '08_cross_sections','08_10_cross_section_plots')

    str_output = os.path.join(str_root_folder, '08_cross_sections')
    
    # ---- read in input files
    gdf_aoi_ar = gpd.read_file(str_aoi_path)
    gdf_mjr_axis_ln = gpd.read_file(str_mjr_axis_path)
    gdf_stream_ln = gpd.read_file(str_stream_path)
    gdf_stream_segments_ln = gpd.read_file(str_stream_segments)
    gdf_hull_ar = gpd.read_file(str_bridge_hull_path)
    
    # define the "wgs" espg
    wgs = "epsg:4326"
    
    flt_max_snap_stream = 200
    
    # --- create the kml
    kml = simplekml.Kml()
    kml = Kml(name=str_kml_name)
    
    str_output = os.path.join(str_output, str_kmz_name)
    
    #Description in the information folder
    str_descritpion = "<b>Area of Interest Name</b>: " + str_aoi_name + '<br>'
    str_descritpion += "<b>Bare Earth terrain path</b>: " + str_cog_dem_path+ '<br>'
    str_descritpion += "<b>Bridge COPC path</b>: " + str_input_copc_file+ '<br>'
    str_descritpion += "<b>COPC point classification</b>: " + str(int_class)+ '<br>'
    str_descritpion += "<b>COPC name</b>: " + str_copc_name+ '<br>'
    str_descritpion += "<b>COPC short name</b>: " + str_copc_short_name + '<br>'
    str_descritpion += "<b>COPC date flown</b>: " + str(int_copc_date) + '<br>'
    str_descritpion += "<br><b>Date KML Created</b>: " + str_today + '<br>'
    
    fol = kml.newfolder(name='00_information',description=str_descritpion)
    
    # 01 ---- load the area of interest
    gdf_aoi_ar_wgs = gdf_aoi_ar.to_crs(wgs)
    
    fol = kml.newfolder(name='01_area_of_interest')
    
    for index, row in gdf_aoi_ar_wgs.iterrows():
        shape_geom = row['geometry']
        list_coords = list(shape_geom.exterior.coords)
        # append the first points as a new last point to close
        list_coords.append(list_coords[0])
        
        pol = fol.newpolygon(name=str(index))
        
        pol.outerboundaryis = list_coords
        
        pol.style.linestyle.color = simplekml.Color.black
        pol.style.linestyle.width = 6.0
        pol.style.polystyle.color = simplekml.Color.changealphaint(130, simplekml.Color.green)
        
    # 02 ---- load the stream lines
    gdf_stream_ln_wgs = gdf_stream_ln.to_crs(wgs)
    
    fol = kml.newfolder(name='02_stream_lines')
    
    for index, row in gdf_stream_ln_wgs.iterrows():
        geom = row['geometry']
        if geom.geom_type == "LineString":
            geom = row['geometry'].coords[:]
            ls = fol.newlinestring(name=str(index), coords=geom)
    
            ls.style.linestyle.width = 1
            ls.style.linestyle.color = simplekml.Color.cyan
    
            ls.description =  '<b>NWM COMID: </b>' + str(row['feature_id'])
            
    # 03 ---- load the stream segments
    gdf_stream_segments_ln_wgs = gdf_stream_segments_ln.to_crs(wgs)
    
    fol = kml.newfolder(name='03_stream_segments')
    
    for index, row in gdf_stream_segments_ln_wgs.iterrows():
        geom = row['geometry']
        if geom.geom_type == "LineString":
            geom = row['geometry'].coords[:]
            ls = fol.newlinestring(name=str(index), coords=geom)
    
            ls.style.linestyle.width = 1
            ls.style.linestyle.color = simplekml.Color.coral
    
            ls.description =  '<b>Stream Segment (FATSGTID): </b>' + str(row['HydroID'])
    
    # 04 ---- load the bridge hull polygons
    gdf_hull_ar_wgs = gdf_hull_ar.to_crs(wgs)
    
    fol = kml.newfolder(name='04_bridge hulls')
    
    for index, row in gdf_hull_ar_wgs.iterrows():
        shape_geom = row['geometry']
        
        if isinstance(shape_geom, Polygon):
            list_coords = list(shape_geom.exterior.coords)
        elif isinstance(shape_geom, MultiPolygon):
            shp_largest_polygon = max(shape_geom.geoms, key=lambda polygon: polygon.area)
            list_coords = list(shp_largest_polygon.exterior.coords)
            
        # append the first points as a new last point to close
        list_coords.append(list_coords[0])
        
        pol = fol.newpolygon(name=str(index))
        
        pol.outerboundaryis = list_coords
        
        pol.style.linestyle.color = simplekml.Color.white
        pol.style.linestyle.width = 2.0
        pol.style.polystyle.color = simplekml.Color.changealphaint(130, simplekml.Color.green)
        
    # 05 ---- load the bridge hull polygons
    # load the bridge hull polygons
    gdf_mjr_axis_ln_wgs = gdf_mjr_axis_ln.to_crs(wgs)
    
    fol = kml.newfolder(name='05_bridge_lines')
    
    for index, row in gdf_mjr_axis_ln_wgs.iterrows():
        geom = row['geometry'].coords[:]
        ls = fol.newlinestring(name=str(index), coords=geom)
        
        ls.style.linestyle.width = 4
        ls.style.linestyle.color = simplekml.Color.red
        
        ls.description =  '<b>Name: </b>' + str(row['name'])
        
    # --- add marker icon images to kml ---
    icon_path_green = kml.addfile(str_marker_green_filepath)
    icon_path_yellow = kml.addfile(str_marker_yellow_filepath)
    icon_path_white = kml.addfile(str_marker_white_filepath)
    icon_path_purple = kml.addfile(str_marker_purple_filepath)
    
    # create Geodataframe for a point on each line
    gdf_pt = gdf_mjr_axis_ln_wgs.copy()
    
    # geopandas point on of mjr axis lines - midpoint
    gdf_pt['geometry'] = gdf_mjr_axis_ln_wgs.geometry.interpolate(0.5, normalized = True)
    
    fol_bridge_points = kml.newfolder(name='06_bridge_points_for_nwm_prediction')
    fol_bridge_points_low_nbi = kml.newfolder(name='07_bridge_points_low_nbi_score')
    fol_bridge_points_no_stream = kml.newfolder(name='08_bridge_points_no_nwm_stream')
    fol_bridge_points_no_bridge = kml.newfolder(name='09_bridge_points_no_bridge')
    
    # 06 ---- bridge points with NWM predection capabilities
    for index, row in gdf_pt.iterrows():
        
        #str_image_path = str_image_path_header + str(row['uuid']) + '.png'
        str_image_path = os.path.join(str_image_path_header, str(row['uuid']) + '.png')
        
        if os.path.exists(str_image_path):
            path = kml.addfile(str_image_path)
        else:
            print('Cross Section Image not found: ' + path)
            
        geom = row['geometry'].coords[:]
        
        if float(row['convey_ar']) < 1.0:
            pnt = fol_bridge_points_no_bridge.newpoint(coords= geom)
            pnt.style.iconstyle.icon.href = icon_path_purple
            pnt.style.iconstyle.scale = 0.45
        else:
            if row['dist_river'] > flt_max_snap_stream:
                # bridge is too far away from NHD strream
                pnt = fol_bridge_points_no_stream.newpoint(coords= geom)
                pnt.style.iconstyle.icon.href = icon_path_white
                pnt.style.iconstyle.scale = 0.75
                pass
            else:
                if row['score'] < flt_min_conflation_score:
                    # bridge has low NBI score
                    pnt = fol_bridge_points_low_nbi.newpoint(coords= geom)
                    pnt.style.iconstyle.icon.href = icon_path_yellow
                    pnt.style.iconstyle.scale = 0.75
                else:
                    pnt = fol_bridge_points.newpoint(coords= geom)
                    pnt.style.iconstyle.icon.href = icon_path_green
        
        if os.path.exists(str_image_path):
            pnt.description = '<img src="' + path +'" alt="picture" width="1043" height="615" align="center" />'                    
        
    # ----- 
    print('Writing out KML...')
    kml.savekmz(str_output, format=False)  # Saving as KMZ
# ........................................

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='================== CREATE KML FOR BRIDGE DATA ==================')
    
    parser.add_argument('-i',
                        dest = "str_input_dir",
                        help=r'REQUIRED: input directory of processed data for area of interest [contains 00 to 08 folders]: Example: C:\bridge_data\folder_location',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-l',
                        dest = "str_input_json",
                        help=r'REQUIRED: path to the input coniguration json Example: D:\bridge_local_test\config.json',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))
    
    parser.add_argument('-g',
                        dest = "str_global_config_ini_path",
                        help=r'OPTIONAL: directory to national water model input flowlines: Example: G:\X-NWS\X-National_Datasets\nwm_flows.gpkg',
                        required=False,
                        default=r'C:\Users\civil\dev\tx-bridge\src\config_global.ini',
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))
    

    args = vars(parser.parse_args())
    
    str_input_dir = args['str_input_dir']
    str_input_json = args['str_input_json']
    str_global_config_ini_path = args['str_global_config_ini_path']

    # convert the INI to a dictionary
    dict_global_config_data = json.loads(fn_json_from_ini(str_global_config_ini_path))


    print(" ")
    print("+=================================================================+")
    print("|                  CREATE KML FOR BRIDGE DATA                     |")  
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) PATH TO INPUT FOLDERS: " + str_input_dir)
    print("  ---(l) JSON CONFIG FOR RUN FILEPATH: " + str_input_json)
    print("  ---[g]   Optional: GLOBAL INI FILEPATH: " + str_global_config_ini_path )
    print("===================================================================")

    fn_generate_kml(str_input_dir,str_input_json,dict_global_config_data)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    #time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    #print('Compute Time: ' + str(time_pass))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~