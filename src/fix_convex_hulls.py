# Fix the 'fat' convex hulls.
# For curved and irregular bridges, the convex hull is typically an
# overestimation of the plan view footprint of the bridge hull.
# This script determines what polygons need to be adjusted based on a
# approximate hexbin filter in pdal.  If the convex hull area is bigger
# than the hexbin hull area, then it is likely that the hull needs to be
# adjusted.  If the percent difference between the point in the convex and
# hexbin hulls is low then the hull is adjusted.
#
# Created by: Andy Carter, PE
# Created - 2023.08.01
# Last revised - 2023.08.31
#
# tx-bridge environment - sub-process of the 2nd processing script
# ************************************************************

# TODO - 2023.08.29 - Memory Allocation issues - pipeline2

# ************************************************************
import argparse
import configparser

import geopandas as gpd
import pdal
import json
import shapely.wkt

from shapely.geometry import MultiPolygon, Polygon
from shapely.wkt import loads

import os

import time
import datetime
from datetime import date

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


# -----------------
def fn_parse_geometry(wkt_string):
    geometry = loads(wkt_string)
    return geometry
# -----------------


# -----------------
def fn_find_largest_polygon(geometries):
    largest_polygon = None
    largest_area = 0

    for geometry in geometries:
        if isinstance(geometry, Polygon):
            area = geometry.area
            if area > largest_area:
                largest_polygon = geometry
                largest_area = area
        elif isinstance(geometry, MultiPolygon):
            for polygon in geometry.geoms:
                # added geometry.geoms due or deprication issue
                area = polygon.area
                if area > largest_area:
                    largest_polygon = polygon
                    largest_area = area

    return largest_polygon, largest_area
# -----------------


# -----------------
def fn_get_intersection(wkt_geometry1, wkt_geometry2):
    # Parse WKT strings into Shapely geometries
    geometry1 = loads(wkt_geometry1)
    geometry2 = loads(wkt_geometry2)

    # Calculate the intersection
    intersection = geometry1.intersection(geometry2)

    return intersection
# -----------------


# ~~~~~~~~~~~~~~~~~~~~~~
def fn_fix_convex_hulls(str_convex_hull_filepath,str_input_json,dict_global_config_data):
    
    # Need to supress warnings
    warnings.simplefilter(action='ignore', category=Warning)
    
    # read the input shapefile/geopackage
    gdf_bridge_poly = gpd.read_file(str_convex_hull_filepath)
    
    # ------------
    # create copy of the input geodataframe
    str_input_directory = os.path.dirname(str_convex_hull_filepath)
    
    # Get the file name with extension
    str_filename_w_ext = os.path.basename(str_convex_hull_filepath)
    str_filename_wo_ext, str_ext = os.path.splitext(str_filename_w_ext)
    
    str_copy_original = os.path.join(str_input_directory, str_filename_wo_ext + '_convex' + str_ext)
    
    # save a copy of the original
    gdf_bridge_poly.to_file(str_copy_original)
    # ------------
    
    print('Extracting point clouds per hull...')

    # parse the run's configuration file
    with open(str_input_json) as f:
        json_run_data = json.load(f)
        
    # variable from run configuration json
    str_input_copc_file = json_run_data["copc_point_cloud"]["copc_filepath"]

    # variables from the global ini
    flt_hex_edge_size = float(dict_global_config_data['02_polygonize_clusters']['flt_hex_edge_size'])
    int_hex_threshold = int(dict_global_config_data['02_polygonize_clusters']['int_hex_threshold'])
    flt_max_point_pct_diff = float(dict_global_config_data['02_polygonize_clusters']['flt_max_point_pct_diff'])
    
    # initialize lists
    list_int_points_convex_hull = []
    list_flt_area_convex_hull = []
    list_shp_hex_polygon = []
    list_int_points_hexbin = []
    list_flt_area_hexbin = []
    
    # Douglas–Peucker  simplification distance - function of flt_hex_edge_size
    flt_simplify_dist = flt_hex_edge_size * 3
    
    int_count = 0
    l = len(gdf_bridge_poly)

    # TODO - 20230801 - Error when l = 0

    str_prefix = "Hull " + str(int_count) + ' of ' + str(l)
    fn_print_progress_bar(0, l, prefix = str_prefix , suffix = 'Complete', length = 29)

    for index, row in gdf_bridge_poly.iterrows():
        int_count += 1
        str_prefix = "Hull " + str(int_count) + ' of ' + str(l)
        fn_print_progress_bar(int_count, l, prefix = str_prefix , suffix = 'Complete', length = 29)
        
        str_geom_wkt = gdf_bridge_poly['geometry'][index].wkt
        shp_convexhull = shapely.wkt.loads(str_geom_wkt)
        list_flt_area_convex_hull.append(shp_convexhull.area) 
        
        pipeline_class_las = {
            "pipeline": [
                {   
                    'polygon':str_geom_wkt,
                    "filename":str_input_copc_file,
                    "type":"readers.copc",
                    "tag":"readdata"
                },
                {
                    'inputs': [ "readdata" ],
                    'edge_size': flt_hex_edge_size,
                    'threshold': int_hex_threshold,
                    'preserve_topology': True,
                    'smooth': False,
                    "type":"filters.hexbin",
                    "tag":"hex_boundary"
                }
            ]}
    
        #execute the pdal pipeline
        pipeline = pdal.Pipeline(json.dumps(pipeline_class_las))
        n_points = pipeline.execute()
        
        dict_meta = pipeline.metadata
        wkt_hex_boundary = dict_meta["metadata"]['filters.hexbin']['boundary']
    
        # close the pipeline
        #del pipeline
        
        # Parse the geometries from WKT strings
        shp_geometries = [fn_parse_geometry(wkt_hex_boundary)]
    
        # Find the largest polygon and its area
        shp_largest_polygon, flt_largest_area = fn_find_largest_polygon(shp_geometries)
        
        # Create a new polygon using the exterior ring
        shp_largest_polygon_without_holes = Polygon(shp_largest_polygon.exterior)
        
        list_shp_hex_polygon.append(shp_largest_polygon_without_holes)
        list_int_points_convex_hull.append(n_points)
        
        list_flt_area_hexbin.append(shp_largest_polygon_without_holes.area)
        
        pipeline_inhexbin_footprint_las = {
            "pipeline": [
                    {   
                        'polygon': shp_largest_polygon_without_holes.wkt,
                        "filename": str_input_copc_file,
                        "type": "readers.copc"
                    }
            ]}
    
        #execute the pdal pipeline
        pipeline2 = pdal.Pipeline(json.dumps(pipeline_inhexbin_footprint_las))
        n_points_2 = pipeline2.execute()
        
        #print(type(n_points_2))

        list_int_points_hexbin.append(n_points_2)
        
        del pipeline2
        
    # create a geodataframe of the hex filter boundaries
    gdf_hexboundary = gpd.GeoDataFrame(geometry=list_shp_hex_polygon, crs=gdf_bridge_poly.crs)
    
    # add lists to geodataframe
    gdf_hexboundary['pnt_count_convexhull'] = list_int_points_convex_hull
    gdf_hexboundary['convex_hull_area'] = list_flt_area_convex_hull
    gdf_hexboundary['pnt_count_hex_bin'] = list_int_points_hexbin
    gdf_hexboundary['hexbin_hull_area'] = list_flt_area_hexbin
    
    # compute gdf columns to determine what needs to be 'fixed'
    gdf_hexboundary['convex_is_larger'] = gdf_hexboundary.apply(lambda row: True if row['convex_hull_area'] > row['hexbin_hull_area'] else False, axis=1)
    gdf_hexboundary['pct_difference'] = (abs((gdf_hexboundary['pnt_count_hex_bin'] - gdf_hexboundary['pnt_count_convexhull'])/gdf_hexboundary['pnt_count_convexhull'])*100)
    
    # select polygons where the convex hull is bigger than the hexbin polygon
    gdf_problem_bridge = gdf_hexboundary.loc[gdf_hexboundary['convex_is_larger']==True]
    
    # simplify the hex boundary with Douglas–Peucker 
    gdf_problem_bridge['geometry'] = gdf_problem_bridge['geometry'].simplify(tolerance=flt_simplify_dist)

    # select only those hulls where the percent difference between convex and hexbin hulls are less than threshold
    gdf_problem_bridge2 = gdf_problem_bridge.loc[gdf_problem_bridge['pct_difference'] < flt_max_point_pct_diff]
    
    # flag all the problem bridges where (1) the convex hull is larger that the hexbin
    # and (2) the percent difference between the point count in the convex hull and hexbin hull is high
    # -- these are likely the irregular bridges that will need manual hull edits
    gdf_problem_bridge3 = gdf_problem_bridge.loc[gdf_problem_bridge['pct_difference'] > flt_max_point_pct_diff]
    
    # create copy of the input geodataframe
    gdf_bridge_poly_revised = gdf_bridge_poly
    
    # set the default value for coloumn
    gdf_bridge_poly_revised['adj_hull'] = False
    gdf_bridge_poly_revised['bad_hull'] = False
    
    print("+-----------------------------------------------------------------+")
    int_count = 0
    l = len(gdf_problem_bridge2)
    print('Hulls needing adjustment: ' + str(l))

    if l > 0:
        str_prefix = "Adjusting " + str(int_count) + ' of ' + str(l)
        fn_print_progress_bar(0, l, prefix = str_prefix , suffix = 'Complete', length = 26)
        
        for index, row in gdf_problem_bridge2.iterrows():
            
            int_count += 1
            str_prefix = "Adjusting " + str(int_count) + ' of ' + str(l)
            fn_print_progress_bar(int_count, l, prefix = str_prefix , suffix = 'Complete', length = 26)
            
            wkt_simple_hex_hull = gdf_problem_bridge2.loc[index]['geometry'].wkt
            wkt_convex_hull = gdf_bridge_poly.loc[index]['geometry'].wkt
            
            # Get the intersection of the two polygons
            shp_intersection = fn_get_intersection(wkt_simple_hex_hull, wkt_convex_hull)
            
            # Set the 'geometry' and 'adjusted_hull' values for the current row in gdf_bridge_poly_revised
            gdf_bridge_poly_revised.loc[index, 'geometry'] = shp_intersection
            gdf_bridge_poly_revised.loc[index, 'adj_hull'] = True
        
        # overwrite the input convex hulls with revised hulls
        gdf_bridge_poly_revised.to_file(str_convex_hull_filepath)
    else:
        print('No hulls to adjust')
        
    # ---- Flaging the hulls that are too complex to sort out
    # --- these will likely need manual edits
    if len(gdf_problem_bridge3) > 0:
        for index, row in gdf_problem_bridge3.iterrows():
            gdf_bridge_poly_revised.loc[index, 'bad_hull'] = True
        
        # overwrite the input convex hulls with revised hulls
        gdf_bridge_poly_revised.to_file(str_convex_hull_filepath)
    # ---- 
    
    
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='==================== FIX CONVEX BRIDGE HULLS ===================')
    
    parser.add_argument('-i',
                        dest = "str_convex_hull_filepath",
                        help=r'REQUIRED: input file path of convex hull geopackage: Example: D:\output\class_13_ar_3857.gpkg',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))
    
    parser.add_argument('-l',
                        dest = "str_input_json",
                        help=r'REQUIRED: path to the input coniguration json Example: D:\bridge_local_test\config.json',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))
    
    parser.add_argument('-g',
                        dest = "str_global_config_ini_path",
                        help=r'OPTIONAL: directory to national water model input flowlines: Example: C:\Users\civil\dev\tx-bridge\src\config_global.ini',
                        required=False,
                        default=r'C:\Users\civil\dev\tx-bridge\src\config_global.ini',
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))
    
    args = vars(parser.parse_args())
    
    str_convex_hull_filepath = args['str_convex_hull_filepath']
    str_input_json = args['str_input_json']
    str_global_config_ini_path = args['str_global_config_ini_path']

    # convert the INI to a dictionary
    dict_global_config_data = json.loads(fn_json_from_ini(str_global_config_ini_path))

    print(" ")
    print("+=================================================================+") 
    print("|                   FIX CONVEX BRIDGE HULLS                       |")  
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) PATH TO INPUT CONVEX HULL FILE: " + str_convex_hull_filepath)
    print("  ---(l) JSON CONFIG FOR RUN FILEPATH: " + str_input_json)
    print("  ---[g]   Optional: GLOBAL INI FILEPATH: " + str_global_config_ini_path )
    print("===================================================================")

    fn_fix_convex_hulls(str_convex_hull_filepath,str_input_json,dict_global_config_data)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~