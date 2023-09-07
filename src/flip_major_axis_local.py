# Flip the bridge major axis to be 'left-to-right' looking downstream if
# a National Hydrography Dataset (NHD) stream line is available
#
# Created by: Andy Carter, PE
# Created - 2022.05.17
# Last revised - 2022.05.17
#
# tx-bridge - 06 - sixth processing script
# Uses the 'pdal' conda environment

# ************************************************************
import argparse
import geopandas as gpd
import shapely.geometry
from shapely.geometry import LineString
import math
import numpy as np
import os
import tqdm

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


# =========================================================
#Functions to determine if two lines cross
#https://www.kite.com/python/answers/how-to-check-if-two-line-segments-intersect-in-python

def fn_on_segment(p, q, r):
    if r[0] <= max(p[0], q[0]) and r[0] >= min(p[0], q[0]) and r[1] <= max(p[1], q[1]) and r[1] >= min(p[1], q[1]):
        return True
    return False

def fn_orientation(p, q, r):
    val = ((q[1] - p[1]) * (r[0] - q[0])) - ((q[0] - p[0]) * (r[1] - q[1]))
    if val == 0 : return 0
    return 1 if val > 0 else -1

def fn_intersects(seg1, seg2):
    p1, q1 = seg1
    p2, q2 = seg2
    
    o1 = fn_orientation(p1, q1, p2)
    o2 = fn_orientation(p1, q1, q2)
    o3 = fn_orientation(p2, q2, p1)
    o4 = fn_orientation(p2, q2, q1)
    
    if o1 != o2 and o3 != o4:
        return True
    
    if o1 == 0 and fn_on_segment(p1, q1, p2) : return True
    if o2 == 0 and fn_on_segment(p1, q1, q2) : return True
    if o3 == 0 and fn_on_segment(p2, q2, p1) : return True
    if o4 == 0 and fn_on_segment(p2, q2, q1) : return True
    return False 
# =========================================================


# ..........................................................
def fn_flip_major_axis(str_major_axis_ln_path,str_output_dir,flt_mjr_axis,str_nhd_stream_gpkg):
    
    """
    Determine the major axis line from the bridge hull polygons and the
    OpenStreetMap lines.

    """
    
    print(" ")
    print("+=================================================================+")
    print("|        FLIP MAJOR AXIS LINES (DOWNSTREAM LEFT-TO-RIGHT)         |")
    print("|    USING LOCAL NATIONAL HYDROGRAPHY DATASET STEAMLINE FILE      |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) INPUT MAJOR AXIS LINES: " + str_major_axis_ln_path)
    print("  ---(o) OUTPUT DIRECTORY: " + str_output_dir)
    print("  ---[b]   Optional: BUFFER DISTANCE MAJOR AXIS: " + str(flt_mjr_axis) )
    print("  ---[g]   Optional: NHD STREAMLINES: " + str_nhd_stream_gpkg)
    print("===================================================================")
    
    # create the output directory if it does not exist
    os.makedirs(str_output_dir, exist_ok=True)
    
    # read the bridge major axis lines
    gdf_mjr_axis_ln = gpd.read_file(str_major_axis_ln_path)
    
    # get the crs of the user supplied shapefile
    source_crs = gdf_mjr_axis_ln.crs
    
    # define the "wgs" espg - assumed NHD projection
    lambert = "epsg:3857"
    WGS = "epsg:4326"
    
    # get crs of the input shapefile
    str_crs_model = str(gdf_mjr_axis_ln.crs)
    
    # convert the input shapefile to WGS
    gdf_mjr_axis_ln_lambert = gdf_mjr_axis_ln.to_crs(lambert)
    
    # buffer the input polygons
    gdf_mjr_axis_ln_lambert['geometry'] = gdf_mjr_axis_ln_lambert.geometry.buffer(flt_mjr_axis)
    
    gdf_mjr_axis_buffer_ar_prj = gdf_mjr_axis_ln_lambert.to_crs(str_crs_model)
    
    list_nhd_names = []
    list_nhd_reachcode = []
    
    # -----
    # clip the global nhd streamlines to the area-of-interest
    
    # convert aoi to wgs
    gdf_mjr_axis_ar_wgs = gdf_mjr_axis_ln_lambert.to_crs(WGS)
    
    # get geoDataFrame of the boundary of the input shapefile
    gdf_bounds = gdf_mjr_axis_ar_wgs.bounds
    
    # convert the pandas first row to list of bounding points
    #list_bbox = gdf_bounds.loc[0, :].values.tolist()
    list_bbox = [gdf_bounds['minx'].min(),
                gdf_bounds['miny'].min(),
                gdf_bounds['maxx'].max(),
                gdf_bounds['maxy'].max()]
    
    # convert list to tuple
    tup_bbox = tuple(list_bbox)
    
    # read bounding line data within the bounding box
    gdf_bb = gpd.read_file(str_nhd_stream_gpkg,bbox=tup_bbox)
    
    # convert stream data to source crs
    gdf_stream_bb_prj = gdf_bb.to_crs(source_crs)
    
    # remove the datetime coloumns
    gdf_stream_bb_prj_clean = gdf_stream_bb_prj.select_dtypes(exclude=['datetime64'])
    
    # intersect the stream line file and the mjr_axis_lines (buffered area)
    gdf_stream_intersection = gpd.sjoin(gdf_stream_bb_prj_clean,gdf_mjr_axis_buffer_ar_prj, predicate='intersects')

    # remove the datetime columns
    gdf_stream_intersection = gdf_stream_intersection.select_dtypes(exclude=['datetime64'])
    # -----
    
    
    for index_0, row_0 in tqdm.tqdm(gdf_mjr_axis_ln.iterrows(),
                                    total =gdf_mjr_axis_ln.shape[0],
                                    desc='Flip Lines',
                                    bar_format = "{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
                                    ncols=65):
        
        # initialize default values
        str_nhd_name = "99-No NHD Streams"
        str_nhd_reachcode = np.nan
        
        int_road_count = 0 #number of times road crosses a waterway
        b_reverse_road = False
        
        mjr_axis_wkt = gdf_mjr_axis_ln.geometry[index_0] #road geom to WellKnownText
        ls_mjr_axis_points = list(mjr_axis_wkt.coords)
        
        # get all the streams that cross the given current major axis
        # note all the streams that cross a given bridge will have a "index_right" equal to the mjr_axis id
        gdf_current_streams = gdf_stream_intersection.loc[gdf_stream_intersection['index_right'] == index_0]
        
        gdf_current_streams = gdf_current_streams.explode(index_parts=True)
        
        for index, row in gdf_current_streams.iterrows(): #For each stream
            river_geom_wkt = row['geometry'] #river geom to WellKnownText
            ls_river_points = list(river_geom_wkt.coords) #create list of river geom coordinates
        
            i = 0
            #get each edge between the series of points on road
    
            for point in ls_mjr_axis_points[:-1]:
                road_edge = tuple(ls_mjr_axis_points[i:i+2]) 
                j = 0
    
                #get each edge between the series of points on river
                for point_river in ls_river_points[:-1]:
                    river_edge = tuple(ls_river_points[j:j+2])
                    j +=1
    
                    if fn_intersects(road_edge, river_edge):
                        int_road_count += 1 #number of times road crosses a waterway
    
                        # TODO - 2022.12.30 - are the field names of NHD serivce lower case?
                        try:
                            str_nhd_name = row['GNIS_NAME']
                        except:
                            str_nhd_name = row['gnis_name']
    
                        try:
                            str_nhd_reachcode = row['REACHCODE']
                        except:
                            str_nhd_reachcode = row['reachcode']
    
                        # Get the river vector from the edge points
                        d_xRiv = river_edge[1][0]-river_edge[0][0]
                        d_yRiv = river_edge[1][1]-river_edge[0][1]
                        river_vector = [d_xRiv, d_yRiv]
    
                        # Get the road vector from the edge points
                        d_xRoad = road_edge[1][0]-road_edge[0][0]
                        d_yRoad = road_edge[1][1]-road_edge[0][1]
                        bridge_vector = [d_xRoad, d_yRoad]
    
                        # Cross product to determine if the road is left-to-right looking downstream
                        a = np.cross(river_vector,bridge_vector)
    
                        # A negative cross-product means the road is in the correct direction
                        if a > 0:
                            #Set flag to reverse (flip) the bridge from OSM
                            b_reverse_road = True
                i += 1
                    
        if b_reverse_road and ((int_road_count % 2)!= 0):
            # Reverse road is True and int_road_count is odd
            ls_mjr_axis_points.reverse() # reverse the list of bridge points
    
            flip_road_wkt = LineString(ls_mjr_axis_points) # convert list of points into WellKnownText
            gdf_mjr_axis_ln.at[index_0,'geometry'] = flip_road_wkt
    
        list_nhd_names.append(str_nhd_name)
        list_nhd_reachcode.append(str_nhd_reachcode)
        
    gdf_mjr_axis_ln['nhd_name'] = list_nhd_names
    gdf_mjr_axis_ln['reachcode'] = list_nhd_reachcode
    
    str_file_shp_to_write = os.path.join(str_output_dir, 'flip_mjr_axis_ln.shp')
    
    gdf_mjr_axis_ln.to_file(str_file_shp_to_write)
# ..........................................................


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='======== FLIP MAJOR AXIS LINES (DOWNSTREAM LEFT-TO-RIGHT) =========')

    
    parser.add_argument('-i',
                        dest = "str_major_axis_ln_path",
                        help=r'REQUIRED: path to major axis lines Example: C:\test\cloud_harvest\04_major_axis_lines\mjr_axis_ln.shp',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))

    parser.add_argument('-o',
                        dest = "str_output_dir",
                        help=r'REQUIRED: directory to flipped major axis lines: Example: C:\test\cloud_harvest\06_flipped_major_axis',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-b',
                        dest = "flt_mjr_axis",
                        help='OPTIONAL: distance to buffer major axis: Default=4.0 (Lambert units - meters)',
                        required=False,
                        default=0.3,
                        metavar='FLOAT',
                        type=float)
    
    parser.add_argument('-g',
                        dest = "str_nhd_stream_gpkg",
                        help=r'OPTIONAL: path to NHD stream lines Example: D:\tx_bridge_input_datasets\nhd\NHD_H_Texas_State_GPKG\nhd_h_texas_state_flowline_4269.gpkg',
                        required=True,
                        default=r'D:\tx_bridge_input_datasets\nhd\NHD_H_Texas_State_GPKG\nhd_h_texas_state_flowline_4269.gpkg',
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))
    
    args = vars(parser.parse_args())
    
    str_major_axis_ln_path = args['str_major_axis_ln_path']
    str_output_dir = args['str_output_dir']
    flt_mjr_axis = args['flt_mjr_axis']
    str_nhd_stream_gpkg = args['str_nhd_stream_gpkg']
    
    fn_flip_major_axis(str_major_axis_ln_path,
                       str_output_dir,
                       flt_mjr_axis,
                       str_nhd_stream_gpkg)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~