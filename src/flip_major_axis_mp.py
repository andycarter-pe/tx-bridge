# Flip the bridge major axis to be 'left-to-right' looking downstream if
# a National Hydrography Dataset (NHD) stream line is available
#
# Created by: Andy Carter, PE
# Created - 2023.03.03
#
# tx-bridge - 06 - sixth processing script
# Uses the 'pdal' conda environment

# ************************************************************
import argparse
import geopandas as gpd
import shapely.geometry
from shapely.geometry import LineString
import urllib
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
def fn_flip_major_axis(str_major_axis_ln_path,
                       str_aoi_ar_path,
                       str_nhd_stream_path,
                       str_output_dir,
                       flt_mjr_axis):
    
    """
    Flip major axis lines to be 'left-to-right' looking downstream if the
    road crosses a stream line.  Attribute the major axis line with the nhd
    stream name

    """
    
    print(" ")
    print("+=================================================================+")
    print("|        FLIP MAJOR AXIS LINES (DOWNSTREAM LEFT-TO-RIGHT)         |")
    print("|                   FROM LOCAL NHD STREAM FILE                    |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) INPUT MAJOR AXIS LINES: " + str_major_axis_ln_path)
    print("  ---(a) AREA OF INTEREST POLYGON: " + str_aoi_ar_path)
    print("  ---(n) NHD STREAM LINES: " + str_nhd_stream_path)
    print("  ---(o) OUTPUT DIRECTORY: " + str_output_dir)
    print("  ---[b]   Optional: BUFFER DISTANCE MAJOR AXIS: " + str(flt_mjr_axis) )
    print("===================================================================")
    
    # create the output directory if it does not exist
    os.makedirs(str_output_dir, exist_ok=True)
    
    # ---------------------------
    # clip the NHD streamline geopackage to the area-of-interest 
    # projection of nhd lines
    prj_nhd = "epsg:4269"
    
    # read the "area of interest" shapefile in to geopandas dataframe
    gdf_aoi_prj = gpd.read_file(str_aoi_ar_path)
    
    # convert aoi to prj_nhd
    gdf_aoi_prj_nhd = gdf_aoi_prj.to_crs(prj_nhd)
    
    # get the crs of the user supplied shapefile
    source_crs = gdf_aoi_prj.crs
    
    # get geoDataFrame of the boundary of the input shapefile
    gdf_bounds = gdf_aoi_prj_nhd.bounds
    
    # convert the pandas first row to list of bounding points
    list_bbox = gdf_bounds.loc[0, :].values.tolist()
    
    # convert list to tuple
    tup_bbox = tuple(list_bbox)
    
    print("Clipping NHD Stream line file to AOI ... ~1 minute")
    
    # read bounding line data within the bounding box
    gdf_streams_in_bb = gpd.read_file(str_nhd_stream_path, layer='NHDFlowline', bbox=tup_bbox)
    
    # convert nhd streams to source crs
    gdf_streams_in_bb_prj = gdf_streams_in_bb.to_crs(source_crs)
    
    # -- if there is a need to write out the clipped nhd lines
    #print('Writing clipped nhd stream file ... ~1 minute')
    #str_file_gpkg_to_write = os.path.join(str_output_dir, 'nhd_stream_clip.gpkg')
    #gdf_streams_in_bb_prj.to_file(str_file_gpkg_to_write, driver="GPKG")
    # ---------------------------
    
    # read the major axis lines
    gdf_mjr_axis_ln = gpd.read_file(str_major_axis_ln_path)
    
    # buffer the major axis lines
    gdf_mjr_axis_ln['geometry'] = gdf_mjr_axis_ln.geometry.buffer(flt_mjr_axis)
    
    # reproject the gdf_streams_in_bb_prj to crs of gdf_mjr_axis_ln
    mjr_axis_crs = gdf_mjr_axis_ln.crs
    gdf_streams_in_bb_mjr_axis_crs = gdf_streams_in_bb_prj.to_crs(mjr_axis_crs)
    
    print('Determining nhd stream lines within buffered major axis ...')
    gdf_streams_within_mjr_axis_buffer = gpd.sjoin(gdf_streams_in_bb_mjr_axis_crs, gdf_mjr_axis_ln)
    
    str_file_gpkg_to_write = os.path.join(str_output_dir, 'nhd_stream_in_buffer.gpkg')
    gdf_streams_within_mjr_axis_buffer.to_file(str_file_gpkg_to_write, driver="GPKG")
    
    # re-read the major axis lines to convert back to lines
    gdf_mjr_axis_ln = gpd.read_file(str_major_axis_ln_path)

    list_nhd_names = []
    list_nhd_reachcode = []
    
    for index_mjr_axis, row_0 in tqdm.tqdm(gdf_mjr_axis_ln.iterrows(),
                                           total =gdf_mjr_axis_ln.shape[0],
                                           desc='Flip Lines',
                                           bar_format = "{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
                                           ncols=65):
    
    #for index_mjr_axis, row_0 in gdf_mjr_axis_ln.iterrows():
        # initialize default values
        str_nhd_name = "99-No NHD Streams"
        str_nhd_reachcode = np.nan
        
        # get a geodataframe of the streams that cross
        gdf_crossing_streams = gdf_streams_within_mjr_axis_buffer.loc[gdf_streams_within_mjr_axis_buffer['index_right'] == index_mjr_axis]
        
        int_road_count = 0 # number of times road crosses a waterway
        b_reverse_road = False
        
        mjr_axis_wkt = gdf_mjr_axis_ln.geometry[index_mjr_axis] #road geom to WellKnownText
        ls_mjr_axis_points = list(mjr_axis_wkt.coords)
        
        for index, row in gdf_crossing_streams.iterrows(): #For each stream
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
                            str_possible_nhd_name = row['GNIS_NAME']
                        except:
                            str_possible_nhd_name = row['gnis_name']
    
                        try:
                            str_possible_nhd_reachcode = row['REACHCODE']
                        except:
                            str_possible_nhd_reachcode = row['reachcode']
                        
                        # for roads with multiple stream crossings,
                        # change the name only if it isn't None
                        if int_road_count > 1:
                            if str_possible_nhd_name != None:
                                str_nhd_name = str_possible_nhd_name
                                str_nhd_reachcode = str_possible_nhd_reachcode
                        else:
                            str_nhd_name = str_possible_nhd_name
                            str_nhd_reachcode = str_possible_nhd_reachcode
    
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
            gdf_mjr_axis_ln.at[index_mjr_axis,'geometry'] = flip_road_wkt
            
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
                        help=r'REQUIRED: path to major axis lines Example: D:\globus_transfer\tx-bridge-south_central\04_major_axis_lines',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))
    
    parser.add_argument('-a',
                        dest = "str_aoi_ar_path",
                        help=r'REQUIRED: path to area of interest polygon Example: D:\globus_transfer\merge_output\aoi_south_central_ar_3857.shp',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))
    
    parser.add_argument('-n',
                        dest = "str_nhd_stream_path",
                        help=r'REQUIRED: path to nhd lines Example: D:\tx_bridge_input_datasets\nhd\NHD_H_Texas_State_GPKG\NHD_H_Texas_State_GPKG.gpkg',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))

    parser.add_argument('-o',
                        dest = "str_output_dir",
                        help=r'REQUIRED: directory to flipped major axis lines: Example: D:\globus_transfer\tx-bridge-south_central\06_flipped_major_axis',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-b',
                        dest = "flt_mjr_axis",
                        help='OPTIONAL: distance to buffer major axis: Default=0.3 (Lambert units - meters)',
                        required=False,
                        default=0.3,
                        metavar='FLOAT',
                        type=float)
    
    args = vars(parser.parse_args())
    
    str_major_axis_ln_path = args['str_major_axis_ln_path']
    str_aoi_ar_path = args['str_aoi_ar_path']
    str_nhd_stream_path = args['str_nhd_stream_path']
    str_output_dir = args['str_output_dir']
    flt_mjr_axis = args['flt_mjr_axis']

    
    fn_flip_major_axis(str_major_axis_ln_path,
                       str_aoi_ar_path,
                       str_nhd_stream_path,
                       str_output_dir,
                       flt_mjr_axis)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~