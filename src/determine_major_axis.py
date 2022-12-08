# From the OpenStreetMap linework and the polygons of the bridges, determine
# the longest line that crosses the bridge polygons.  This is to determine the
# 'major axis' line for each bridge.
#
# Created by: Andy Carter, PE
# Created - 2022.05.16
# Last revised - 2022.05.16
#
# tx-bridge - fourth processing script
# Uses the 'pdal' conda environment


# ************************************************************
import argparse
import geopandas as gpd
import pandas as pd

import os

from shapely.geometry import LineString

import time
import datetime

import tqdm
from time import sleep
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


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def fn_get_major_axis_for_polygon(shp_bridge_ar_fn, flt_bridge_buffer_fn, gdf_trans):

    """
    Last revised - 20220418
    
    Get the major axis of the polygon object 'bridge' from an input line vector dataset 'transportation'

    Args:

        shp_bridge_ar_fn: polygon shape of the bridge
        flt_bridge_buffer_fn: distance to buffer the bridge polygon
        gdf_trans: Geodataframe of linear transportation with crs set to same as shp_bridge_ar_fn
        
    Returns:

        shp_major_axis: shapely linestring of the major axis
    """
    
    # bridge polygon to Linestring
    shp_bridge_ln = LineString(list(shp_bridge_ar_fn.exterior.coords))

    # buffer the shape - to get some distance beyond the abutments
    shp_bridge_buffer_ar = shp_bridge_ar_fn.buffer(flt_bridge_buffer_fn)

    # create a geodataframe of the buffered polygon
    gdf_current_bridge_buffer = gpd.GeoDataFrame(index=[0], crs = gdf_trans.crs, geometry = [shp_bridge_buffer_ar])

    # compute the area of the buffered polygon
    gdf_current_bridge_buffer['area'] = gdf_current_bridge_buffer['geometry'].area

    ## determine the trasportation linear features that clip to boundary polygon

    # clip the roads/rail to the bridge lines
    gdf_current_bridge_trans_intersect = gpd.overlay(gdf_trans, gdf_current_bridge_buffer, how='intersection')

    # compute a length field for each intersected and clipped line
    gdf_current_bridge_trans_intersect['length'] = gdf_current_bridge_trans_intersect['geometry'].length

    ## pull only the intersecting lines that could be a 'major axis' [longer than the square root of the area]
    list_possible_major_ln = []

    for index, row in gdf_current_bridge_trans_intersect.iterrows():
        flt_minor_axis = row['area'] / row['length']
        if flt_minor_axis < row['length']:
            list_possible_major_ln.append(index)

    if len(list_possible_major_ln) > 0:
        # create a new gdf of possible major axis
        gdf_possible_major_axis_ln = gdf_current_bridge_trans_intersect.iloc[list_possible_major_ln]

        # resest the dataframe index
        gdf_possible_major_axis_ln = gdf_possible_major_axis_ln.reset_index()

        list_long_axis_id_cross_twice = []

        # add lines that cross the at least twice
        for index, row in gdf_possible_major_axis_ln.iterrows():

            # get shape of current row (transportation line)
            shp_transport_ln = gdf_possible_major_axis_ln.iloc[index]['geometry']

            # intersection of the buffered bridge deck exterior and 'long axis' line
            shp_intersect_pnt = shp_transport_ln.intersection(shp_bridge_ln)

            if shp_intersect_pnt.geom_type == "MultiPoint":
                #long axis crosses the buffered boundary more than once
                list_long_axis_id_cross_twice.append(index)

        # crete a geodataframe of just the indecies in list_long_axis_id_cross_twice
        if len(list_long_axis_id_cross_twice) > 0:
            gdf_possible_major_axis_cross_twice_ln = gdf_possible_major_axis_ln.iloc[list_long_axis_id_cross_twice]

            # TODO - do we want the line nearest the polygon centroid? - 20220418
            # get the longest line of the remaining lines
            
            int_rowid_longest = gdf_possible_major_axis_cross_twice_ln['length'].idxmax()

            try:
                shp_major_axis = gdf_possible_major_axis_cross_twice_ln.iloc[int_rowid_longest]['geometry']
            except:
                # TODO - Error Found - It is possible that that two lines of the same length are found
                # and these lines are in opposite directions - Need to fix
                # 2022.12.05 - MAC
                shp_major_axis = gdf_possible_major_axis_cross_twice_ln.iloc[0]['geometry']
                print('Error Found')

            return(shp_major_axis)
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++


# --------------------------------------------------------
def fn_determine_major_axis(str_bridge_polygons_path,str_trans_line_path,str_output_dir,flt_buffer_hull):
    
    """
    Determine the major axis line from the bridge hull polygons and the
    OpenStreetMap lines.

    Args:
        str_bridge_polygons_path: path convex hull polygons
        str_trans_line_path: path to the OpenStreetMap transporation lines
        str_output_dir: where to write the shapefile of the hull lines
        flt_buffer_hull: distance to extend the major axis beyond hull - OSM linework units = AOI units

    Returns:
        geodataframe of major axis
    """
    
    print(" ")
    print("+=================================================================+")
    print("|              DETERMINE MAJOR-AXIS FOR BRIDGE HULLS              |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(b) INPUT CONVEX HULL SHAPEFILE PATH: " + str_bridge_polygons_path)
    print("  ---(t) TRANSPORTATION SHAPEFILE PATH: " + str_trans_line_path)
    print("  ---(o) OUTPUT DIRECTORY: " + str_output_dir)
    print("  ---[x]   Optional: AXIS BUFFER DISTANCE: " + str(flt_buffer_hull) )
    print("===================================================================")
    
    # create the output directory if it does not exist
    os.makedirs(str_output_dir, exist_ok=True)
    
    # read the bridge polygons
    gdf_bridge_ar = gpd.read_file(str_bridge_polygons_path)
    
    # read the transporation linework
    gdf_trans = gpd.read_file(str_trans_line_path)
    
    # set the crs of the gdf_bridge_ar
    gdf_bridge_ar = gdf_bridge_ar.to_crs(gdf_trans.crs)
    
    # get the major axis for each polygon in the provided hulls
    list_major_axis_shp = []
    
    
    for index, row in tqdm.tqdm(gdf_bridge_ar.iterrows(),
                                total = gdf_bridge_ar.shape[0],
                                desc='Determine axis',
                                bar_format = "{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
                                ncols=65):
        shp_bridge_ar = row['geometry']
        sleep(0)
        
        shp_major_axis = fn_get_major_axis_for_polygon(shp_bridge_ar, flt_buffer_hull, gdf_trans)
        list_major_axis_shp.append(shp_major_axis)

    # add the major axis linestrings to the geodataframe
    gdf_bridge_ar['mjr_axis'] = list_major_axis_shp
    
    # add new coloumns
    gdf_bridge_ar["hull_len"] = None
    gdf_bridge_ar["avg_width"] = None
    
    # computing the bridge length and width
    for index,row in gdf_bridge_ar.iterrows():
        if not (pd.isnull(row['mjr_axis'])):
            # Geodataframe has a valid linestring for mjr axis
            shp_mjr_axis_ln = row['mjr_axis']
            shp_bridge_ar = row['geometry']
            
            # clip major axis line to bridge hull
            # TODO - what if there is no intersection? 20220421
            shp_mjr_axis_clip = shp_mjr_axis_ln.intersection(shp_bridge_ar)
            
            flt_bridge_area = shp_bridge_ar.area
            
            flt_mjr_axis_length = shp_mjr_axis_clip.length
            flt_avg_width = flt_bridge_area / flt_mjr_axis_length
            
            gdf_bridge_ar.iat[index, gdf_bridge_ar.columns.get_loc('hull_len')] = flt_mjr_axis_length
            gdf_bridge_ar.iat[index, gdf_bridge_ar.columns.get_loc('avg_width')] = flt_avg_width
    
    # copy the geodataframe
    gdf_bridge_mjr_axis_ln = gdf_bridge_ar
    
    # set the active geometry to the mjr_axis
    gdf_bridge_mjr_axis_ln = gdf_bridge_mjr_axis_ln.set_geometry("mjr_axis")
    
    # get row in gdf_bridge_mjr_axis_ln where 'mjr_axis' is null
    nan_mjr_axis_rows = gdf_bridge_mjr_axis_ln[gdf_bridge_mjr_axis_ln['mjr_axis'].isnull()]
    
    # remove the null rows from the dataframe
    gdf_bridge_mjr_axis_ln = gdf_bridge_mjr_axis_ln.drop(nan_mjr_axis_rows.index)
    
    # delete the las_paths
    # TODO -2022.05.16 stringify instead
    del gdf_bridge_mjr_axis_ln['las_paths']
    del gdf_bridge_mjr_axis_ln['geometry']
    
    str_file_shp_to_write = os.path.join(str_output_dir, 'mjr_axis_ln.shp')
    
    # TODO - Processing Error - Projection ??? MAC - 2022.12.03
    gdf_bridge_mjr_axis_ln = gdf_bridge_mjr_axis_ln.set_crs(gdf_trans.crs)
    
    gdf_bridge_mjr_axis_ln.to_file(str_file_shp_to_write)

# --------------------------------------------------------

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='============== DETERMINE MAJOR-AXIS FOR BRIDGE HULLS ==============')
    
    parser.add_argument('-b',
                        dest = "str_bridge_polygons_path",
                        help=r'REQUIRED: path to bridge hull polygon shapefile Example: C:\test\cloud_harvest\02_shapefile_of_hulls\class_17_ar_3857.shp',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))

    parser.add_argument('-t',
                        dest = "str_trans_line_path",
                        help=r'REQUIRED: path to transporation line shapefile: Example: C:\test\cloud_harvest\03_osm_trans_lines\osm_trans_ln.shp',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))
    
    parser.add_argument('-o',
                        dest = "str_output_dir",
                        help=r'REQUIRED: directory to write line shapefile: Example: C:\test\cloud_harvest\04_major_axis_lines',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-x',
                        dest = "flt_buffer_hull",
                        help='OPTIONAL: distance to extend major axis beyond hull: Default=20',
                        required=False,
                        default=20,
                        metavar='FLOAT',
                        type=float)
    
    args = vars(parser.parse_args())
    
    str_bridge_polygons_path = args['str_bridge_polygons_path']
    str_trans_line_path = args['str_trans_line_path']
    str_output_dir = args['str_output_dir']
    flt_buffer_hull = args['flt_buffer_hull']
    
    fn_determine_major_axis(str_bridge_polygons_path,
                            str_trans_line_path,
                            str_output_dir,
                            flt_buffer_hull)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~