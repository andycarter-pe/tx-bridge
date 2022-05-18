# For the bridge 'major axis' lines that were extracted, determine the
# name of the line fromm OpenStreetMap data.
#
# Created by: Andy Carter, PE
# Created - 2022.05.18
# Last revised - 2022.05.18
#
# tx-bridge - 07 - seventh processing script
# Uses the 'pdal' conda environment

# ************************************************************
import argparse
import geopandas as gpd
import shapely.geometry
import pandas as pd
import numpy as np
import osmnx as ox
import os

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


# ..........................................................
def fn_assign_osm_names_major_axis(str_aoi_shp_path,str_mjr_axis_shp_path,str_output_dir,flt_perct_on_line,flt_offset):
    
    """
    Given a line shapefile of the 'major axis' lines, get the name of the line
    for roads and railroad.

    """
    
    print(" ")
    print("+=================================================================+")
    print("|          ASSIGN OPENSTEETMAP NAMES TO MAJOR AXIS LINES          |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(a) INPUT AREA OF INTEREST POLYGON: " + str_aoi_shp_path)
    print("  ---(i) INPUT MAJOR AXIS LINES: " + str_mjr_axis_shp_path)
    print("  ---(o) OUTPUT DIRECTORY: " + str_output_dir)
    print("  ---[r]   Optional: RATIO QUERRY LOCATION: " + str(flt_perct_on_line) )
    print("===================================================================")
    
    # create the output directory if it does not exist
    os.makedirs(str_output_dir, exist_ok=True)
    
    # define the "world geodetic system" espg
    # this is the projection of the OpenStreetMap OSMNX request
    wgs = "epsg:4326"
    
    # read the "area of interest" shapefile in to geopandas dataframe
    gdf_aoi_prj = gpd.read_file(str_aoi_shp_path)
    
    # convert "area of interest" to wgs
    gdf_aoi_wgs = gdf_aoi_prj.to_crs(wgs)
    
    # get the crs of the user supplied shapefile
    source_crs = gdf_aoi_prj.crs
    
    # get geoDataFrame of the boundary of the input shapefile
    gdf_bounds = gdf_aoi_wgs.bounds
    
    # convert the pandas first row to list of bounding points
    list_bbox = gdf_bounds.loc[0, :].values.tolist()
    
    # convert list to tuple
    tup_bbox = tuple(list_bbox)
    
    # shapely geom of bbox from tuple
    bbox_polygon = shapely.geometry.box(*tup_bbox, ccw=True)
    
    # --- get openstreetmap data ---
    print('Getting the OpenStreetMap data...')
    
    # get the roadway graph
    G = ox.graph_from_polygon(bbox_polygon, network_type='drive_service', simplify=False, retain_all=True)
    
    # convert the graph to a projected graph
    P = ox.projection.project_graph(G, to_crs=source_crs)
    
    # projected graph to geodataframes
    gdf_nodes, gdf_edges = ox.graph_to_gdfs(P)
    
    # remove the MultiIndexing
    gdf_edges.reset_index(inplace=True)
    
    # get the railway graph
    G_rail = ox.graph_from_polygon(bbox_polygon,retain_all=False, truncate_by_edge=False, simplify=False,
                                   custom_filter='["railway"~"tram|rail"]')
    
    # convert the graph to a projected graph
    P_rail = ox.projection.project_graph(G_rail, to_crs=source_crs)
    
    # projected graph to geodataframes
    gdf_nodes_rail, gdf_edges_rail = ox.graph_to_gdfs(P_rail)
    
    # remove the MultiIndexing
    gdf_edges_rail.reset_index(inplace=True)
    
    # combine the road and rail edge geodataframes
    gdf_edges_road_rail = pd.concat([gdf_edges,gdf_edges_rail], ignore_index=True)
    
    # --- get major axis names ---
    print('Getting major axis names...')
    
    # read the mjr axis lines
    gdf_mjr_axis_ln = gpd.read_file(str_mjr_axis_shp_path)
    
    # create Geodataframe for a point on each line
    gdf_mjr_axis_pt = gpd.read_file(str_mjr_axis_shp_path)
    
    # geopandas point on of mjr axis lines
    gdf_mjr_axis_pt['geometry'] = gdf_mjr_axis_pt.geometry.interpolate(flt_perct_on_line, normalized = True)
    
    bbox = gdf_mjr_axis_pt.bounds + [-flt_offset, -flt_offset, flt_offset, flt_offset]
    
    hits = bbox.apply(lambda row: list(gdf_edges_road_rail.sindex.intersection(row)), axis=1)
    
    df_tmp = pd.DataFrame({
        # index of points table
        "pt_idx": np.repeat(hits.index, hits.apply(len)),
        # ordinal position of line - access via iloc later
        "line_i": np.concatenate(hits.values)
    })
    
    # join back to the lines on line_i; we use reset_index() to give us the ordinal position of each line
    df_tmp = df_tmp.join(gdf_edges_road_rail.reset_index(drop=True), on="line_i")
    
    # join back to the original points to get their geometry
    # rename the point geometry as "point"
    df_tmp = df_tmp.join(gdf_mjr_axis_pt.geometry.rename("point"), on="pt_idx")
    
    # convert back to a GeoDataFrame
    df_tmp = gpd.GeoDataFrame(df_tmp, geometry="geometry", crs=gdf_mjr_axis_pt.crs)
    
    df_tmp["snap_dist"] = df_tmp.geometry.distance(gpd.GeoSeries(df_tmp.point))
    
    # discard any lines that are greater than tolerance from points
    df_tmp = df_tmp.loc[df_tmp.snap_dist <= flt_offset]
    
    # sort on ascending snap distance, so that closest goes to top
    df_tmp = df_tmp.sort_values(by=["snap_dist"])
    
    # group by the index of the points and take the first, which is the closest line 
    gdf_closest = df_tmp.groupby("pt_idx").first()
    
    # construct a GeoDataFrame of the closest lines
    gdf_closest = gpd.GeoDataFrame(gdf_closest, geometry="geometry")
    
    gdf_mjr_axis_name = gdf_closest[["name", "ref"]]
    
    # replace the name 'None' with the 'ref' value
    gdf_mjr_axis_name.name.fillna(gdf_mjr_axis_name.ref, inplace=True)
    
    # left join the lines with the names
    gdf_mjr_axis_ln = gdf_mjr_axis_ln.join(gdf_mjr_axis_name)
    
    str_file_shp_to_write = str_output_dir + '\\' +'flip_mjr_axis_w_name_ln.shp'
    gdf_mjr_axis_ln.to_file(str_file_shp_to_write)
# ..........................................................

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='============ ASSIGN OPENSTEETMAP NAMES TO MAJOR AXIS ==============')

    parser.add_argument('-a',
                        dest = "str_aoi_shp_path",
                        help=r'REQUIRED: area of interest shapefile Example: C:\test\cloud_harvest\00_aoi_shapefile\huc_12_aoi_2277.shp',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))

    parser.add_argument('-i',
                        dest = "str_mjr_axis_shp_path",
                        help=r'REQUIRED: major axis line shapefile: Example: C:\test\cloud_harvest\06_flipped_major_axis\flip_mjr_axis_ln.shp',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))
    
    parser.add_argument('-o',
                        dest = "str_output_dir",
                        help=r'REQUIRED: directory to flipped major axis lines: Example: C:\test\cloud_harvest\07_major_axis_names',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-r',
                        dest = "flt_perct_on_line",
                        help='OPTIONAL: ratio distance to create a point on major axis: Default=0.35',
                        required=False,
                        default=0.35,
                        metavar='FLOAT',
                        type=float)
    
    # hard setting this value
    # distance to search around mjr axis' points for nearest osm line
    flt_offset  = 0.01
    
    args = vars(parser.parse_args())
    
    str_aoi_shp_path = args['str_aoi_shp_path']
    str_mjr_axis_shp_path = args['str_mjr_axis_shp_path']
    str_output_dir = args['str_output_dir']
    flt_perct_on_line = args['flt_perct_on_line']
    
    fn_assign_osm_names_major_axis(str_aoi_shp_path,
                                   str_mjr_axis_shp_path,
                                   str_output_dir,
                                   flt_perct_on_line,
                                   flt_offset)

    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~