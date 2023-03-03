# Given an input polygon shapefile, get a shapefile of the transportation
# alignments (lines) from OpenStreetMaps.
#
# Created by: Andy Carter, PE
# Created - 2022.04.27
# Last revised - 2022.07.20
#
# tx-bridge - third processing script
# Uses the 'pdal' conda environment


# ************************************************************
import argparse
import geopandas as gpd
import pandas as pd
import shapely.geometry

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


# ````````````````````````````````````````````````````````
def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')
# ````````````````````````````````````````````````````````


# --------------------------------------------------------
def fn_get_osm_lines_from_shp(str_input_path,str_output_dir,b_simplify_graph,b_get_drive_service,b_get_railroad):
    
    """
    Fetch the OSM road and rail linework from first polygon in the user supplied polygon

    Args:
        str_input_path: path to the requested polygon shapefile
        str_output_dir: path to write the output shapefile
        b_simplify_graph:T/F to simplify the graphs
        b_get_drive_service: T/F to fetch roads
        b_get_railroad: T/F to fecth railroad
        
    Returns:
        geodataframe of transporation lines (edges)
    """
    
    print(" ")
    print("+=================================================================+")
    print("|         OPENSTREETMAP TRANSPORTATION LINES FROM POLYGON         |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) INPUT SHAPEFILE PATH: " + str_input_path)
    print("  ---(o) OUTPUT DIRECTORY: " + str_output_dir)
    print("  ---[s]   Optional: SIMPLIFY OSM GRAPHS: " + str(b_simplify_graph) )
    print("  ---[d]   Optional: GET OSM 'SERVICE DRIVES': " + str(b_get_drive_service) )
    print("  ---[r]   Optional: GET OSM 'RAILWAYS': " + str(b_get_railroad) ) 
    print("===================================================================")

    print("Fetching OpenStreetMap linework...")
    
    # create the output directory if it does not exist
    os.makedirs(str_output_dir, exist_ok=True)

    # option to turn off the SettingWithCopyWarning
    pd.set_option('mode.chained_assignment', None)

    wgs = "epsg:4326"
    
    # read the "area of interest" shapefile in to geopandas dataframe
    gdf_aoi_prj = gpd.read_file(str_input_path)
    
    # buffer the gdf_aoi_prj
    gdf_aoi_prj = gdf_aoi_prj.buffer(10000)
    
    # convert aoi to wgs
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
    
    b_got_rail = False
    if b_get_railroad:
        try:
            G_rail = ox.graph_from_polygon(bbox_polygon,
                            retain_all=False, truncate_by_edge=False, simplify=False,
                            custom_filter='["railway"~"tram|rail"]')
            b_got_rail = True
            
            # convert the graph to a projected graph
            P_rail = ox.projection.project_graph(G_rail, to_crs=source_crs)
        
            # copy the projected graph
            P_rail_simple = P_rail.copy()
        
            if b_simplify_graph:
                # simplify the road graph
                P_rail_simple = ox.simplify_graph(P_rail_simple)
        
            # projected graph to geodataframes
            gdf_rail_nodes, gdf_rail_edges = ox.graph_to_gdfs(P_rail_simple)
        
            # remove the MultiIndexing
            gdf_rail_edges.reset_index(inplace=True)
        except:
            print('Warning:  Railroad data not found on OSM')
    
    b_got_drive = False
    if b_get_drive_service:
        try:
            # get the roadway graph
            G = ox.graph_from_polygon(bbox_polygon, network_type='drive_service', simplify=False, retain_all=True)
            
            b_got_drive = True
            
            # convert the graph to a projected graph
            P = ox.projection.project_graph(G, to_crs=source_crs)
            
            # copy the projected graph
            P2 = P.copy()
            
            if b_simplify_graph:
                # simplify the road graph
                P2 = ox.simplify_graph(P2)
            
            # projected graph to geodataframes
            gdf_road_nodes, gdf_road_edges = ox.graph_to_gdfs(P2)
            
            # remove the MultiIndexing
            gdf_road_edges.reset_index(inplace=True)
        except:
            print('Warning:  Roadway data not found')
    
    b_file_to_create = False
    
    # both rail and drive data
    if b_got_rail and b_got_drive:
        # merge the two dataframes
        gdf_trans_edge = pd.concat([gdf_rail_edges, gdf_road_edges])
        b_file_to_create = True
            
    if b_got_rail and not b_got_drive:
        # rail only
        gdf_trans_edge = gdf_rail_edges
        b_file_to_create = True
        
    if not b_got_rail and b_got_drive:
        # drive only
        gdf_trans_edge = gdf_road_edges
        b_file_to_create = True
    
    if b_file_to_create:
        if 'name' not in gdf_trans_edge.columns:
            gdf_trans_edge['name'] = ''
            
        # sample to the selected coloumns
        gdf_edges_mod = gdf_trans_edge[['u','v', 'osmid', 'name','geometry']]
            
        gdf_edges_mod['name'] = gdf_edges_mod['name'].astype(str)
        
        # convert coloumns to string (to stringify lists)
        gdf_edges_mod['osmid'] = gdf_edges_mod['osmid'].astype(str)
        gdf_edges_mod['name'] = gdf_edges_mod['name'].astype(str)
        
        # write a shapefile of the lines
        
        str_file_shp_to_write = os.path.join(str_output_dir, 'osm_trans_ln.shp')
        gdf_edges_mod.to_file(str_file_shp_to_write)
        
        return gdf_edges_mod
    else:
        "ERROR: No OSM data found. (rail or road)"
    
# --------------------------------------------------------


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='========= OPENSTREETMAP TRANSPORTATION LINES FROM POLYGON =========')
    
    parser.add_argument('-i',
                        dest = "str_input_path",
                        help=r'REQUIRED: path to the input shapefile (polygon) Example: C:\test\cloud_harvest\00_aoi_shapefile\huc_12_aoi_2277.shp',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))

    parser.add_argument('-o',
                        dest = "str_output_dir",
                        help=r'REQUIRED: directory to write line shapefile: Example: C:\test\cloud_harvest\03_osm_trans_lines',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-s',
                        dest = "b_simplify_graph",
                        help='OPTIONAL: simplify sampled graphs: Default=True',
                        required=False,
                        default=True,
                        metavar='T/F',
                        type=str2bool)
    
    parser.add_argument('-d',
                        dest = "b_get_drive_service",
                        help='OPTIONAL: sample the drive_service OSM layer: Default=True',
                        required=False,
                        default=True,
                        metavar='T/F',
                        type=str2bool)
    
    parser.add_argument('-r',
                        dest = "b_get_railroad",
                        help='OPTIONAL: sample the railway OSM layer: Default=True',
                        required=False,
                        default=True,
                        metavar='T/F',
                        type=str2bool)
    
    args = vars(parser.parse_args())
    
    str_input_path = args['str_input_path']
    str_output_dir = args['str_output_dir']
    b_simplify_graph = args['b_simplify_graph']
    b_get_drive_service = args['b_get_drive_service']
    b_get_railroad = args['b_get_railroad']
    
    fn_get_osm_lines_from_shp(str_input_path,
                              str_output_dir,
                              b_simplify_graph,
                              b_get_drive_service,
                              b_get_railroad)
    
    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~