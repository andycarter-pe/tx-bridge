# Extract the bridge deck dem and ground profile from the major axis line
#
# Created by: Andy Carter, PE
# Created - 2022.05.19
# Last revised - 2022.05.19
#
# tx-bridge - 08 - eighth processing script
# Uses the 'pdal' conda environment

# ************************************************************
import argparse
import geopandas as gpd
import pandas as pd
import urllib.request
# import rasterio
import rioxarray as rio
from rasterio.io import MemoryFile
import math
import numpy as np
from shapely.geometry import LineString
import os
import tqdm

import matplotlib.pyplot as plt
import matplotlib.ticker as tick

import time
import datetime
from datetime import date
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

# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
def fn_distance(x1,x2,y1,y2):
    sq1 = (x1-x2)*(x1-x2)
    sq2 = (y1-y2)*(y1-y2)
    return math.sqrt(sq1 + sq2)
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

# ..........................................................
def fn_extract_deck_profile(str_mjr_axis_shp_path,
                            str_deck_dem_path,
                            str_output_dir,
                            b_is_feet,
                            flt_mjr_axis,
                            int_resolution,
                            flt_xs_sample_interval):
    
    """
    Get a profile of the bridge deck and ground data for a given major axis line

    """
    
    print(" ")
    print("+=================================================================+")
    print("|           EXTRACT BRIDGE DECK PROFILE FROM MAJOR AXIS           |")
    print("|                Created by Andy Carter, PE of                    |")
    print("|             Center for Water and the Environment                |")
    print("|                 University of Texas at Austin                   |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) INPUT MAJOR AXIS LINES: " + str_mjr_axis_shp_path)
    print("  ---(d) INPUT PATH TO DECK DEMS: " + str_deck_dem_path)
    print("  ---(o) OUTPUT DIRECTORY: " + str_output_dir)
    print("  ---[v]   Optional: DATA IN FEET: " + str(b_is_feet) )
    print("===================================================================")
    
    # create the output directory if it does not exist
    os.makedirs(str_output_dir, exist_ok=True)
    
    # read the major axis lines
    gdf_mjr_axis_ln = gpd.read_file(str_mjr_axis_shp_path)
    
    # define the "lambert" espg
    lambert = "epsg:3857"
    
    # get crs of the input shapefile
    str_crs_model = str(gdf_mjr_axis_ln.crs)
    
    # convert the input shapefile to lambert
    gdf_mjr_axis_ln_lambert = gdf_mjr_axis_ln.to_crs(lambert)
    
    # buffer the input lines
    gdf_mjr_axis_ln_lambert['geometry'] = gdf_mjr_axis_ln_lambert.geometry.buffer(flt_mjr_axis)
    
    for index, row in tqdm.tqdm(gdf_mjr_axis_ln_lambert.iterrows(),
                                total =gdf_mjr_axis_ln_lambert.shape[0],
                                desc='Extract Profiles',
                                bar_format = "{desc}:({n_fmt}/{total_fmt})|{bar}| {percentage:.1f}%",
                                ncols=65):
        
        # the geometry from the requested polygon as wellKnownText
        boundary_geom_WKT = gdf_mjr_axis_ln_lambert['geometry'][index]  # to WellKnownText
        
        # the bounding box of the requested lambert polygon
        b = boundary_geom_WKT.bounds
        
        # convert the bounding coordinates to integers
        list_int_b = []
        for i in b:
            list_int_b.append(int(i//1))
            
        int_tile_x = list_int_b[2] - list_int_b[0]
        int_tile_y = list_int_b[3] - list_int_b[1]
        
        str_URL_header = r'https://elevation.nationalmap.gov/arcgis/services/3DEPElevation/ImageServer/WCSServer?'
        str_URL_query_1 = r'SERVICE=WCS&VERSION=1.0.0&REQUEST=GetCoverage&coverage=DEP3Elevation&CRS=EPSG:3857&FORMAT=GeoTiff'

        str_bbox = str(list_int_b[0]) + "," + str(list_int_b[1]) + "," + str(list_int_b[2]) + "," + str(list_int_b[3])

        str_URL_query_bbox = "&BBOX=" + str_bbox
        str_URL_query_dim = "&WIDTH=" + str(int_tile_x/int_resolution) + "&HEIGHT=" + str(int_tile_y/int_resolution)

        str_url = str_URL_header + str_URL_query_1 + str_URL_query_bbox + str_URL_query_dim
        
        # url request to get terrain
        http_response_raster = urllib.request.urlopen(str_url)

        # convert the response to bytes
        byte_response_raster = http_response_raster.read()
        


        with MemoryFile(byte_response_raster) as memfile:
            with memfile.open() as ground_terrain_src:
                
                # read the DEM as a "Rioxarray"
                ground_dem = rio.open_rasterio(ground_terrain_src).squeeze()
                
                # reproject the raster to the same projection as the road
                ground_dem_local_proj = ground_dem.rio.reproject(gdf_mjr_axis_ln.crs, nodata = np.nan)
                
                if b_is_feet:
                    # scale the raster from meters to feet
                    ground_dem_local_proj = ground_dem_local_proj * 3.28084
        
        # build a file path string to the deck dem
        if  b_is_feet:
            str_deck_path = str_deck_dem_path + '\\' + str(index) + '_bridge_deck_dem_vert_ft.tif'
        else:
            str_deck_path = str_deck_dem_path + '\\' + str(index) + '_bridge_deck_dem_vert_m.tif'
        
        # read the bridge deck DEM as a "Rioxarray"
        deck_dem = rio.open_rasterio(str_deck_path)

        # reproject the raster to the same projection as the road
        deck_dem_local_proj = deck_dem.rio.reproject(gdf_mjr_axis_ln.crs, nodata = np.nan)
        
        total_length = 0

        for i in range(len(gdf_mjr_axis_ln.geometry[index].coords)-1):
            start_coords = list(gdf_mjr_axis_ln.geometry[index].coords)[i]
            end_coords = list(gdf_mjr_axis_ln.geometry[index].coords)[i+1]
            
            #Get the length of the current road edge
            len_current_edge = fn_distance(start_coords[0],end_coords[0],start_coords[1],end_coords[1])
            
            x_ls = [start_coords[0]]
            y_ls = [start_coords[1]]
            
            # create a point at a requested interval - sort of
            n_points = int(len_current_edge // flt_xs_sample_interval)
            
            if n_points > 0:
                for j in np.arange(1, n_points):
                    x_dist = end_coords[0] - start_coords[0]
                    y_dist = end_coords[1] - start_coords[1]
                    point = [(start_coords[0] + (x_dist/(n_points))*j), (start_coords[1] + (y_dist/(n_points))*j)]
                    x_ls.append(point[0])
                    y_ls.append(point[1])
            
            if total_length == 0: # on the first road edge
                # Getting the station - horizontal distance
                df = pd.DataFrame({'x': x_ls,'y': y_ls})
                gdf = gpd.GeoDataFrame(df, geometry = gpd.points_from_xy(df.x, df.y))
                    
                gdf.crs = gdf_mjr_axis_ln.crs
                    
                gdf['h_distance'] = 0 # Intialize the variable in dataframe
                
                for index2, row2 in gdf.iterrows():
                    gdf['h_distance'].loc[index2] = gdf.geometry[0].distance(gdf.geometry[index2])
                    
                gdf['elev_grnd'] = np.nan # Intialize the variable in dataframe
                gdf['elev_deck'] = np.nan
                    
                for index3, row3 in gdf.iterrows():
                    # get the value at nearest point on the rioxarray
                    arr_np_raster_val = ground_dem_local_proj.sel(x = row3['x'], y = row3['y'], method="nearest").values
                    if arr_np_raster_val.size == 1:
                        gdf['elev_grnd'].loc[index3] = arr_np_raster_val
                        
                    arr_np_deck_val = deck_dem_local_proj.sel(x = row3['x'], y = row3['y'], method="nearest").values
                    if arr_np_deck_val.size == 1:
                        gdf['elev_deck'].loc[index3] = arr_np_deck_val
                del df
                
            else: #Any edge of road other than the first edge
                if i == (len(gdf_mjr_axis_ln.geometry[index].coords)-2):
                    #This is the last edge on the road- add the last point
                    x_ls.append(end_coords[0])
                    y_ls.append(end_coords[1])
        
                df_newEdge = pd.DataFrame({'x': x_ls,'y': y_ls})
                gdf_newEdge = gpd.GeoDataFrame(df_newEdge, geometry = gpd.points_from_xy(df_newEdge.x, df_newEdge.y))
                gdf_newEdge.crs = gdf_mjr_axis_ln.crs
        
                gdf_newEdge['h_distance'] = 0 #Intialize the variable in dataframe
        
                for index2, row2 in gdf_newEdge.iterrows():
                    gdf_newEdge['h_distance'].loc[index2] = gdf_newEdge.geometry[0].distance(gdf_newEdge.geometry[index2]) + total_length
        
                gdf_newEdge['elev_grnd'] = np.nan
                gdf_newEdge['elev_deck'] = np.nan
        
                for index3, row3 in gdf_newEdge.iterrows():
                    # get the value at nearest point on the rioxarray
                    arr_np_raster_val = ground_dem_local_proj.sel(x = row3['x'], y = row3['y'], method="nearest").values
                    if arr_np_raster_val.size == 1:
                        gdf_newEdge['elev_grnd'].loc[index3] = arr_np_raster_val
                        
                    arr_np_deck_val = deck_dem_local_proj.sel(x = row3['x'], y = row3['y'], method="nearest").values
                    if arr_np_deck_val.size == 1:
                        gdf_newEdge['elev_deck'].loc[index3] = arr_np_deck_val
                        
                gdf = pd.concat([gdf,gdf_newEdge], ignore_index=False)
        
                del df_newEdge
                del gdf_newEdge
                    
            total_length += len_current_edge
        
        # reset the index of the gdf
        gdf = gdf.reset_index(drop=True)
        
        list_station = gdf['h_distance'].tolist()
        list_ground_elev = gdf['elev_grnd'].tolist()
        list_deck_elev = gdf['elev_deck'].tolist()
        
        # keep only the records that don't have null values
        gdf_other = gdf[~gdf.isna().any(1)]
        
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # ### Simplify the line of the road section ###
        # create a Linestring from the (station, elevation) line
        ar_coordinates = gdf_other[['h_distance', 'elev_deck']].to_numpy()
        line = LineString(ar_coordinates)
        
        # using shapely - simplify the line to 0.1 feet
        # TODO - Better Filter for noise - 2022.05.19
        # https://stackoverflow.com/questions/37598986/reducing-noise-on-data
        
        tolerance = 0.2
        simplified_line = line.simplify(tolerance, preserve_topology=False)
        
        list_simple_line = list(zip(*simplified_line.coords.xy))
        list_simple_deck_sta = [item[0] for item in list_simple_line]
        list_simple_deck_elev = [item[1] for item in list_simple_line]
        
        # TODO - Convert to plotting function - 2022.05.19
        # ---------------------------
        # plot the station - elevation data

        fig = plt.figure()
        fig.patch.set_facecolor('gainsboro')
        fig.suptitle(gdf_mjr_axis_ln_lambert['name'][index] + ' @ ' + gdf_mjr_axis_ln_lambert['nhd_name'][index], fontsize=14, fontweight='bold')
        #fig.suptitle(gdf_mjr_axis_ln_lambert['nhd_name'][index], fontsize=14, fontweight='bold')
        
        ax = plt.gca()
        today = date.today()
        
        ax.text(0.98, 0.04, 'Created: ' + str(today),
                verticalalignment='bottom',
                horizontalalignment='right',
                backgroundcolor='w',
                transform=ax.transAxes,
                fontsize=6,
                style='italic')
        
        ax.text(0.98, 0.09, 'Reach Code:' + str(gdf_mjr_axis_ln_lambert['reachcode'][index]),
                verticalalignment='bottom',
                horizontalalignment='right',
                backgroundcolor='w',
                transform=ax.transAxes, fontsize=6, style='italic')
        
        ax.text(0.98, 0.14, 'University of Texas',
                verticalalignment='bottom',
                horizontalalignment='right',
                backgroundcolor='w',
                transform=ax.transAxes, fontsize=6, style='italic')
        
        plt.plot(list_station, list_ground_elev, label = "ground")  # creates the line
        plt.plot(list_simple_deck_sta, list_simple_deck_elev, label = "deck_simple")  # creates the line
        
        if b_is_feet:
            plt.ylabel('Elevation (ft)')
            plt.xlabel('Station (ft)')
        else:
            plt.ylabel('Elevation (m)')
            plt.xlabel('Station (m)')
        
        plt.grid(True)
        
        # Save the bridge envelope 
        str_file_name = str(index) + '_bridge_envelope.png'
        str_rating_image_path = str_output_dir + '\\' + str_file_name
        
        plt.savefig(str_rating_image_path,
            dpi=300,
            bbox_inches="tight")
    
        plt.cla()
        plt.close('all')
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if __name__ == '__main__':

    flt_start_run = time.time()
    
    parser = argparse.ArgumentParser(description='========== EXTRACT BRIDGE DECK PROFILE FROM MAJOR AXIS ============')

    parser.add_argument('-i',
                        dest = "str_mjr_axis_shp_path",
                        help=r'REQUIRED: major axis line shapefile: Example: C:\test\cloud_harvest\07_major_axis_names\flip_mjr_axis_w_name_ln.shp',
                        required=True,
                        metavar='FILE',
                        type=lambda x: is_valid_file(parser, x))
    
    parser.add_argument('-d',
                        dest = "str_deck_dem_path",
                        help=r'REQUIRED: directory containing bridge deck DEMs: Example: C:\test\cloud_harvest\05_bridge_deck_dems',
                        required=True,
                        metavar='DIR',
                        type=str)
    
    parser.add_argument('-o',
                        dest = "str_output_dir",
                        help=r'REQUIRED: directory to flipped major axis lines: Example: C:\test\cloud_harvest\08_deck_profile',
                        required=True,
                        metavar='DIR',
                        type=str)

    parser.add_argument('-v',
                        dest = "b_is_feet",
                        help='OPTIONAL: create vertical data in feet: Default=True',
                        required=False,
                        default=True,
                        metavar='T/F',
                        type=str2bool)
    

    # hard coded constants
    flt_mjr_axis = 4 # distance to buffer major axis - lambert units - meters
    int_resolution = 1 # requested resolution in lambert units - meters
    flt_xs_sample_interval = 1 # interval to sample points along a line for cross section - crs units
    
    args = vars(parser.parse_args())
    
    str_mjr_axis_shp_path = args['str_mjr_axis_shp_path']
    str_deck_dem_path = args['str_deck_dem_path']
    str_output_dir = args['str_output_dir']
    b_is_feet = args['b_is_feet']

    
    fn_extract_deck_profile(str_mjr_axis_shp_path,
                            str_deck_dem_path,
                            str_output_dir,
                            b_is_feet,
                            flt_mjr_axis,
                            int_resolution,
                            flt_xs_sample_interval)
                            

    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    
    print('Compute Time: ' + str(time_pass))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~