#! /usr/bin/env python
"""
This script allows to sort Sentinel-1 data for homogeneous large-scale mapping.
"""

# import stdlib modules
import itertools

# some more libs for plotting and DB connection
import fiona
import geopandas as gpd

from shapely.ops import unary_union

# import internal modules
from ost.helpers.db import pgHandler
from ost.helpers import vector as vec

# script infos
__author__ = 'Andreas Vollrath'
__copyright__ = 'phi-lab, European Space Agency'

__license__ = 'GPL'
__version__ = '1.0'
__maintainer__ = 'Andreas Vollrath'
__email__ = ''
__status__ = 'Production'


def read_s1_inventory(inputfile):
    '''Reads a Sentinel-1 OST conform inventory shapefile into GeoDataFrame

    This function intends to transform different spatial formats in which
    inventory can be stored to transform into a geopandas GeoDataFrame
    that will be handled by all other methods.


    Args:
        inputfile (str or path): path to an OST compliant invetory shapefile

    Returns:
        GeoDataFrame ():
    '''

    if inputfile[-4:] == '.shp':
        print(' INFO: Importing Sentinel-1 inventory data from ESRI '
              ' shapefile:\n {}'.format(inputfile))
        column_names = ['id', 'identifier', 'polarisationmode',
                        'orbitdirection', 'acquisitiondate', 'relativeorbit',
                        'orbitnumber', 'producttype', 'slicenumber', 'size',
                        'beginposition', 'endposition',
                        'lastrelativeorbitnumber', 'lastorbitnumber', 'uuid',
                        'platformidentifier', 'missiondatatakeid',
                        'swathidentifier', 'ingestiondate',
                        'sensoroperationalmode', 'geometry']

        out_frame = gpd.read_file(inputfile)
        out_frame.columns = column_names

    elif inputfile[-7:] == '.sqlite':
        print(' INFO: Importing Sentinel-1 inventory data from spatialite '
              ' DB file:\n {}'.format(inputfile))
        # needs to be added
    else:
        print(' INFO: Importing Sentinel-1 inventory data from PostGreSQL DB '
              ' table:\n {}'.format(inputfile))
        db_connect = pgHandler()
        sql = 'select * from {}'.format(inputfile)
        out_frame = gpd.GeoDataFrame.from_postgis(sql, db_connect.connection,
                                                  geom_col='geometry')

    if len(out_frame) >= 0:
        print(' INFO: Succesfully converted inventory data into a'
              ' GeoPandas Geo-Dataframe.')

    return out_frame


def _remove_double_entries(inventory_df):
    '''Removing acquisitions that appear twice in the inventory

    Sometimes acquisitions are processed more than once. The last 4 digits of
    the re-processed scene identifier then change. When searching for data,
    both scenes will be found, even though they are the same.

    This routine checks of such appearance and selects the latest product.

    Args:
        inventory_df (gdf): an OST compliant Sentinel-1 inventory GeoDataFrame

    Returns:
        inventory_df (gdf): the manipulated inventory GeodataFrame

    '''

    # filter footprint data frame for obit direction and polarisation &
    # get unqiue entries
    idx = inventory_df.groupby(
        inventory_df['identifier'].str.slice(0, 63))[
            'ingestiondate'].transform(max) == inventory_df['ingestiondate']

    # re-initialize GDF geometry due to groupby function
    crs = fiona.crs.from_epsg(4326)
    gdf = gpd.GeoDataFrame(inventory_df[idx], geometry='geometry', crs=crs)
    print(' INFO: {} frames remain after double entry removal'.format(
        len(inventory_df[idx])))
    return gdf


def _remove_outside_aoi(aoi_gdf, inventory_df):
    '''Removes scenes that are located outside the AOI

    The search routine works over a simplified representation of the AOI.
    This may then include acquistions that do not overlap with the AOI.

    In this step we sort out the scenes that are completely outside the
    actual AOI.

    Args:
        aoi_gdf (gdf): the aoi as an GeoDataFrame
        inventory_df (gdf): an OST compliant Sentinel-1 inventory GeoDataFrame

    Returns:
        inventory_df (gdf): the manipulated inventory GeodataFrame

    '''

    # get columns of input dataframe for later return function
    cols = inventory_df.columns

    # 1) get only intersecting footprints (double, since we do this before)
    inventory_df = gpd.sjoin(inventory_df, aoi_gdf,
                             how='inner', op='intersects')

    # if aoi  gdf has an id field we need to rename the changed id_left field
    if 'id_left' in inventory_df.columns.tolist():
        # rename id_left to id
        inventory_df.columns = [
            'id' if x == 'id_left' else x
            for x in inventory_df.columns.tolist()]

    return inventory_df[cols]


def _handle_equator_crossing(inventory_df):
    '''Adjustment of track number when crossing the equator

    OST relies on unique numbers of relative orbit. For ascending tracks
    crossing the equator the relative orbit will increase by 1.

    This routine checks for the appearance of such kind and unifies the
    relativeorbitnumbers so that the inventory is complinat with the
    subsequent batch processing routines of OST

    Args:
        inventory_df (gdf): an OST compliant Sentinel-1 inventory GeoDataFrame

    Returns:
        inventory_df (gdf): the manipulated inventory GeodataFrame

    '''

    # get the relativeorbitnumbers that change with equator crossing
    tracks = inventory_df.lastrelativeorbitnumber[
        inventory_df['relativeorbit'] != inventory_df[
            'lastrelativeorbitnumber']].unique().tolist()

    for track in tracks:

        # get dates
        dates = inventory_df.acquisitiondate[
            (inventory_df['relativeorbit'] == track)].unique()
        for date in dates:

            # ----------------------------------------------------
            # ### NEEDS TO BE ADDED THE CHECK
            # check if consecutive orbitnumers are from the same track
            # subdf = inventory_df[(inventory_df['acquisitiondate'] == date) &
            #                     (inventory_df['relativeorbit'] == track) |
            #                     (inventory_df['acquisitiondate'] == date) &
            #                     (inventory_df['relativeorbit'] ==
            #             str(int(track) - 1))].sort_values(['beginposition'])
            #
            # for row in subdf.iterrows():
            # ----------------------------------------------------

            # get index of
            idx = inventory_df[(inventory_df['acquisitiondate'] == date) &
                               (inventory_df['relativeorbit'] == track)].index

            # reset relative orbit number
            inventory_df.set_value(idx, 'relativeorbit', str(int(track) - 1))

    return inventory_df


def _exclude_marginal_tracks(aoi_gdf, inventory_df, area_reduce=0.1):
    '''
    This function takes the AOI and the footprint inventory
    and checks if any of the tracks are unnecessary, i.e.
    if the AOI can be covered by all the remaining tracks.

    The output will be a subset of the given dataframe,
    including only the relevant tracks.

    Args:
        aoi_gdf (gdf): the aoi as an GeoDataFrame
        inventory_df (gdf): an OST compliant Sentinel-1 inventory GeoDataFrame
        area_reduce (float): reduction of AOI by square degrees

    Returns:
        inventory_df (gdf): the manipulated inventory GeodataFrame

    '''

    # get Area of AOI
    aoi_area = aoi_gdf.area.sum()

    # create a list of tracks for that date (sometimes more than one)
    tracklist = inventory_df['relativeorbit'].unique()

    for track in tracklist:

        trackunion = inventory_df.geometry[
            inventory_df['relativeorbit'] != track].unary_union
        intersect_track = aoi_gdf.geometry.intersection(trackunion).area.sum()

        if intersect_track >= aoi_area - area_reduce:
            print(' INFO: excluding track {}'.format(track))
            inventory_refined = inventory_df[
                inventory_df['relativeorbit'] != track]

    # see if there is actually any marginal track
    try:
        inventory_df = inventory_refined
        print(' INFO: {} frames remain after non-AOI overlap'.format(
            len(inventory_df)))
    except NameError:
        pass
    else:
        print(' INFO: All tracks fully overlap the AOI. Not removing anything')
    return inventory_df


def _remove_incomplete_tracks(aoi_gdf, inventory_df):
    '''Removes incomplete tracks with respect to the AOI

    Sentinel-1 follows an operational acquisition scheme where .
    However, in some cases, a complete acquisition was not possible, which may
    result in only a partial overlap with the AOI. For subsequent batch
    processing this is unwanted, since it affects the length of the time-series
    within the AOI.

    This function excludes tracks that do not fully cross the AOI.

    Args:
        aoi_gdf (gdf): the aoi as an GeoDataFrame
        inventory_df (gdf): an OST compliant Sentinel-1 inventory GeoDataFrame

    Returns:
        inventory_df (gdf): the manipulated inventory GeodataFrame

    '''

    # define final output gdf
    out_frame = gpd.GeoDataFrame(columns=inventory_df.columns)

    # create a list of tracks for that date (sometimes more than one)
    tracklist = inventory_df['relativeorbit'].unique()

    for track in tracklist:

        # get area of AOI intersect for all acq.s of this track
        trackunion = inventory_df['geometry'][
            inventory_df['relativeorbit'] == track].unary_union
        intersect_track = aoi_gdf.geometry.intersection(trackunion).area.sum()

        # loop through dates
        for date in sorted(inventory_df['acquisitiondate'][
                inventory_df['relativeorbit'] == track].unique(),
                           reverse=False):

            gdf_date = inventory_df[(inventory_df['relativeorbit'] == track) &
                                    (inventory_df['acquisitiondate'] == date)]

            # get area of AOI intersect for all acq.s of this track
            date_union = gdf_date.geometry.unary_union
            intersect_date = aoi_gdf.geometry.intersection(
                date_union).area.sum()

            if intersect_track <= intersect_date + 0.15:
                out_frame = out_frame.append(gdf_date)

    print(' INFO: {} frames remain after removal of non-full AOI crossing'
          .format(len(out_frame)))
    return out_frame


def _handle_non_continous_swath(inventory_df):
    '''Removes incomplete tracks with respect to the AOI

    In some cases the AOI is covered by 2 different parts of the same track.
    OST assumes that acquisitions with the same "relative orbit" (i.e. track)
    should be merged. However, SNAP will fail when slices of acquisitions
    are missing in between. Therefore this routine renames the tracks into
    XXX_1, XXX_2, XXX_n, dependent on the number of segments.

    Args:
        inventory_df (gdf): an OST compliant Sentinel-1 inventory GeoDataFrame

    Returns:
        inventory_df (gdf): the manipulated inventory GeodataFrame

    '''

    tracks = inventory_df.lastrelativeorbitnumber.unique()
    inventory_df['slicenumber'] = inventory_df['slicenumber'].astype(int)

    for track in tracks:

        dates = inventory_df.acquisitiondate[
            inventory_df['relativeorbit'] == track].unique()

        for date in dates:

            subdf = inventory_df[(inventory_df['acquisitiondate'] == date) &
                                 (inventory_df['relativeorbit'] == track)
                                 ].sort_values('slicenumber')

            if (len(subdf) <= int(subdf.slicenumber.max()) -
                    int(subdf.slicenumber.min())):

                i = 1
                last_slice = int(subdf.slicenumber.min()) - 1

                for _, row in subdf.iterrows():

                    if int(row.slicenumber) - int(last_slice) > 1:
                        i += 1

                    uuid = row.uuid
                    new_id = '{}.{}'.format(row.relativeorbit, i)
                    idx = inventory_df[inventory_df['uuid'] == uuid].index
                    inventory_df.set_value(idx, 'relativeorbit', new_id)
                    last_slice = row.slicenumber

    return inventory_df


def _forward_search(aoi_gdf, inventory_df, area_reduce=0):
    '''
    This functions loops through the acquisition dates and
    identifies the time interval needed to create full coverages.
    '''

    # get AOI area
    aoi_area = aoi_gdf.area.sum()

    # initialize some stuff for subsequent for-loop
    intersect_area, i = 0, 0
    datelist = []
    gdf_union = None
    start_date = None
    out_frame = gpd.GeoDataFrame(columns=inventory_df.columns)

    # loop through dates
    for date in sorted(inventory_df['acquisitiondate'].unique(),
                       reverse=False):

        # set starting date for curent mosaic
        if start_date is None:
            start_date = date

            # ofr th emoment, just take the first mosaic
            if i != 0:
                break

        # create a list of tracks for that date (sometimes more than one)
        tracklist = inventory_df['relativeorbit'][
            (inventory_df['acquisitiondate'] == date)].unique()

        for track in tracklist:

            # get all footprints for each date
            gdf = inventory_df[(inventory_df['acquisitiondate'] == date) &
                               (inventory_df['relativeorbit'] == track)]

            # get a unified geometry for date/track combination
            union = gdf.geometry.unary_union

            # add to overall union and to out_frame
            out_frame = out_frame.append(gdf)

            # just for first loop
            if gdf_union is None:
                # create new overall union
                gdf_union = union
            else:
                # union of unified footprints and footprints before
                polys = [gdf_union, union]
                gdf_union = unary_union(polys)

            # get intersection with aoi and calculate area
            inter = aoi_gdf.geometry.intersection(gdf_union)
            intersect_area = inter.area.sum()

            # for the datelist, we reset some stuff for next mosaic
            if intersect_area >= aoi_area - area_reduce:
                datelist.append([start_date, date])
                start_date = None
                gdf_union = None

    return datelist, gpd.GeoDataFrame(out_frame, geometry='geometry')


def _backward_search(aoi_gdf, inventory_df, datelist, area_reduce=0):
    '''
    This function takes the footprint dataframe and the datelist
    created by the _forward_search function to sort out
    duplicate tracks apparent in the mosaics.
    It searches from the last acqusiition date backwards
    in order to assure minimum time gap between the acquisitions of
    different swaths.
    '''

    # get AOI area
    aoi_area = aoi_gdf.area.sum()

    # create empty dataFrame for output
    temp_df = gpd.GeoDataFrame(columns=inventory_df.columns)
    out_frame = gpd.GeoDataFrame(columns=inventory_df.columns)
    gdf_union, intersect_area = None, 0

    # sort the single full coverages from _forward_search
    for dates in datelist:

        # extract scenes for single mosaics
        gdf = inventory_df[(inventory_df['acquisitiondate'] <= dates[1]) &
                           (inventory_df['acquisitiondate'] >= dates[0])]

        # we create an emtpy list and fill with tracks used for the mosaic,
        # so they are not used twice
        included_tracks = []

        # loop through dates backwards
        for date in sorted(gdf['acquisitiondate'].unique(), reverse=True):

            # create a list of tracks for that date (sometimes more than one)
            tracklist = gdf['relativeorbit'][
                (gdf['acquisitiondate'] == date)].unique()

            for track in tracklist:

                # we want every track just once, so we check
                if track not in included_tracks:

                    included_tracks.append(track)

                    # get all footprints for each date and track
                    track_gdf = gdf[(gdf['acquisitiondate'] == date) &
                                    (gdf['relativeorbit'] == track)]

                    # re-initialize GDF due to groupby fucntion
                    track_gdf = gpd.GeoDataFrame(track_gdf,
                                                 geometry='geometry')

                    # get a unified geometry for date/track combination
                    union = track_gdf.geometry.unary_union

                    # add to overall union and to out_frame
                    temp_df = temp_df.append(track_gdf)

                    # just for first loop
                    if gdf_union is None:
                        # create new overall union
                        gdf_union = union
                    else:
                        # union of unified footprints and footprints before
                        polys = [gdf_union, union]
                        gdf_union = unary_union(polys)

                    # get intersection with aoi and calulate area
                    inter = aoi_gdf.geometry.intersection(gdf_union)
                    intersect_area = inter.area.sum()

                    # we break the loop if we found enough
                    if intersect_area >= aoi_area - area_reduce:

                        # cleanup scenes
                        out_frame = out_frame.append(temp_df)
                        temp_df = gpd.GeoDataFrame(
                            columns=inventory_df.columns)
                        gdf_union = None

                        # stop for loop
                        break

            # we break the loop if we found enough
            if intersect_area >= aoi_area - area_reduce:
                break

    return gpd.GeoDataFrame(out_frame, geometry='geometry',
                            crs={'init': 'epsg:4326', 'no_defs': True})


def search_refinement(aoi, inventory_df, inventory_dir,
                      exclude_marginal=True,
                      full_aoi_crossing=True,
                      mosaic_refine=True,
                      area_reduce=0.05, complete_coverage=True):
    '''A function to refine the Sentinel-1 search by certain criteria

    Args:
        aoi (WKT str):
        inventory_df (GeoDataFrame):
        inventory_dir (str or path):

    Returns:
        refined inventory (dictionary):
        coverages (dictionary):

    '''
    # creat AOI GeoDataframe and calulate area
    aoi_gdf = vec.wkt_to_gdf(aoi)
    aoi_area = aoi_gdf.area.sum()
    # get all polarisations apparent in the inventory
    pols = inventory_df['polarisationmode'].unique()

    # get orbit directions apparent in the inventory
    orbit_directions = inventory_df['orbitdirection'].unique()

    # create inventoryDict
    inventory_dict = {}
    coverage_dict = {}

    # loop through all possible combinations
    for pol, orb in itertools.product(pols, orbit_directions):

        print(' INFO: Coverage analysis for {} tracks in {} polarisation.'
              .format(orb, pol))

        # subset the footprint for orbit direction and polarisations
        inv_df_sorted = inventory_df[
            (inventory_df['polarisationmode'] == pol) &
            (inventory_df['orbitdirection'] == orb)]

        print(' INFO: {} frames for {} tracks in {} polarisation.'.format(
            len(inv_df_sorted), orb, pol))

        # calculate intersected area
        inter = aoi_gdf.geometry.intersection(inv_df_sorted.unary_union)
        intersect_area = inter.area.sum()

        # we do a first check if the scenes do not fully cover the AOI
        if (intersect_area <= aoi_area - area_reduce) and complete_coverage:
            print(' WARNING: Set of footprints does not fully cover AOI. ')

        # otherwise we go on
        else:

            # apply the different sorting steps
            inventory_refined = _remove_double_entries(inv_df_sorted)

            inventory_refined = _remove_outside_aoi(aoi_gdf, inventory_refined)

            if orb == 'ASCENDING':
                inventory_refined = _handle_equator_crossing(inventory_refined)

            # get number of tracks
            nr_of_tracks = len(inventory_refined.relativeorbit.unique())
            print(nr_of_tracks)
            if exclude_marginal is True and nr_of_tracks > 1:
                inventory_refined = _exclude_marginal_tracks(
                    aoi_gdf, inventory_refined, area_reduce)

            if full_aoi_crossing is True:
                inventory_refined = _remove_incomplete_tracks(
                    aoi_gdf, inventory_refined)

            inventory_refined = _handle_non_continous_swath(inventory_refined)

            if mosaic_refine is True:
                datelist, inventory_refined = _forward_search(
                    aoi_gdf, inventory_refined, area_reduce)
                inventory_refined = _backward_search(
                    aoi_gdf, inventory_refined, datelist, area_reduce)

            if len(inventory_refined) != 0:
                vec.inventory_to_shp(
                    inventory_refined, '{}/{}_{}_{}.shp'.format(
                        inventory_dir, len(datelist), orb, ''.join(pol.split())
                        )
                    )
                inventory_dict['{}_{}'.format(
                    orb, ''.join(pol.split()))] = inventory_refined
                coverage_dict['{}_{}'.format(
                    orb, ''.join(pol.split()))] = len(datelist)

            print(' INFO: Found {} full coverage mosaics.'
                  .format(len(datelist)))

    return inventory_dict, coverage_dict
