from concurrent.futures import ThreadPoolExecutor
import csv
import glob
import logging
import os
import shutil
import uuid
import click
from click_option_group import optgroup, RequiredMutuallyExclusiveOptionGroup
from jinja2 import Environment, FileSystemLoader, select_autoescape
from owslib.csw import CatalogueServiceWeb
from owslib.wms import WebMapService
from postgres import Postgres
from PIL import Image
import Levenshtein as Lvn # https://rawgit.com/ztane/python-Levenshtein/master/docs/Levenshtein.html


def check_wms_map_image(fn):
    status = None
    logging.info('Checking image: %s', fn)

    if os.path.exists(fn):
        if os.path.getsize(fn) > 0:
            with Image.open(fn) as im:
                try:
                    im_colors_list = im.getcolors(im.size[0] * im.size[1])
                # TODO improve caught exception specifity
                except Exception:
                    logging.exception("Exception raised when checking image.")
                    status = "Invalid"
                else:
                    try:
                        number_of_cols_in_img = len(im_colors_list)
                    # TODO improve caught exception specifity
                    except Exception:
                        logging.exception("Exception raised when checking image.")
                        status = "Invalid"
                    else:
                        if number_of_cols_in_img > 1:
                            status = "seems to be populated"
                        else:
                            # other cause of this could be that data is not visible at this scale
                            status = "seems to all be background / no layer features in extent?"
        else:
            status = "seems to be a nosize img"
    else:
        status = "Image does not exist"

    logging.info('Image Status is: %s', status)
    return status


def generate_report(out_path):
    context = []
    csv_fname = os.path.join(out_path, 'wms_layers.csv')
    if os.path.exists(csv_fname):
        with open(csv_fname, 'r') as inpf:
            c = 1
            my_reader = csv.reader(inpf)
            for r in my_reader:
                if c > 1:
                    context.append(r)
                c += 1

    env = Environment(
        loader=FileSystemLoader('/home/james/PycharmProjects/mapcatalogue/templates'),
        autoescape=select_autoescape(['html', 'xml'])
    )
    template = env.get_template('wms_validation_report_templ.html')

    with open(os.path.join(out_path, 'wms_validation_report.html'), 'w') as outpf:
        outpf.write(template.render(my_list=context))


def get_ogc_type(url):
    """
    given an ogc url i.e. a GetCapabilities idenfity if this is a WMS; WFS etc and if it`s a
    GetMap or GetCapabilties

    :param url: an ogc url
    :return: an ogc_type i.e. WMS:GetCapabilties
    """
    ogc_type = None

    if 'wms' in url.lower():
        if 'request=getcapabilities' in url.lower():
            ogc_type = 'WMS:GetCapabilties'
        if 'request=getmap' in url.lower():
            ogc_type = 'WMS:GetMap'
    elif 'wcs' in url.lower():
        if 'request=describecoverage' in url.lower():
            ogc_type = 'WCS:DescribeCoverage'
        if 'request=getcoverage' in url.lower():
            ogc_type = 'WCS:GetCoverage'
    elif 'wfs' in url.lower():
        if 'request=getcapabilities' in url.lower():
            ogc_type = 'WFS:GetCapabilities'
        if 'request=getfeature' in url.lower():
            ogc_type = 'WFS:GetFeature'

    return ogc_type


def reverse_geocode_wgs84_boundingbox(pg_conn_str, wgs84_bbox):
    """
    Given a wgs84 boundingbox provided as a 4 element tuple, identify using a Natural Earth global countries dataset
    held in PostGIS db which countries the boundingbox intersects with.

    :param pg_conn_str: a SQLAlchemy type Pg connection string to the db holding the natural earth polygon data
    :param wgs84_bbox: 4 element tuple i.e. (-74.66163, 39.65041, -72.00061, 41.61214)
    :return: a list of dictionaries in form: [{'country': 'United States', 'continent': 'North America'}, ...]
    """
    geographies = None

    if isinstance(wgs84_bbox, tuple):
        if len(wgs84_bbox) == 4:
            try:
                db = Postgres(pg_conn_str)
            except Exception:
                logging.exception('Could not connect to Pg using provided pg_conn_str')
            else:
                sql = """
                SELECT DISTINCT b.name_long, b.continent
                FROM
                geocrud.natural_earth_world_map_units b
                where st_intersects(st_makeenvelope({bbox_xmin}, {bbox_ymin}, {bbox_xmax}, {bbox_ymax}, 4326), b.geom)
                """.format(
                    bbox_xmin=wgs84_bbox[0],
                    bbox_ymin=wgs84_bbox[1],
                    bbox_xmax=wgs84_bbox[2],
                    bbox_ymax=wgs84_bbox[3]
                )
                try:
                    rs = db.all(sql)
                except Exception:
                    logging.exception('Problem running query, maybe table geocrud.natural_earth_world_map_units does not exist')
                else:
                    geographies = []
                    for r in rs:
                        geographies.append({'country':r[0] , 'continent':r[1]})
        else:
            raise ValueError('wrong number of elements in wgs84_bbox tuple')
    else:
        raise TypeError('wgs84_bbox must be a 4 item tuple')

    return geographies


def search_wms_for_layer_matching_csw_record_title(wms, csw_record_title):
    """
    new streamlined version of search_wms_for_layer_matching_csw_record_title() that only retrieves wms layer
    elements when needed and now no longer makes as WMS GetMap request to test the WMS as we will do this in
    a seperate function

    :param wms: OWSLib WebMapService object
    :param csw_record_title: title of CSW record we want to search for a matching WMS layer for (matching on lyr title)
    :return: dict
    """
    found_match = False
    wms_get_cap_error = False  # track if we get an error when instantaiting OWSLib WMS obj
    matching_wms_layer_title = None  # to hold (mandatory) wms layer <Title> which is human readable layer identifier
    matching_wms_layer_name = None  # to hold wms layer <Name> which is machine-to-machine layer identifier
    matching_wms_layer_wgs84_bbox = None  # to hold WMS layer <Ex_GeographicBoundingBox>
    matching_wms_layer_projected_bbox = None  # to hold WMS layer <BoundingBox>

    # Note - Only if a WMS layer has both title and name is it a named-layer that GetMap requests can be issued to
    # retrieve. OWSLib only lists such named-layers. Layers that only have title are category layers that hold
    # other mappable layers. Layer title is the human readable description of the name so it is this that we attempt
    # to match to CSW record title

    only_1_choice = False  # track if WMS only exposes 1 layer
    exact_match = False  # track if there is exact match between CSW record title and WMS layer title
    match_dist = -1  # track levenshtein distance between CSW record title and matched WMS layer title
    min_levenshtein_dist = 1000000
    layers_checked_count = 0  # track number of WMS layers checked when matching to CSW record title

    # Capture accessconstraints from top level WMS identification metadata. Access constraints might not be present
    # in the CSW record itself but present in the WMS as point-of-access?
    wms_top_level_accessconstraints = wms.identification.accessconstraints

    # iterate through named layers in WMS and look for a layer whose title matches CSW record title
    # we cannot just look for CSW record title in wms.contents as wms.contents keys are WMS layer
    # record names and match is on title
    for i in wms.contents:
        wms_layer_name = wms[i].name  # WMS Layer <Name> Machine-Readable
        wms_layer_title = wms[i].title  # WMS Layer <Title> Human-Readable

        # if have an exact match we can shortcut having to go through rest of the layers
        if wms_layer_title == csw_record_title:
            matching_wms_layer_name = wms_layer_name
            exact_match = True
            break
        else:
            # otherwise we need to look through WMS layers and look for a layer whose title most closely matches the
            # CSW record title. Closeness of match is determined using Levenshtein distance (LD of 0 means 2 strings
            # are identical
            levenshtein_dist = Lvn.distance(csw_record_title, wms_layer_title)
            if levenshtein_dist < min_levenshtein_dist:
                min_levenshtein_dist = levenshtein_dist
                matching_wms_layer_name = wms_layer_name
        layers_checked_count += 1

    only_1_choice = False
    if layers_checked_count == 1:
        only_1_choice = True

    if exact_match:
        match_dist = 0
    else:
        match_dist = min_levenshtein_dist

    if matching_wms_layer_name is not None:
        if matching_wms_layer_name in list(wms.contents):
            found_match = True
            matching_wms_layer_title = wms.contents[matching_wms_layer_name].title
            matching_wms_layer_wgs84_bbox = wms.contents[matching_wms_layer_name].boundingBoxWGS84
            matching_wms_layer_projected_bbox = wms.contents[matching_wms_layer_name].boundingBox

    matched_wms_layer = {
        'found_match': found_match,
        'wms_get_cap_error': wms_get_cap_error,  # i.e. if this is False we were unable to search WMS
        'wms_top_level_accessconstraints': wms_top_level_accessconstraints,
        'matching_wms_layer_title': matching_wms_layer_title,
        'matching_wms_layer_name': matching_wms_layer_name,
        'matching_wms_layer_wgs84_bbox': matching_wms_layer_wgs84_bbox,
        'matching_wms_layer_projected_bbox': matching_wms_layer_projected_bbox,
        'only_1_choice': only_1_choice,
        'exact_match': exact_match,
        'match_dist': match_dist
    }

    return matched_wms_layer


# TODO use reverse_geocode_wgs84_boundingbox() to geocode the CSW record / WMS layer extent
# TODO improve / use a dictionary or namedtuple to store data since using a list is painful
def retrieve_and_loop_through_csw_recordset(params):
    out_records = []
    csw_url = params[0]
    start_pos = params[1]
    resultset_size = params[2]
    ogc_srv_type = params[3]
    out_path = params[4]
    restrict_wms_layers_to_match = params[5]
    test_wms_get_map = params[6]

    try:
        csw = CatalogueServiceWeb(csw_url)
    # TODO improve caught exception specifity
    except Exception:
        logging.exception("Exception raised when subsequentially instantiating CSW.")
    else:
        try:
            csw.getrecords2(startposition=start_pos, maxrecords=resultset_size)
        # TODO improve caught exception specifity
        except Exception:
            logging.exception("Exception raised when retrieving subsequent set of records from CSW.")
        else:
            logging.info('Processing records for CSW {}, from startposition: {}'.format(csw_url, str(start_pos)))
            for rec in csw.records:
                r = None
                r = csw.records[rec]

                if r is not None:
                    csw_rec_identifier = r.identifier
                    csw_rec_abstract = r.abstract
                    if csw_rec_abstract is not None:
                        csw_rec_abstract = csw_rec_abstract.replace("\n", "")
                    csw_rec_modified = r.modified
                    csw_rec_publisher = r.publisher

                    # fetch / clean-up title
                    csw_rec_title = r.title
                    if csw_rec_title is not None:
                        csw_rec_title = csw_rec_title.replace("\n", "")

                    # fetch / clean-up subjects
                    # convert the list of subjects to a string. Sometimes the list has a None, so filter these off
                    csw_rec_subjects = r.subjects
                    if csw_rec_subjects is not None:
                        csw_rec_subjects = ', '.join(list(filter(None, csw_rec_subjects)))

                    # fetch / clean-up references
                    csw_rec_references = r.references
                    if csw_rec_references is not None:
                        ogc_urls = []
                        # TODO what do we do if there is more than 1 WMS included in CSW references list?
                        for ref in csw_rec_references:
                            url = ref['url']
                            wms_url_domain = None
                            ogc_url_type = None
                            logging.info('Found URL {} in record references'.format(url))
                            if url is not None:
                                ogc_url_type = get_ogc_type(url)

                            if ogc_url_type is not None:
                                if ogc_url_type == ogc_srv_type:
                                    if url.startswith('http'):
                                        wms_url_domain = url.replace('https://', '').replace('http://', '').split('/')[0]
                                    wms_layers = None
                                    logging.info('URL ogc_url_type is: {0} SO searching WMS URL {1} for Matching WMS Layer'.format(
                                        ogc_url_type, url
                                    ))
                                    found_matching_wms_layer = False
                                    wms_get_cap_error = False

                                    # instantiate OWSLib WebMapService object
                                    try:
                                        wms = WebMapService(url, version='1.3.0', timeout=30)
                                    # TODO improve caught exception specifity
                                    except Exception:
                                        logging.exception("Exception raised when instantiating WMS.")
                                        wms_get_cap_error = True
                                    else:
                                        logging.info('WMS WAS instantiated OK')
                                        # search the WMS for a layer matching CSW record title
                                        matched_wms_layer = search_wms_for_layer_matching_csw_record_title(
                                            wms=wms,
                                            csw_record_title=csw_rec_title
                                        )
                                        if matched_wms_layer['found_match']:
                                            found_matching_wms_layer = True

                                        if found_matching_wms_layer:
                                            wms_get_map_error = None
                                            made_get_map_req = None
                                            image_status = None
                                            out_image_fname = None

                                            wms_layer_for_record_title = matched_wms_layer['matching_wms_layer_title']
                                            logging.info('Found matching WMS Layer for CSW record in WMS, matched WMS layer title is'.format(wms_layer_for_record_title))
                                            wms_layer_for_record_name = matched_wms_layer['matching_wms_layer_name']
                                            wms_top_level_accessconstraints = matched_wms_layer['wms_top_level_accessconstraints']
                                            bbox_wgs84 = matched_wms_layer['matching_wms_layer_wgs84_bbox']
                                            bbox_projected = matched_wms_layer['matching_wms_layer_projected_bbox']
                                            match_dist = matched_wms_layer['match_dist']
                                            only_1_choice = matched_wms_layer['only_1_choice']

                                            if test_wms_get_map:
                                                #test i.e. do GetMap request for the layers from the WMS
                                                wms_get_map_error, made_get_map_req, image_status, out_image_fname = test_wms_layer(
                                                    wms=wms,
                                                    wms_layer_name=matched_wms_layer['matching_wms_layer_name'],
                                                    out_path=out_path,
                                                    request_wgs84_layer_extent=True,
                                                    request_projected_layer_extent=False,
                                                    request_custom_extent=False,
                                                    custom_extent_bbox=None
                                                )

                                            out_records.append([
                                                csw_url,
                                                csw_rec_identifier,
                                                csw_rec_publisher,
                                                csw_rec_title,
                                                csw_rec_subjects,
                                                csw_rec_abstract,
                                                csw_rec_modified,
                                                url,
                                                wms_url_domain,
                                                wms_layer_for_record_title,
                                                wms_layer_for_record_name,
                                                wms_top_level_accessconstraints,
                                                only_1_choice,
                                                match_dist,
                                                bbox_wgs84,
                                                bbox_projected,
                                                wms_get_cap_error,
                                                wms_get_map_error,
                                                made_get_map_req,
                                                image_status,
                                                out_image_fname
                                            ])
                                        else:
                                            logging.info('Found ZERO matching WMS Layers for CSW record in WMS {0}'.format(url))
                                else:
                                    logging.info('URL ogc_url_type is NONE-WMS OGC SERVICE: {} SO SKIPPING searching for record title'.format(ogc_url_type))
                            else:
                                logging.info('URL ogc_url_type is None i.e. NOT AN OGC SERVICE SO SKIPPING')

    return out_records


def search_csw_for_ogc_endpoints(out_path, csw_url, limit_count=0, ogc_srv_type='WMS:GetCapabilties', restrict_wms_layers_to_match=True, test_wms_get_map=True):
    limit_count = limit_count
    try:
        csw = CatalogueServiceWeb(csw_url)
    # TODO improve caught exception specifity
    except Exception:
        logging.exception("Exception raised when initially instantiating CSW.")
    else:
        # MaxRecordDefault Constraint under OperationsMetadata indicates maximum number of
        # records that can be returned per query. It may be obtained using:
        # resultset_size = int(csw.constraints['MaxRecordDefault'].values[0])
        # however it is not always available. By default OWSLib getrecords2() has maxrecords=10
        # so just go with this.
        resultset_size = 10
        logging.info('NOT using MaxRecordDefault CSW Constraint. Using default of 10')

        try:
            csw.getrecords2(startposition=0)
        # TODO improve caught exception specifity
        except Exception:
            logging.exception("Exception raised when retrieving initial records from CSW.")
        else:
            num_records = csw.results['matches']
            logging.info('CSW Total Number of Matching Records: %s', str(num_records))

            limited = False
            if limit_count > 0:
                limited = True
                if limit_count < num_records:
                    num_records = limit_count
                if limit_count < resultset_size:
                    resultset_size = limit_count

            logging.info('CSW Records to retrieve: %s', str(num_records))

            # create job list
            jobs = [[csw_url, start_pos, resultset_size, ogc_srv_type, out_path, restrict_wms_layers_to_match, test_wms_get_map] for start_pos in range(0, num_records, resultset_size)]

            pool = ThreadPoolExecutor(max_workers=10)

            write_header = False
            if not os.path.exists(os.path.join(out_path, 'wms_layers.csv')):
                write_header = True

            with open(os.path.join(out_path, 'wms_layers.csv'), 'a') as outpf:
                my_writer = csv.writer(outpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
                out_fields = [
                    'csw_url',  # 0
                    'csw_record_identifier',  # 1
                    'csw_record_publisher',  # 2
                    'csw_record_title',  # 3
                    'csw_record_subjects',  # 4
                    'csw_record_abstract',  # 5
                    'csw_record_modified',  # 6
                    'wms_url',  # 7
                    'wms_url_domain',  # 8
                    'wms_layer_for_record_title',  # 9
                    'wms_layer_for_record_name',  # 10
                    'wms_access_constraints',  # 11
                    'only_1_choice',  # 12
                    'match_dist',  # 13
                    'bbox_wgs84',  # 14
                    'bbox_projected',  # 15
                    'wms_get_cap_error',  # 16
                    'wms_get_map_error',  # 17
                    'made_get_map_req',  # 18
                    'image_status',  # 19
                    'out_image_fname'  # 20
                ]

                if write_header:
                    my_writer.writerow(out_fields)

                for job in pool.map(retrieve_and_loop_through_csw_recordset, jobs):
                    out_recs = job
                    if len(out_recs) > 0:
                        for r in out_recs:
                            my_writer.writerow(r)


# TODO need to implement request_projected_layer_extent as alternative to issuing request for WGS84 map
# TODO need to implement request_custom_extent to make request for defined extent rather than whole layer extent
# TODO need to work out some way of working out a more refined bbox so we avoid
#  making request for very large e.g. all of world/all of UK etc extents when defaulting to layer extent
# TODO handle WMS Layer Style
def test_wms_layer(wms, wms_layer_name, out_path, request_wgs84_layer_extent=True, request_projected_layer_extent=False, request_custom_extent=False, custom_extent_bbox=None):
    """
    Test a WMS layer by making a GetMap request for it and then running image processing validation on the retrieved image

    Defaults to making a GetMap request that corresponds to the layer`s bbox in WGS84 since this can be guaranteed to be
    available for all layers

    returns following info regarding this testing

    wms_get_cap_error - True/False - WMS GetCapabilties error i.e. OWSLib could not instantiate WMS obj using wms_url
    made_get_map_req - True/False - Made GetMap request. Might be false if bbox problematic
    wms_get_map_error - True/False - WMS GetMap error generated when making WMS GetMap request
    image_status - string describing state of map image returned from the GetMap request and written to disk
    out_image_fname - full path to the map image returned from the GetMap request and written to disk

    :param wms: OWSLib WebMapService object
    :param wms_layer_name: name of WMS layer to request
    :param out_path: where to write image retrieved from WMS
    :param request_wgs84_layer_extent: request map corresponding to entire layer wgs84 bbox, defaults to True
    :param request_projected_layer_extent: request map corresponding to entire layer projected bbox, defaults to False
    :param request_custom_extent: request map corresponding to a custom bbox, defaults to False
    :param custom_extent_bbox: custom bbox
    :return:
    """
    wms = wms
    wms_layer_name = wms_layer_name
    out_path = out_path
    request_wgs84_layer_extent = request_wgs84_layer_extent
    request_projected_layer_extent = request_projected_layer_extent
    request_custom_extent = request_custom_extent
    custom_extent_bbox = custom_extent_bbox
    wms_get_map_error = False
    made_get_map_req = False
    image_status = None
    out_image_fname = None

    if request_wgs84_layer_extent:
        logging.info('Requested to test GetMap for Layer {0} WGS84 BBox'.format(wms_layer_name))
        if wms_layer_name in list(wms.contents):
            wms_layer_bbox = wms.contents[wms_layer_name].boundingBoxWGS84
            try:
                img = wms.getmap(
                    layers=[wms_layer_name],
                    srs='EPSG:4326',
                    bbox=wms_layer_bbox,
                    size=(400, 400),
                    format='image/png'
                )
            # TODO improve caught exception specifity
            except Exception:
                logging.exception("Exception raised when making WMS GetMap Request.")
                wms_get_map_error = True
            else:
                made_get_map_req = True
                logging.info('GetMap request made OK')
                logging.info('Writing map to temp image')
                out_image_fname = os.path.join(
                    out_path,
                    "".join([str(uuid.uuid1().int), "_wms_map.png"])
                )
                with open(out_image_fname, 'wb') as outpf:
                    outpf.write(img.read())

                if os.path.exists(out_image_fname):
                    image_status = check_wms_map_image(out_image_fname)

    if request_projected_layer_extent:
        pass

    if request_custom_extent:
        if custom_extent_bbox is not None:
            pass
        else:
            print('Custom map extent requested but no custom extent provided')

    return wms_get_map_error, made_get_map_req, image_status, out_image_fname


def tidy(path, skip_files=None):
    """
    purge all items in a folder

    :param path: path to a folder
    :param skip_files:
    :return: None
    """

    if skip_files is None:
        for f in glob.glob(os.path.join(path, "*")):
            try:
                os.remove(f)
            except OSError:
                shutil.rmtree(f)
    else:
        for f in glob.glob(os.path.join(path, "*")):
            if f not in skip_files:
                try:
                    os.remove(f)
                except OSError:
                    shutil.rmtree(f)


@click.command()
@optgroup.group('CSW sources', cls=RequiredMutuallyExclusiveOptionGroup, help='Source of CSW(s) to be searched')
@optgroup.option('-cswURL', 'csw_url', type=str, help='A single supplied CSW URL')
@optgroup.option('-csvFile', 'csv_file', type=click.Path(exists=True), help='One or more CSW URLs listed in a CSV file')
@click.option('-out_path', required=True, type=click.Path(exists=True), help='Path to write outputs to')
@click.option('-search_limit', default=0, type=int, help='Limit the number of CSW records searched')
@click.option('-log_level', default='debug', type=click.Choice(['debug', 'info']), help='Log Level')
@click.option('-createReport', 'create_report', default='y', type=click.Choice(['y', 'n']), help='Generate an HTML report')
@click.option('-geocoder_db_conn_str', type=str, help='(Geocoder) Pg connection string for db holding Natural Earth World Map Units polygons')
@click.option('-test_wms_get_map', default='y', type=click.Choice(['y', 'n']), help='Issue GetMap req to WMS Layers & valiadate image')
def wms_layer_finder(**params):
    """Searching CSW(s) for WMS layers. Using WMS Version 1.3.0. Using WGS84 BBox to test WMS returns map image."""
    csv_file = params['csv_file']
    csw_url = params['csw_url']
    out_path = params['out_path']
    search_limit = params['search_limit']
    log_level = params['log_level']
    create_report = params['create_report']
    geocoder_db_conn_str = params['geocoder_db_conn_str']
    test_wms_get_map = params['test_wms_get_map']
    csw_list = []

    if log_level == 'debug':
        print('csw_url: ', csw_url)
        print('csv_file: ', csv_file)
        print('out_path: ', out_path)
        print('search_limit: ', search_limit, type(search_limit))
        print('log_level: ', log_level)
        print('create_report: ', create_report)
        print('geocoder_db_conn_str: ', geocoder_db_conn_str)

    if csv_file is not None:
        with open(csv_file, 'r') as input_file:
            my_reader = csv.DictReader(input_file)
            for r in my_reader:
                csw_list.append(r['url'])
        print('Found {} CSWs in specified CSV file'.format(str(len(csw_list))))
    else:
        print('Searching single CSW')
        csw_list.append(csw_url)

    if search_limit == 0:
        print('All records in CSW(s) will be searched')
    else:
        print('Limiting search to {} records in each CSW'.format(str(search_limit)))

    # first purge all files currently in the out_path folder so we start from afresh
    tidy(out_path)

    # setup logging
    logging_level = None
    if log_level == 'debug':
        logging_level = logging.DEBUG
    elif log_level == 'info':
        logging_level = logging.INFO

    logging.basicConfig(
        filename=os.path.join(out_path, 'mapcatalog.log'),
        filemode='w',
        format='%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(funcName)s - %(lineno)d - %(message)s',
        level=logging_level
    )

    logging.info('Starting')

    have_geocoder = False
    if geocoder_db_conn_str is not None:
        have_geocoder = True
        logging.info('Have Pg geocoder(Natural Earth)')

    if test_wms_get_map == 'y':
        test_wms_get_map = True
    else:
        test_wms_get_map = False

    if log_level == 'debug':
        print('test_wms_get_map:', test_wms_get_map)

    # go through each CSW in turn and search for records that have associated OGC endpoints
    for csw_url in csw_list:
        print('Searching CSW: ', csw_url)
        logging.info('CSW to search is: %s', csw_url)
        search_csw_for_ogc_endpoints(
            out_path=out_path,
            csw_url=csw_url,
            limit_count=search_limit,
            ogc_srv_type='WMS:GetCapabilties',
            test_wms_get_map=test_wms_get_map
        )

    if create_report == 'y':
        # generate HTML report
        print('Creation of HTML report was requested. Generating...')
        logging.info('Generating Report')
        generate_report(out_path)

    logging.info('Done')


if __name__ == "__main__":
    wms_layer_finder()


