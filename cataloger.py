import csv
from concurrent.futures import ThreadPoolExecutor
import os
import glob
import shutil
import uuid
import xml
from jinja2 import Environment, FileSystemLoader, select_autoescape
import Levenshtein as Lvn # https://rawgit.com/ztane/python-Levenshtein/master/docs/Levenshtein.html
import owslib
from owslib.csw import CatalogueServiceWeb
from owslib.wms import WebMapService
from pyproj import Transformer
from PIL import Image
import requests
import logging
import click
from click_option_group import optgroup, RequiredMutuallyExclusiveOptionGroup
from postgres import Postgres


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


def validate_bbox(src_bbox):
    valid_bbox = None
    if src_bbox[4] != '':
        src_srs = int((src_bbox[4]).split(':')[1])
        if src_srs == 27700:
            valid_bbox = src_bbox
        else:
            if src_srs in (3857, 4326):
                src_x_min, src_y_min, src_x_max, src_y_max = src_bbox[0], src_bbox[1], src_bbox[2], src_bbox[3]
                transformer = Transformer.from_crs(src_srs, 27700)
                bng_xy_min = transformer.transform(src_x_min, src_y_min)
                bng_xy_max = transformer.transform(src_x_max, src_y_max)
                valid_bbox = (bng_xy_min[0], bng_xy_min[1], bng_xy_max[0], bng_xy_max[1], 'EPSG:27700')

    return valid_bbox


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

# TODO capture addtional WMS layer metadata: accessconstraints since although use conditions might be missing from the
#  CSW record itself, this info might/seems to be present in the WMS layer point-of-access
# TODO seperate out searching for matching WMS layer from testing the WMS layer since the latter can be slow/fragile
def search_wms_for_csw_record_title(ogc_url, record_title, out_path, wms_timeout=30):
    """
    given an ogc_url i.e. a WMS GetCapabilties, search the layers of that WMS
    for a given record_title and find the closest match based on Levenshtein distance

    :param ogc_url: an ogc url
    :param record_title: the layer we want to search for
    :param out_path: where to write getmap images
    :param wms_timeout: how long to wait to retrieve from WMS
    :return:
    """
    only_1_choice = False
    wms_layer_for_record_name = None
    wms_layer_for_record_title = None
    wms_layer_bbox = None
    wms_layer_bbox_srs = None
    wms_layer_bbox_wgs84 = None
    match_dist = -1
    wms_get_cap_error = False
    wms_get_map_error = False
    made_get_map_req = False
    num_layers = 0
    image_status = None
    out_image_fname = None
    wms_layers = []  # list to hold wms layers that match the record title. Currently this will be max of 1

    logging.info('Looking for %s in WMS: %s', record_title, ogc_url)

    try:
        wms = WebMapService(ogc_url, timeout=wms_timeout)
    # TODO improve caught exception specifity
    except Exception:
        logging.exception("Exception raised when instantiating WMS.")
        wms_get_cap_error = True
    else:
        logging.info('WMS WAS instantiated OK')
        logging.info('Searching WMS layer that matches record_title: %s', record_title)
        min_l_dist = 1000000

        matched_wms_layer_name = None
        matched_wms_layer_title = None

        for wms_layer in wms.contents:
            # wms layer <Name> is machine-to-machine layer identifier
            wms_layer_name = wms[wms_layer].name
            # Note: in owslib wms_layer key above is layer <Name>

            # wms layer <Title> is human readable layer identifier
            # <Title> is mandatory
            wms_layer_title = wms[wms_layer].title

            # NOTE:
            # If layer has <Title> ONLY (no <Name>) then layer is a category title with sub-layers. It itself cannot be
            #  requested in a GetMap request
            # If layer has <Title> AND <Name> it is a "named layer" that can be requested in a GetMap request

            # grab wms <BoundingBox> for the layer, this is 5 item tuple, srs is last item
            wms_layer_bbox = wms[wms_layer].boundingBox
            wms_layer_bbox_srs = wms_layer_bbox[4]

            # grab wms <Ex_GeographicBoundingBox> for the layer, this is 4 item tuple, srs implicit
            wms_layer_bbox_wgs84 = wms[wms_layer].boundingBoxWGS84
            num_layers += 1
            record_title_norm = record_title.lower()

            # match on WMS layer title, <Title> is human readable str
            wms_layer_title_norm = wms_layer_title.lower()
            l_dist = Lvn.distance(record_title_norm, wms_layer_title_norm)
            if l_dist < min_l_dist:
                min_l_dist = l_dist
                matched_wms_layer_name = wms_layer_name
                matched_wms_layer_title = wms_layer_title

        if matched_wms_layer_name is not None:
            logging.info('Found matching WMS layer Name / Title: %s / %s', matched_wms_layer_name, matched_wms_layer_title)
            wms_layer_for_record_name = matched_wms_layer_name
            wms_layer_for_record_title = matched_wms_layer_title
            # TODO can we obtain scale hints for the layer. Can this be used to construct better bbox?
            #  wms[wms_layer_for_record].scaleHint BUT not often populated

            logging.info('Attempting to make WMS GetMap request based on layer BBox')
            # TODO handle cases where crs in bbox is not provided
            if wms_layer_bbox_srs != '':
                # TODO why are there cases where bbox srs is empty?
                match_dist = min_l_dist
                made_get_map_req = True
                # TODO improve exception handling when making WMS GetMap request
                #  what is the exception?; why is it being raised; can/what can be done to prevent it
                try:
                    img = wms.getmap(
                        layers=[wms_layer_for_record_name],
                        srs=wms_layer_bbox_srs,
                        bbox=wms_layer_bbox[:4],
                        size=(400, 400),
                        format='image/png'
                        )
                # TODO improve caught exception specifity
                except Exception:
                    logging.exception("Exception raised when making WMS GetMap Request.")
                    wms_get_map_error = True
                else:
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
            else:
                logging.error("bbox_srs IS EMPTY:")

    if num_layers == 1:
        only_1_choice = True

    wms_layers.append([wms_layer_for_record_title, wms_layer_for_record_name, wms_layer_bbox, wms_layer_bbox_srs,
                       wms_layer_bbox_wgs84, match_dist, only_1_choice, wms_get_cap_error, wms_get_map_error,
                       made_get_map_req,
                       image_status, out_image_fname
                       ])

    return wms_layers


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


# TODO need to use reverse_geocode_wgs84_boundingbox() to geolocate the layer extent using layer`s wgs84 bbox
def retrieve_and_loop_through_csw_recordset(params):
    out_records = []
    csw_url = params[0]
    start_pos = params[1]
    resultset_size = params[2]
    ogc_srv_type = params[3]
    out_path = params[4]

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
            for rec in csw.records:
                r = None
                r = csw.records[rec]

                if r is not None:
                    # TODO grab other CSW record elements: identifier uuid, abstract and modified
                    # fetch / clean-up title
                    title = r.title
                    if title is not None:
                        title = title.replace("\n", "")

                    # fetch / clean-up subjects
                    # convert the list of subjects to a string. Sometimes the list has a None, so filter these off
                    subjects = r.subjects
                    if subjects is not None:
                        subjects = ', '.join(list(filter(None, subjects)))

                    # fetch / clean-up references
                    references = r.references
                    if references is not None:
                        ogc_urls = []
                        for ref in references:
                            url = ref['url']
                            ogc_url_type = None
                            logging.info('Found URL {} in record references'.format(url))
                            if url is not None:
                                ogc_url_type = get_ogc_type(url)
                            if ogc_url_type is not None:
                                if ogc_url_type == ogc_srv_type:
                                    wms_layers = None
                                    logging.info('URL ogc_url_type is: {} SO searching for Matching WMS Layer'.format(ogc_url_type))
                                    wms_layers = search_wms_for_csw_record_title(url, title, out_path)

                                    if wms_layers is not None:
                                        if len(wms_layers) > 0:
                                            for wms_layer in wms_layers:
                                                wms_layer_for_record_title = wms_layer[0]
                                                if wms_layer_for_record_title is not None:
                                                    wms_layer_for_record_name = wms_layer[1]
                                                    bbox = wms_layer[2]
                                                    bbox_srs = wms_layer[3]
                                                    bbox_wgs84 = wms_layer[4]
                                                    match_dist = wms_layer[5]
                                                    only_1_choice = wms_layer[6]
                                                    wms_get_cap_error = wms_layer[7]
                                                    wms_get_map_error = wms_layer[8]
                                                    made_get_map = wms_layer[9]
                                                    image_status = wms_layer[10]
                                                    out_image_fname = wms_layer[11]
                                                    out_records.append([
                                                        csw_url,
                                                        title,
                                                        subjects,
                                                        url,
                                                        wms_layer_for_record_title,
                                                        wms_layer_for_record_name,
                                                        only_1_choice,
                                                        match_dist,
                                                        bbox,
                                                        bbox_srs,
                                                        bbox_wgs84,
                                                        wms_get_cap_error,
                                                        wms_get_map_error,
                                                        made_get_map,
                                                        image_status,
                                                        out_image_fname
                                                    ])
                                        else:
                                            logging.info('Found ZERO WMS Layers in WMS {0}'.format(url))
                                else:
                                    logging.info('URL ogc_url_type is NONE-WMS OGC SERVICE: {} SO SKIPPING searching for record title'.format(ogc_url_type))
                            else:
                                logging.info('URL ogc_url_type is None i.e. NOT AN OGC SERVICE SO SKIPPING')

    return out_records


def search_csw_for_ogc_endpoints(out_path, csw_url, limit_count=0, ogc_srv_type='WMS:GetCapabilties', restrict_wms_layers_to_match=True):
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
            jobs = [[csw_url, start_pos, resultset_size, ogc_srv_type, out_path, restrict_wms_layers_to_match] for start_pos in range(0, num_records, resultset_size)]

            pool = ThreadPoolExecutor(max_workers=10)

            write_header = False
            if not os.path.exists(os.path.join(out_path, 'wms_layers.csv')):
                write_header = True

            with open(os.path.join(out_path, 'wms_layers.csv'), 'a') as outpf:
                my_writer = csv.writer(outpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
                out_fields = [
                    'csw_url',  # 0
                    'csw_record_title',  # 1
                    'csw_record_subjects',  # 2
                    'wms_url',  # 3
                    'wms_layer_for_record_title',  # 4
                    'wms_layer_for_record_name',  # 5
                    'only_1_choice',  # 6
                    'match_dist',  # 7
                    'bbox', # 8
                    'bbox_srs',  # 9
                    'bbox_wgs84', # 10
                    'wms_get_cap_error',  # 11
                    'wms_get_map_error',  # 12
                    'made_get_map_req',  # 13
                    'image_status',  # 14
                    'out_image_fname'  # 15
                ]
                # TODO stop writing the header in wms_layers.csv multiple times
                # TODO check that the output CSV is legit i.e. there are NOT trailing seperators
                if write_header:
                    my_writer.writerow(out_fields)

                for job in pool.map(retrieve_and_loop_through_csw_recordset, jobs):
                    out_recs = job
                    if len(out_recs) > 0:
                        for r in out_recs:
                            my_writer.writerow(r)


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

# TODO need to add a cmdline option to provide pg_conn_str
@click.command()
@optgroup.group('CSW sources', cls=RequiredMutuallyExclusiveOptionGroup, help='Source of CSW(s) to be searched')
@optgroup.option('-cswURL', 'csw_url', type=str, help='A single supplied CSW URL')
@optgroup.option('-csvFile', 'csv_file', type=click.Path(exists=True), help='One or more CSW URLs listed in a CSV file')
@click.option('-out_path', required=True, type=click.Path(exists=True), help='Path to write outputs to')
@click.option('-search_limit', default=0, type=int, help='Limit the number of CSW records searched')
@click.option('-log_level', default='debug', type=click.Choice(['debug', 'info']), help='Log Level')
@click.option('-createReport', 'create_report', default='y', type=click.Choice(['y', 'n']), help='Generate an HTML report')
def wms_layer_finder(**params):
    """Search CSW(s) for WMS layers"""
    csv_file = params['csv_file']
    csw_url = params['csw_url']
    out_path = params['out_path']
    search_limit = params['search_limit']
    log_level = params['log_level']
    create_report = params['create_report']
    csw_list = []

    if log_level == 'debug':
        print('csw_url: ', csw_url)
        print('csv_file: ', csv_file)
        print('out_path: ', out_path)
        print('search_limit: ', search_limit, type(search_limit))
        print('log_level: ', log_level)
        print('create_report: ', create_report)

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

    #first purge all files currently in the out_path folder so we start from afresh
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

    # go through each CSW in turn and search for records that have associated OGC endpoints
    for csw_url in csw_list:
        print('Searching CSW: ', csw_url)
        logging.info('CSW to search is: %s', csw_url)
        search_csw_for_ogc_endpoints(
            out_path=out_path,
            csw_url=csw_url,
            limit_count=search_limit,
            ogc_srv_type='WMS:GetCapabilties'
        )

    if create_report == 'y':
        # generate HTML report
        print('Creation of HTML report was requested. Generating...')
        logging.info('Generating Report')
        generate_report(out_path)

    logging.info('Done')


if __name__ == "__main__":
    wms_layer_finder()


