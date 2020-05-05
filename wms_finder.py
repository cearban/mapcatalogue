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

import cataloger as ctlg


def validate_getmap_req(wms_url, wms_layer, aoi_bbox, srs, out_path, wms_timeout=30):
    wms_get_cap_error = False
    wms_get_map_error = False
    made_get_map_req = False
    out_image_fname = None
    image_status = None

    try:
        wms = WebMapService(wms_url, timeout=wms_timeout)
    # TODO improve caught exception specifity
    except Exception:
        logging.exception("Exception raised when instantiating WMS.")
        wms_get_cap_error = True
    else:
        logging.info('Attempting to make WMS GetMap request based on layer BBox')
        made_get_map_req = True
        try:
            img = wms.getmap(
                layers=[wms_layer],
                srs=srs,
                bbox=aoi_bbox,
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
                image_status = ctlg.check_wms_map_image(out_image_fname)

    return {
        'wms_get_cap_error': wms_get_cap_error,
        'wms_get_map_error': wms_get_map_error,
        'made_get_map_req': made_get_map_req,
        'out_image_fname': out_image_fname,
        'image_status': image_status
    }


def retrieve_wms_layers(wms_url, csw_url, record_title, wms_timeout=30):
    wms_layer_name = None
    wms_layer_title = None
    wms_layer_bbox = None
    wms_layer_bbox_srs = None
    wms_layer_bbox_wgs84 = None
    wms_layers = []  # list to hold wms layers that match the record title. Currently this will be max of 1

    logging.info('Retrieving WMS layers from: {}'.format(wms_url))

    try:
        wms = WebMapService(wms_url, timeout=wms_timeout)
    # TODO improve caught exception specifity
    except Exception:
        logging.exception("Exception raised when instantiating WMS.")
        wms_get_cap_error = True
    else:
        logging.info('WMS WAS instantiated OK')
        logging.info('Iterating through WMS layer(s)')

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

            wms_layers.append([csw_url, record_title, wms_url, wms_layer_title, wms_layer_name, wms_layer_bbox, wms_layer_bbox_srs, wms_layer_bbox_wgs84])

    return wms_layers


def retrieve_and_loop_through_csw_recordset(params):
    csw_url = params[0]
    start_pos = params[1]
    resultset_size = params[2]
    ogc_srv_type = params[3]
    out_path = params[4]
    all_wms_layers = []

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
                                ogc_url_type = ctlg.get_ogc_type(url)
                            if ogc_url_type is not None:
                                if ogc_url_type == ogc_srv_type:
                                    wms_url = url
                                    wms_layers = None
                                    logging.info('URL ogc_url_type is: {} SO searching for Matching WMS Layer'.format(ogc_url_type))

                                    # TODO if we have already encountered a WMS before, here or in another thread, how
                                    #  do we avoid hitting the WMS again?

                                    all_wms_layers.append([csw_url, title, wms_url, None, None, None, None, None])

                                    # wms_layers = retrieve_wms_layers(wms_url, csw_url, record_title=title, wms_timeout=30)
                                    # if wms_layers is not None:
                                    #     if len(wms_layers) > 0:
                                    #         all_wms_layers += wms_layers

                                else:
                                    logging.info('URL ogc_url_type is NONE-WMS OGC SERVICE: {} SO SKIPPING searching for record title'.format(ogc_url_type))
                            else:
                                logging.info('URL ogc_url_type is None i.e. NOT AN OGC SERVICE SO SKIPPING')

    return all_wms_layers


def search_csw_for_ogc_endpoints(out_path, csw_url, limit_count=0, ogc_srv_type='WMS:GetCapabilties', restrict_wms_layers_to_match=True):
    limit_count = limit_count
    all_retrieved_wms_layers_in_csw = []
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
            jobs = [[csw_url, start_pos, resultset_size, ogc_srv_type, out_path] for start_pos in range(0, num_records, resultset_size)]

            pool = ThreadPoolExecutor(max_workers=10)

            for job in pool.map(retrieve_and_loop_through_csw_recordset, jobs):
                retrieved_wms_layers = job
                if len(retrieved_wms_layers) > 0:
                    all_retrieved_wms_layers_in_csw += retrieved_wms_layers

            # write to CSV. I assume (OK, I don`t know/understand if we try to do this inside
            # the above for loop we could create problems where 2 or more threads try to open/write to
            # the same file?. Doing it this will way eat memory? (again I don`t really understand) though
            if os.path.exists(os.path.join(out_path, 'just_wms_layers.csv')):
                with open(os.path.join(out_path, 'just_wms_layers.csv'), 'a') as outpf:
                    my_writer = csv.writer(outpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
                    for wms_layer in all_retrieved_wms_layers_in_csw:
                        my_writer.writerow(wms_layer)


def validate_outputs(out_path):
    unq_wms_urls = []
    duplicates = []
    print('Validating Outputs...')
    fn = os.path.join(out_path, 'just_wms_layers.csv')
    if os.path.exists(fn):
        with open(fn, 'r') as inpf:
            my_reader = csv.DictReader(inpf)
            for r in my_reader:
                wms_url = r['wms_url']
                if wms_url not in unq_wms_urls:
                    unq_wms_urls.append(wms_url)
                else:
                    if wms_url not in duplicates:
                        duplicates.append(wms_url)

    if len(duplicates) > 0:
        print('Duplicate WMS URLs are present:')
        for i in sorted(duplicates):
            print(i)


@click.command()
@optgroup.group('CSW sources', cls=RequiredMutuallyExclusiveOptionGroup, help='Source of CSW(s) to be searched')
@optgroup.option('-cswURL', 'csw_url', type=str, help='A single supplied CSW URL')
@optgroup.option('-csvFile', 'csv_file', type=click.Path(exists=True), help='One or more CSW URLs listed in a CSV file')
@click.option('-out_path', required=True, type=click.Path(exists=True), help='Path to write outputs to')
@click.option('-search_limit', default=0, type=int, help='Limit the number of CSW records searched')
@click.option('-log_level', default='debug', type=click.Choice(['debug', 'info']), help='Log Level')
def wms_layer_finder(**params):
    """Search CSW(s) for WMS layers"""
    csv_file = params['csv_file']
    csw_url = params['csw_url']
    out_path = params['out_path']
    search_limit = params['search_limit']
    log_level = params['log_level']
    csw_list = []

    if log_level == 'debug':
        print('csw_url: ', csw_url)
        print('csv_file: ', csv_file)
        print('out_path: ', out_path)
        print('search_limit: ', search_limit, type(search_limit))
        print('log_level: ', log_level)

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

    # setup logging
    logging_level = None
    if log_level == 'debug':
        logging_level = logging.DEBUG
    elif log_level == 'info':
        logging_level = logging.INFO

    logging.basicConfig(
        filename=os.path.join(out_path, 'wms_finder.log'),
        filemode='w',
        format='%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(funcName)s - %(lineno)d - %(message)s',
        level=logging_level
    )

    logging.info('Starting')

    csv_header = [
        'csw_url',
        'record_title',
        'wms_url',
        'wms_layer_title',
        'wms_layer_name',
        'wms_layer_bbox',
        'wms_layer_bbox_srs',
        'wms_layer_bbox_wgs84'
    ]

    # create the empty output CSV with header now
    with open(os.path.join(out_path, 'just_wms_layers.csv'), 'w') as outpf:
        my_writer = csv.writer(outpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        my_writer.writerow(csv_header)

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

    logging.info('Done')

    validate_outputs(out_path)


if __name__ == "__main__":
    wms_layer_finder()
