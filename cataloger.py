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


def check_wms_map_image(fn):
    status = None
    logging.info('Checking image: %s', fn)

    if os.path.exists(fn):
        if os.path.getsize(fn) > 0:
            with Image.open(fn) as im:
                try:
                    im_colors_list = im.getcolors(im.size[0] * im.size[1])
                #except TypeError as ex:
                except Exception as ex:
                    logging.error("Exception raised when checking image:", exc_info=True)
                    status = "Invalid"
                else:
                    try:
                        number_of_cols_in_img = len(im_colors_list)
                    #except TypeError as ex:
                    except Exception as ex2:
                        logging.error("Exception raised when checking image:", exc_info=True)
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


def search_ogc_service_for_record_title(ogc_url, record_title, out_path, wms_timeout=5):
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
    wms_layer_for_record = None
    bbox = None
    bbox_srs = None
    match_dist = None
    wms_get_cap_error = False
    wms_get_map_error = False
    made_get_map_req = False
    num_layers = 0
    image_status = None
    out_image_fname = None

    logging.info('Looking for %s in WMS: %s', record_title, ogc_url)

    # TODO improve exception handling when instantiating WMS
    try:
        wms = WebMapService(ogc_url, timeout=wms_timeout)
    # except owslib.util.ServiceException as owslib_srv_ex:
    #     logging.error("Exception raised when instantiating WMS:", exc_info=True)
    #     wms_get_cap_error = True
    # except requests.exceptions.RequestException as requests_ex:
    #     logging.error("Exception raised when instantiating WMS:", exc_info=True)
    #     wms_get_cap_error = True
    # except AttributeError as attrib_error_ex:
    #     logging.error("Exception raised when instantiating WMS:", exc_info=True)
    #     wms_get_cap_error = True
    # except ValueError as value_error_ex:
    #     logging.error("Exception raised when instantiating WMS:", exc_info=True)
    #     wms_get_cap_error = True
    # except xml.etree.ElementTree.ParseError as etree_ex:
    #     logging.error("Exception raised when instantiating WMS:", exc_info=True)
    #     wms_get_cap_error = True
    except Exception as ex_instaniate_wms:
        logging.error("Exception raised when instantiating WMS:", exc_info=True)
        wms_get_cap_error = True
    else:
        logging.info('WMS WAS instantiated OK')
        logging.info('Searching WMS layer that matches record_title: %s', record_title)
        min_l_dist = 1000000
        matched_layer = None
        for wms_layer in wms.contents:
            num_layers += 1
            record_title_norm = record_title.lower()
            wms_layer_norm = wms_layer.lower()
            l_dist = Lvn.distance(record_title_norm, wms_layer_norm)
            if l_dist < min_l_dist:
                min_l_dist = l_dist
                matched_layer = wms_layer

        if matched_layer is not None:
            logging.info('Found matching WMS layer: %s', matched_layer)
            wms_layer_for_record = matched_layer
            wms_layer_bbox = wms[wms_layer_for_record].boundingBox
            bbox_srs = wms_layer_bbox[4]

            logging.info('Attempting to make WMS GetMap request based on layer BBox')
            if bbox_srs != '':
                match_dist = min_l_dist
                made_get_map_req = True
                # TODO improve exception handling when making WMS GetMap request
                try:
                    img = wms.getmap(
                        layers=[wms_layer_for_record],
                        srs=bbox_srs,
                        bbox=wms_layer_bbox[:4],
                        size=(400, 400),
                        format='image/png'
                        )
                # except owslib.util.ServiceException as owslib_srv_ex2:
                #     logging.error("Exception raised when making WMS GetMap Request:", exc_info=True)
                #     wms_get_map_error = True
                # except requests.exceptions.RequestException as requests_ex:
                #     logging.error("Exception raised when making WMS GetMap Request:", exc_info=True)
                #     wms_get_map_error = True
                except Exception as ex_wms_getmap:
                    logging.error("Exception raised when making WMS GetMap Request:", exc_info=True)
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

    return [wms_layer_for_record, bbox, bbox_srs, match_dist, only_1_choice, wms_get_cap_error, wms_get_map_error, made_get_map_req, image_status, out_image_fname]


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


# TODO need to grab record temporal information
def query_csw(params):
    out_records = []
    csw_url = params[0]
    start_pos = params[1]
    ogc_srv_type = params[2]
    out_path = params[3]
    try:
        csw = CatalogueServiceWeb(csw_url)
    except Exception as csw_ex:
    #except (owslib.util.ServiceException, requests.exceptions.RequestException) as csw_ex:
        logging.error("Exception raised when instantiating CSW:", exc_info=True)
    else:
        csw.getrecords2(startposition=start_pos)

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
                        if url is not None:
                            ogc_url_type = get_ogc_type(url)
                        if ogc_url_type is not None:
                            if ogc_url_type == ogc_srv_type:
                                # interogating WMS here
                                res = search_ogc_service_for_record_title(url, title, out_path)

                                wms_layer_for_record = res[0]
                                if wms_layer_for_record is not None:
                                    bbox = res[1]
                                    bbox_srs = res[2]
                                    match_dist = res[3]
                                    only_1_choice = res[4]
                                    wms_get_cap_error = res[5]
                                    wms_get_map_error = res[6]
                                    made_get_map = res[7]
                                    image_status = res[8]
                                    out_image_fname = res[9]
                                    out_records.append([
                                        title,
                                        subjects,
                                        url,
                                        wms_layer_for_record,
                                        only_1_choice,
                                        match_dist,
                                        bbox_srs,
                                        wms_get_cap_error,
                                        wms_get_map_error,
                                        made_get_map,
                                        image_status,
                                        out_image_fname
                                    ])

    return out_records


def search_csw_for_ogc_endpoints(out_path, csw_url, limit_count=0, ogc_srv_type='WMS:GetCapabilties'):
    limit_count = limit_count
    try:
        csw = CatalogueServiceWeb(csw_url)
    except Exception as csw_ex:
    #except (owslib.util.ServiceException, requests.exceptions.RequestException) as csw_ex:
        logging.error("Exception raised when instantiating CSW:", exc_info=True)
    else:
        resultset_size = int(csw.constraints['MaxRecordDefault'].values[0])
        logging.info('CSW MaxRecordDefault: %s', str(resultset_size))

        csw.getrecords2(startposition=0)
        num_records = csw.results['matches']
        logging.info('CSW Total Number of Matching Records: %s', str(num_records))

        limited = False
        if limit_count > 0:
            limited = True
            if limit_count < num_records:
                num_records = limit_count

        logging.info('CSW Records to retrieve: %s', str(num_records))

        jobs = [[csw_url, i, ogc_srv_type, out_path] for i in range(0, num_records, resultset_size)]

        pool = ThreadPoolExecutor(max_workers=10)

        with open(os.path.join(out_path, 'wms_layers.csv'), 'w') as outpf:
            my_writer = csv.writer(outpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            out_fields = [
                'title',  # 0
                'subjects',  # 1
                'url',  # 2
                'wms_layer_for_record',  # 3
                'only_1_choice',  # 4
                'match_dist',  # 5
                'bbox_srs',  # 6
                'wms_get_cap_error',  # 7
                'wms_get_map_error',  # 8
                'made_get_map_req',  # 9
                'image_status',  # 10
                'out_image_fname'  # 11
            ]
            my_writer.writerow(out_fields)

            for job in pool.map(query_csw, jobs):
                out_recs = job
                if len(out_recs) > 0:
                    for r in out_recs:
                        my_writer.writerow(r)


def generate_report(out_path):
    logging.info('Generating Report')
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


@click.command()
@click.argument('out_path', type=click.Path(exists=True))
@click.option('-n', '--max_records_to_search', default=0, type=int)
def build_wms_catalog(out_path, max_records_to_search):
    """
    i.e
    python cataloger.py /home/james/geocrud/wms_cataloger_out -max_records_to_search 500
    python cataloger.py /home/james/geocrud/wms_cataloger_out -n 500

    :param out_path: where output is written i.e. '/home/james/geocrud/wms_cataloger_out'
    :param max_records_to_search: limit the number of records in each CSW to be searched i.e. 500
    :return:
    """

    # first purge all files currently in the out_path folder so we start from afresh
    tidy(out_path)

    # setup logging
    logging.basicConfig(
        filename=os.path.join(out_path, 'mapcatalog.log'),
        filemode='w',
        format='%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(funcName)s - %(lineno)d - %(message)s',
        level=logging.DEBUG,
        datefmt='%m/%d/%Y %I:%M:%S %p'
    )

    logging.info('Starting')

    # get list of CSWs to be searched
    csw_list = []
    with open('data/csw_catalogue.csv', 'r') as inpf:
        my_reader = csv.DictReader(inpf)
        for r in my_reader:
            csw_list.append(r['csw'])

    # go through each CSW in turn and search for records that have associated OGC endpoints
    for csw_url in csw_list:
        logging.info('CSW to search is: %s', csw_url)
        search_csw_for_ogc_endpoints(
            out_path=out_path,
            csw_url=csw_url,
            limit_count=max_records_to_search,
            ogc_srv_type='WMS:GetCapabilties'
        )

    # generate an html report with embedded thumbnails of WMS GetMap request results
    generate_report(out_path)

    logging.info('Done')


if __name__ == "__main__":
    build_wms_catalog()

