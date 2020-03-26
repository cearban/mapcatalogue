import csv
import os
from owslib.csw import CatalogueServiceWeb
from owslib.fes import PropertyIsEqualTo, PropertyIsLike, BBox
from owslib.wms import WebMapService
from pyproj import Transformer
import requests
import uuid
import owslib
import logging
import glob
from PIL import Image

# logging.basicConfig(
#     filename='/home/james/geocrud/wms_getmaps/wms_getmap_log.txt',
#     filemode='w',
#     level=logging.DEBUG,
#     format='%(levelname)s:%(message)s'
# )


class MyBbox:
    def __init__(self, xmin, ymin, xmax, ymax, srs):
        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax
        self.srs = srs

    def __eq__(self, other):
        return self.xmin == other.xmin and \
               self.ymin == other.ymin and \
               self.xmax == other.xmax and \
               self.ymax == other.ymax and \
               self.srs == other.srs


def sub_from_bbox(xmin, ymin, xmax, ymax, buffer=1000):
    centroid_x = (xmin + xmax) / 2.0
    centroid_y = (ymin + ymax) / 2.0
    bbox = (int(centroid_x), int(centroid_y), int(centroid_x + buffer), int(centroid_y + buffer))
    return bbox


def bbox_to_bng(src_crs, bbox):
    """
    convert a bbox in some crs other than BNG to BNG

    :param src_crs:
    :param bbox
    :return:
    """
    bbox_bng = None
    if src_crs in (3857, 4326):
        src_x_min, src_y_min, src_x_max, src_y_max = bbox[0], bbox[1], bbox[2], bbox[3]
        transformer = Transformer.from_crs(src_crs, 27700)
        bng_xy_min = transformer.transform(src_x_min, src_y_min)
        bng_xy_max = transformer.transform(src_x_max, src_y_max)
        bbox_bng = (int(bng_xy_min[0]), int(bng_xy_min[1]), int(bng_xy_max[0]), int(bng_xy_max[1]), 'EPSG:27700')

    return bbox_bng


def interrogate_wms_layer(wms_url, wms_lyr_name):
    """
    Interrogate/Test a WMS by making a GetMap request for an AOI of it`s buffered center

    :param wms_url:
    :param wms_lyr_name:
    :return:
    """
    wms = WebMapService(wms_url)

    lyr_bbox = wms[wms_lyr_name].boundingBox
    if lyr_bbox[4] == 'EPSG:3857':
        print("Converted layer BBOX from 3857 to 27700")
        lyr_bbox = bbox_to_bng(3857, lyr_bbox)

    uk_bbox = MyBbox(0.0, 0.0, 700000.0, 1300000.0, 'EPSG:27700')
    oth_bbox = MyBbox(lyr_bbox[0], lyr_bbox[1], lyr_bbox[2], lyr_bbox[3], lyr_bbox[4])

    if not oth_bbox == uk_bbox:
        print(wms_url)
        #print(wms.version)
        #print(wms.identification.title)
        #print(wms.identification.abstract)
        print(wms_lyr_name)
        #print(lyr_bbox)

        sub_bbox = sub_from_bbox(lyr_bbox[0], lyr_bbox[1], lyr_bbox[2], lyr_bbox[3], buffer=1000)
        #print(sub_bbox)
        out_fn = os.path.join(
            '/home/james/geocrud/wms_getmaps',
            "".join([str(uuid.uuid1().int), "_wms_map.png"])
        )

        with open(out_fn, 'wb') as outpf:
            try:
                img = wms.getmap(
                    layers=[wms_lyr_name],
                    srs='EPSG:27700',
                    bbox=sub_bbox,
                    size=(400, 400),
                    format='image/png'
                    )
                outpf.write(img.read())
                logging.info("For WMS {} layer {}, wrote map to {}".format(wms_url, wms_lyr_name, out_fn))
            except owslib.util.ServiceException as ex:
                print("Exception generated when making GetMap request:")
                print(ex)
                logging.error("For WMS {} layer {}, had problems writing map to {}".format(wms_url, wms_lyr_name, out_fn))


# def fetch_wms_layers_in_wms_catalogue():
#     # data.gov.uk csw endpoint
#     csw_url = 'https://ckan.publishing.service.gov.uk/csw?request=GetCapabilities&service=CSW&version=2.0.2'
#     csw = CatalogueServiceWeb(csw_url)
#
#     # search for Greenspace
#     my_query = PropertyIsEqualTo('csw:AnyText', 'Greenspace')
#     wms_catalog_fn = catalog_web_map_services(csw, my_query)
#
#     wms_layers = []
#
#     # 1. build up the catalog of WMSs
#     if os.path.exists(wms_catalog_fn):
#         with open(wms_catalog_fn, 'r') as inpf:
#             my_reader = csv.DictReader(inpf)
#             for r in my_reader:
#                 wms_title = r["title"]
#                 wms_url = r['url']
#
#                 try:
#                     # default timeout is 30
#                     wms = WebMapService(wms_url, timeout=5)
#                     #print('OK', wms_url)
#                     #print('wms has these layers:')
#                     print('OK', wms_url)
#                     for wms_lyr_name in wms.contents:
#                         wms_layers.append([wms_url, wms_lyr_name])
#                 except (requests.RequestException, AttributeError) as ex:
#                     print('Exception(s) raised with ', wms_url)
#                     print(ex)
#
#     with open('/home/james/geocrud/wms_getmaps/wms_list.csv', 'w') as outpf:
#         my_writer = csv.writer(outpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
#         my_writer.writerow(["wms_url", "lyr_name"])
#         for i in wms_layers:
#             my_writer.writerow([i[0], i[1]])


def interrogate_wms_layers(fn):
    sample_l = []
    if os.path.exists(fn):
        with open(fn, 'r') as inpf:
            my_reader = csv.DictReader(inpf)
            for r in my_reader:
                wms_url = r['wms_url']
                lyr_name = r['lyr_name']
                if wms_url not in sample_l:
                    sample_l.append(wms_url)
                    interrogate_wms_layer(wms_url, lyr_name)


def check_wms_map_image(fn):
    status = None

    if os.path.exists(fn):
        if os.path.getsize(fn) > 0:
            im = Image.open(fn)
            if len(im.getcolors()) > 1:
                status = "seems to be populated"
            else:
                status = "seems to all be background / no layer features in extent?"
        else:
            status = "seems to be a nosize img"

    return status


def get_ogc_type(url):
    ogc_type = None

    if 'wms' in url.lower():
        if 'getcapabilities' in url.lower():
            ogc_type = 'WMS:GetCapabilties'
        if 'getmap' in url.lower():
            ogc_type = 'WMS:GetMap'
    elif 'wcs' in url.lower():
        if 'describecoverage' in url.lower():
            ogc_type = 'WCS:DescribeCoverage'
        if 'getcoverage' in url.lower():
            ogc_type = 'WCS:GetCoverage'
    elif 'wfs' in url.lower():
        if 'getcapabilities' in url.lower():
            ogc_type = 'WFS:GetCapabilities'
        if 'getfeature' in url.lower():
            ogc_type = 'WFS:GetFeature'

    return ogc_type


# TODO grab keywords(subjects); spatial and temporal (not always avail). So able to group
#  records by theme, time and spatial extent
#   created, date, modified, temporal, subjects, bbox
def search_csw_for_ogc_endpoints(csw_url, search_term=None, limit_count=0):
    out_records = []
    csw = CatalogueServiceWeb(csw_url)
    retrieved_first_set = False
    record_count = 10000000
    limit = False
    if limit_count > 0:
        limit = True
        record_count = limit_count

    start_pos = 0
    max_record_default = int(csw.constraints['MaxRecordDefault'].values[0])
    r_idx = 0

    while start_pos < record_count:
        if search_term is None:
            csw.getrecords2(startposition=start_pos)
        else:
            csw_query = PropertyIsEqualTo('csw:AnyText', search_term)
            csw.getrecords2(constraints=[csw_query], startposition=start_pos)
        # we only know how many records there are when we have retrieved records for the first time
        if not retrieved_first_set:
            # TODO - check the logic here
            if not limit:
                record_count = csw.results['matches']
            else:
                # we are limiting
                # but the limit we have set might be much larger than actual record count
                if csw.results['matches'] < record_count:
                    record_count = csw.results['matches']
            retrieved_first_set = True

        for rec in csw.records:
            r = None
            if limit:
                if r_idx < record_count:
                    r = csw.records[rec]
            else:
                r = csw.records[rec]

            r_idx += 1

            if r is not None:
                references = r.references
                ogc_urls = []
                for ref in references:
                    url = ref['url']
                    ogc_url_type = get_ogc_type(url)
                    if ogc_url_type is not None:
                        ogc_urls.append([ogc_url_type, url])
                if len(ogc_urls) > 0:
                    for u in ogc_urls:
                        out_records.append([r_idx, r.title, u[0], u[1]])
                else:
                    out_records.append([r_idx, r.title, 'NotFound', 'NotFound'])
        start_pos += max_record_default

    return out_records


def main():
    u = 'https://ckan.publishing.service.gov.uk/csw?request=GetCapabilities&service=CSW&version=2.0.2'
    #ogc_endpoints = search_csw_for_ogc_endpoints(csw_url=u, search_term='Greenspace', limit_count=1000)
    ogc_endpoints = search_csw_for_ogc_endpoints(csw_url=u, limit_count=1000)

    with open('/home/james/Desktop/ogc_endpoints.csv', 'w') as outpf:
        my_writer = csv.writer(outpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        my_writer.writerow(['record_id', 'record_title', 'ogc_url_type', 'ogc_url'])
        for e in ogc_endpoints:
            my_writer.writerow([e[0], e[1], e[2], e[3]])


if __name__ == "__main__":
    main()
