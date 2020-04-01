import csv
import os
import owslib
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
import xml
import pprint
import Levenshtein as lvn

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


def main():
    out_records = []
    c = 0
    in_fn = '/home/james/geocrud/wms_layers.csv'
    out_fn = '/home/james/geocrud/wms_layers_w_bbox.csv'

    # with open(out_fn, 'w') as outpf:
    #     my_writer = csv.writer(outpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
    #     my_writer.writerow(['c', 'title', 'url', 'wms_layer_for_record', 'bb'])
    #
    #     if os.path.exists(in_fn):
    #         with open(in_fn, 'r') as inpf:
    #             my_reader = csv.DictReader(inpf)
    #             for r in my_reader:
    #                 c += 1
    #                 title = r['title']
    #                 subjects = r['subjects']
    #                 url = r['url']
    #                 wms_layer_for_record = r['wms_layer_for_record']
    #                 only_1_choice = r['only_1_choice']
    #                 match_dist = r['match_dist']
    #                 wms_error = r['wms_error']
    #                 if wms_error == 'False':
    #                     #print('title: ', title)
    #                     #print('subjects: ', subjects)
    #                     #print('url: ', url)
    #                     #print('wms_layer_for_record: ', wms_layer_for_record)
    #                     #print('only_1_choice: ', only_1_choice)
    #                     #print('match_dist: ', match_dist)
    #                     #print('wms_error: ', wms_error)
    #                     bb = None
    #                     try:
    #                         wms = WebMapService(url, timeout=5)
    #                     except owslib.util.ServiceException as ex_owslib:
    #                         print('owslib exception: ', ex_owslib)
    #                     except requests.exceptions.ReadTimeout as ex_req_rdto:
    #                         print('Requests exception: ', ex_req_rdto)
    #                     except requests.exceptions.RequestException as ex_req_reqex:
    #                         print('Requests exception: ', ex_req_reqex)
    #                     else:
    #                         try:
    #                             lyr = wms[wms_layer_for_record]
    #                         except KeyError as ex:
    #                             pass
    #                             #print(str(ex))
    #                         else:
    #                             bb = wms[wms_layer_for_record].boundingBox
    #
    #                     if bb is not None:
    #                         my_writer.writerow([c, title, url, wms_layer_for_record, bb])

    c = 0

    all_srs = {}

    with open(out_fn, 'r') as inpf:
        my_reader = csv.DictReader(inpf)
        with open('/home/james/Desktop/bng_wms_bboxes.csv', 'w') as outpf:
            my_writer = csv.writer(outpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            my_writer.writerow(['id', 'title', 'url', 'lyr_name', 'xmin', 'ymin', 'xmax', 'ymax', 'srs'])
            for r in my_reader:
                c += 1
                id = int(r['c'])
                title = r['title']
                url = r['url']
                lyr_name = r['wms_layer_for_record']
                lyr_bbox = ((r['bb'])[1:-1]).split(',')
                xmin, ymin, xmax, ymax, srs = float(lyr_bbox[0]), float(lyr_bbox[1]), float(lyr_bbox[2]), float(lyr_bbox[3]), (lyr_bbox[4].replace("'", "")).strip()
                is_bng = False
                if srs is not None:
                    if srs == 'EPSG:27700':
                        is_bng = True

                if is_bng:
                    my_writer.writerow([id, title, url, lyr_name, xmin, ymin, xmax, ymax, srs])

if __name__ == "__main__":
    main()