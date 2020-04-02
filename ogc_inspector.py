import csv
import os
import uuid
import xml
from jinja2 import Environment, FileSystemLoader, select_autoescape
import owslib
from owslib.wms import WebMapService
from PIL import Image
import requests
from shapely.geometry import box


class UkRegions:
    def __init__(self):
        self.regions = {}
        self._populate()

    def _populate(self):
        uk_region_extents = {
            'northern_ireland': (-69.0, 466166.0, 177754.0, 610292.0),
            'scotland': (5513.0, 530253.0, 470323.0, 1220302.0),
            'england': (82672.0, 5338.0, 655605.0, 657534.0),
            'wales': (146612.0, 164586.0, 355313.0, 395984.0),
            'uk': (0.0, 0.0, 700000.0, 1300000.0)
        }

        for rgn in uk_region_extents:
            extents = uk_region_extents[rgn]
            bbox = box(extents[0], extents[1], extents[2], extents[3])
            self.regions[rgn] = bbox

    def get_region(self, region_name):
        geom = None
        if region_name in self.regions:
            geom = self.regions[region_name]

        return geom

    def as_dict(self):
        return self.regions


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
    else:
        status = "Image does not exist"

    return status


def interrogate_wms_layer(wms_url, wms_lyr_name, aoi_bbox):
    wms_error = False
    out_fn = None
    try:
        wms = WebMapService(wms_url)
    except (owslib.util.ServiceException,
            requests.RequestException, AttributeError, xml.etree.ElementTree.ParseError) as ex1:
        wms_error = True
        print(ex1)
    else:
        try:
            img = wms.getmap(
                layers=[wms_lyr_name],
                srs='EPSG:27700',
                bbox=aoi_bbox,
                size=(400, 400),
                format='image/png'
                )
        except owslib.util.ServiceException as ex2:
            wms_error = True
            print("Exception generated when making GetMap request:")
            print(ex2)
        else:
            out_fn = os.path.join(
                '/home/james/geocrud/wms_getmaps',
                "".join([str(uuid.uuid1().int), "_wms_map.png"])
            )
            with open(out_fn, 'wb') as outpf:
                outpf.write(img.read())

    return wms_error, out_fn


def fetch_bbox_for_wms_layers(in_fn='/home/james/geocrud/wms_layers.csv',
                              out_fn='/home/james/geocrud/wms_layers_w_bbox.csv'):
    c = 0

    with open(out_fn, 'w') as outpf:
        my_writer = csv.writer(outpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        my_writer.writerow(['c', 'title', 'url', 'wms_layer_for_record', 'bb'])

        if os.path.exists(in_fn):
            with open(in_fn, 'r') as inpf:
                my_reader = csv.DictReader(inpf)
                for r in my_reader:
                    c += 1
                    title = r['title']
                    subjects = r['subjects']
                    url = r['url']
                    wms_layer_for_record = r['wms_layer_for_record']
                    only_1_choice = r['only_1_choice']
                    match_dist = r['match_dist']
                    wms_error = r['wms_error']
                    if wms_error == 'False':
                        bb = None
                        try:
                            wms = WebMapService(url, timeout=5)
                        except owslib.util.ServiceException as ex_owslib:
                            print('owslib exception: ', ex_owslib)
                        except requests.exceptions.ReadTimeout as ex_req_rdto:
                            print('Requests exception: ', ex_req_rdto)
                        except requests.exceptions.RequestException as ex_req_reqex:
                            print('Requests exception: ', ex_req_reqex)
                        else:
                            try:
                                lyr = wms[wms_layer_for_record]
                            except KeyError as ex:
                                print(ex)
                            else:
                                bb = wms[wms_layer_for_record].boundingBox

                        if bb is not None:
                            my_writer.writerow([c, title, url, wms_layer_for_record, bb])


def interrogate_wms_layers(in_fn='/home/james/geocrud/wms_layers_w_bbox.csv'):
    out_records = []

    uk_regions = (UkRegions()).as_dict()
    uk = uk_regions['uk']

    c = 0
    all_srs = {}

    with open(in_fn, 'r') as inpf:
        my_reader = csv.DictReader(inpf)

        for r in my_reader:
            c += 1
            if c < 10:
                id = int(r['c'])
                title = r['title']
                url = r['url']
                lyr_name = r['wms_layer_for_record']
                lyr_bbox = ((r['bb'])[1:-1]).split(',')
                xmin = float(lyr_bbox[0])
                ymin = float(lyr_bbox[1])
                xmax = float(lyr_bbox[2])
                ymax = float(lyr_bbox[3])
                srs = (lyr_bbox[4].replace("'", "")).strip()
                print(c, "For Layer {}".format(title))
                is_bng = False
                if srs is not None:
                    if srs == 'EPSG:27700':
                        is_bng = True

                if is_bng:
                    wms_bbox = box(xmin, ymin, xmax, ymax)
                    eq_uk_std = False

                    print("\tBBox IS BNG".format(title))
                    print("\tBBox area = {} km2".format(round(wms_bbox.area/1000000.0), 2))

                    if wms_bbox.equals(uk_regions['uk']):
                        eq_uk_std = True
                        print("\t\tBBox IS equal to UK standard BBox".format(title))
                    else:
                        print("\t\tBBox IS NOT equal to UK standard BBox".format(title))
                        print("\t\tIt however intersects with the following regions:")

                        for rgn in uk_regions:
                            if rgn != 'uk':
                                rgn_g = uk_regions[rgn]
                                if wms_bbox.intersects(rgn_g):

                                    crude_comparision = round(((wms_bbox.area / rgn_g.area) * 100.0), 4)

                                    print("\t\t\t {} ({}%)".format(rgn, crude_comparision))

                    print("\tMaking GetMap() request using Layer BBox...")
                    wms_error, out_fn = interrogate_wms_layer(
                        wms_url=url,
                        wms_lyr_name=lyr_name,
                        aoi_bbox=wms_bbox.bounds
                    )

                    if not wms_error:
                        image_status = None
                        print("\t\tWrote GetMap image to {}".format(out_fn))
                        image_status = check_wms_map_image(out_fn)
                        print("\t\t{}".format(image_status))
                    else:
                        print("\t\tError making GetMap req/writing image")

                    out_records.append([
                        url,
                        lyr_name,
                        wms_error,
                        image_status,
                        out_fn
                    ])

                    print("\n")

    if len(out_records) > 0:
        env = Environment(
            loader=FileSystemLoader('/home/james/PycharmProjects/mapcatalogue/templates'),
            autoescape=select_autoescape(['html', 'xml'])
        )
        template = env.get_template('wms_validation_report_templ.html')
        with open('/home/james/geocrud/wms_getmaps/wms_validation_report.html', 'w') as outpf:
            outpf.write(template.render(my_list=out_records))


def main():
    interrogate_wms_layers()


if __name__ == "__main__":
    main()