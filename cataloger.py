import csv
import os
from owslib.csw import CatalogueServiceWeb
from owslib.fes import PropertyIsEqualTo, PropertyIsLike, BBox
from owslib.wms import WebMapService
from pyproj import Transformer


def bbox_to_bng(src_crs, bbox, sub_bbox=False, buffer=100):
    """
    convert a bbox in some crs other than BNG to BNG

    :param src_crs:
    :param bbox:
    :param sub_bbox:
    :param buffer:
    :return:
    """
    bbox_bng = None
    if src_crs in (3857, 4326):
        src_x_min, src_y_min, src_x_max, src_y_max = bbox[0], bbox[1], bbox[2], bbox[3]
        transformer = Transformer.from_crs(src_crs, 27700)
        bng_xy_min = transformer.transform(src_x_min, src_y_min)
        bng_xy_max = transformer.transform(src_x_max, src_y_max)
        bbox_bng = (int(bng_xy_min[0]), int(bng_xy_min[1]), int(bng_xy_max[0]), int(bng_xy_max[1]))
        if sub_bbox:
            centroid_x = (bng_xy_max[0] + bng_xy_min[0]) / 2.0
            centroid_y = (bng_xy_max[1] + bng_xy_min[1]) / 2.0
            bbox_bng = (int(centroid_x), int(centroid_y), int(centroid_x + buffer), int(centroid_y + buffer))

    return bbox_bng


def fetch_web_map_services_from_csw(csw):
    """
    for the records of a given CSW obtain a list of WMS
    this is not really true?

    :param csw:
    :return: [[wms_title, wms_url]]
    """

    wms_l = []

    for rec in csw.records:
        r = csw.records[rec]
        title = r.title
        references = r.references

        for r in references:
            is_wms = False
            if 'wms' in (r['url']).lower():
                if 'getcapabilities' in (r['url']).lower():
                    is_wms = True

            if is_wms:
                wms_l.append([title, r['url']])

    return wms_l


def catalog_web_map_services(csw, query, refresh=False):
    """
    catalog a CSW

    :param csw:
    :param query:
    :param refresh:
    :return:
    """
    csv_catalog_fname = 'data/wms_catalog_urls.csv'

    do_stuff = False

    if os.path.exists(csv_catalog_fname):
        if refresh:
            do_stuff = True
    else:
        do_stuff = True

    if do_stuff:
        start_pos = 0
        max_record_default = int(csw.constraints['MaxRecordDefault'].values[0])
        csw.getrecords2(constraints=[query], startposition=0)
        n_matches = csw.results['matches']

        start_pos += max_record_default
        l = []
        wms_l = fetch_web_map_services_from_csw(csw)
        for i in wms_l:
            l.append(i)

        while start_pos < n_matches:
            csw.getrecords2(constraints=[query], startposition=start_pos)
            wms_l = fetch_web_map_services_from_csw(csw)
            for i in wms_l:
                l.append(i)
            start_pos += max_record_default

        with open('data/wms_catalog_urls.csv', 'w') as outpf:
            my_writer = csv.writer(outpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            my_writer.writerow(["title", "url"])
            for i in l:
                my_writer.writerow([i[0], i[1]])

    return csv_catalog_fname


def interrogate_wms_layer(wms_url, wms_lyr_name):
    """
    Interrogate/Test a WMS by making a GetMap request for an AOI of it`s buffered center

    :param wms_url:
    :param wms_lyr_name:
    :return:
    """
    wms = WebMapService(wms_url)

    sample_bbox = bb_bng = bbox_to_bng(
        3857,
        wms[wms_lyr_name].boundingBox,
        sub_bbox=True,
        buffer=5000
    )

    out_fn = '/home/james/geocrud/wms_getmaps/wms_map.png'

    with open(out_fn, 'wb') as outpf:
        img = wms.getmap(
            layers=[wms_lyr_name],
            srs='EPSG:27700',
            bbox=sample_bbox,
            size=(400, 400),
            format='image/png'
            )
        outpf.write(img.read())


def main():
    # data.gov.uk csw endpoint
    csw_url = 'https://ckan.publishing.service.gov.uk/csw?request=GetCapabilities&service=CSW&version=2.0.2'
    csw = CatalogueServiceWeb(csw_url)

    # search for Greenspace
    my_query = PropertyIsEqualTo('csw:AnyText', 'Greenspace')
    wms_catalog_fn = catalog_web_map_services(csw, my_query)

    wms_url = None
    wms_lyr_name = None

    # 1. build up the catalog of WMSs
    if os.path.exists(wms_catalog_fn):
        with open(wms_catalog_fn, 'r') as inpf:
            my_reader = csv.DictReader(inpf)
            for r in my_reader:
                if (r["title"]).startswith('St Albans'):
                    wms_title = r["title"]
                    wms_url = r['url']
                    wms = WebMapService(wms_url)
                    for l in wms.contents:
                        if l == 'Polling_Districts':
                            wms_lyr_name = l

    # 2. interrogate a WMS in the catalogue
    if (wms_url is not None) and (wms_lyr_name is not None):
        print(wms_title)
        print(wms_url)
        print(wms.version)
        print(wms.identification.title)
        print(wms.identification.abstract)
        print(wms_lyr_name)
        interrogate_wms_layer(wms_url, wms_lyr_name)


if __name__ == "__main__":
    main()
