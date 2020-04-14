import csv
import xml
import owslib
from owslib.csw import CatalogueServiceWeb
from owslib.wms import WebMapService
import Levenshtein as Lvn # https://rawgit.com/ztane/python-Levenshtein/master/docs/Levenshtein.html
import requests
from concurrent.futures import ThreadPoolExecutor


def search_ogc_service_for_record_title(ogc_url, record_title, wms_timeout=5):
    """
    given an ogc_url i.e. a WMS GetCapabilties, search the layers of that WMS
    for a given record_title and find the closest match based on Levenshtein distance

    :param ogc_url: an ogc url
    :param record_title: the layer we want to search for
    :param wms_timeout: how long to wait to retrieve from WMS
    :return:
    """
    only_1_choice = False
    wms_layer_for_record = None
    match_dist = None
    wms_error = False
    num_layers = 0

    try:
        wms = WebMapService(ogc_url, timeout=wms_timeout)
    except (owslib.util.ServiceException, requests.RequestException, AttributeError, xml.etree.ElementTree.ParseError) as ex:
        wms_error = True
    else:
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
            wms_layer_for_record = matched_layer
            match_dist = min_l_dist

    if num_layers == 1:
        only_1_choice = True

    return [wms_layer_for_record, match_dist, only_1_choice, wms_error]


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


# TODO just retrieve all WMSs rather than layers?
# TODO add logging
# TODO grab temporal and spatial elements
# TODO currently subjects is retrieved from CSW record but what about keywords from WMS itself?
def query_csw(params):
    out_records = []
    csw_url = params[0]
    start_pos = params[1]
    ogc_srv_type = params[2]
    csw = CatalogueServiceWeb(csw_url)
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
                            res = search_ogc_service_for_record_title(url, title)
                            wms_layer_for_record = res[0]
                            if wms_layer_for_record is not None:
                                match_dist = res[1]
                                only_1_choice = res[2]
                                wms_error = res[3]
                                out_records.append([
                                        title,
                                        subjects,
                                        url,
                                        wms_layer_for_record,
                                        only_1_choice,
                                        match_dist,
                                        wms_error
                                    ])

    return out_records


def search_csw_for_ogc_endpoints(out_csv_fname, csw_url, limit_count=0, ogc_srv_type='WMS:GetCapabilties', debug=False):
    limit_count = limit_count
    csw = CatalogueServiceWeb(csw_url)
    resultset_size = int(csw.constraints['MaxRecordDefault'].values[0])

    if debug:
        print('CSW MaxRecordDefault:{}'.format(str(resultset_size)))

    csw.getrecords2(startposition=0)
    num_records = csw.results['matches']

    if debug:
        print('CSW Total Number of Matching Records:{}'.format(str(num_records)))

    limited = False
    if limit_count > 0:
        limited = True
        if limit_count < num_records:
            num_records = limit_count

    if debug:
        print('num_records: {} (limited:{})'.format(str(num_records), limited))

    jobs = [[csw_url, i, ogc_srv_type] for i in range(0, num_records, resultset_size)]

    pool = ThreadPoolExecutor(max_workers=10)

    with open(out_csv_fname, 'w') as outpf:
        my_writer = csv.writer(outpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        out_fields = ['title', 'subjects', 'url', 'wms_layer_for_record', 'only_1_choice', 'match_dist', 'wms_error']
        my_writer.writerow(out_fields)

        job_n = 1
        for job in pool.map(query_csw, jobs):
            out_recs = job
            if len(out_recs) > 0:
                for r in out_recs:
                    my_writer.writerow(r)


def main():
    csw_list = []
    with open('data/csw_catalogue.csv', 'r') as inpf:
        my_reader = csv.DictReader(inpf)
        for r in my_reader:
            csw_list.append(r['csw'])

    for csw_url in csw_list:
        search_csw_for_ogc_endpoints(
            out_csv_fname='/home/james/Desktop/wms_layers.csv',
            csw_url=csw_url,
            limit_count=1000,
            ogc_srv_type='WMS:GetCapabilties'
        )


if __name__ == "__main__":
    main()

