import csv
import xml
from owslib.csw import CatalogueServiceWeb
from owslib.fes import PropertyIsEqualTo
from owslib.wms import WebMapService
import Levenshtein as Lvn # https://rawgit.com/ztane/python-Levenshtein/master/docs/Levenshtein.html
import requests


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
    wms_timed_out = False
    error_during_wms_parsing = False

    num_layers = 0

    try:
        wms = WebMapService(ogc_url, timeout=wms_timeout)
        min_l_dist = 1000000
        matched_layer = None
        for wms_layer in wms.contents:
            num_layers += 1
            record_title_norm = record_title.lower()
            wms_layer_norm = wms_layer.lower()
            l_dist = Lvn.distance(record_title_norm, wms_layer_norm)
            if l_dist < min_l_dist:
                min_l_dist = l_dist
                matched_layer = wms_layer_norm
        if matched_layer is not None:
            wms_layer_for_record = matched_layer
            match_dist = min_l_dist
    except requests.RequestException as ex:
        wms_timed_out = True
    except AttributeError as ex:
        error_during_wms_parsing = True
    except xml.etree.ElementTree.ParseError as ex:
        error_during_wms_parsing = True

    if num_layers == 1:
        only_1_choice = True

    return [wms_layer_for_record, match_dist, only_1_choice, wms_timed_out, error_during_wms_parsing]


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


# TODO speed up using threading / multiprocessing?
#  https://blog.floydhub.com/multiprocessing-vs-threading-in-python-what-every-data-scientist-needs-to-know/

# TODO grab spatial and temporal (not always avail). So able to group by subject, spatial and temporal
def search_csw_for_ogc_endpoints(csw_url, search_term=None, limit_count=0, ogc_srv_type='WMS:GetCapabilties', csv_fname=None, debug=False):
    """
    query all exposed records in an OGC CSW and search for records that have WMS endpoints

    :param csw_url: OGC CSW GetCapabilties
    :param search_term: search term if we want to limit to other than all
    :param limit_count: restrict number of records
    :param ogc_srv_type: the type of OGC endpoint we care about defaults to WMS:GetCapabilties
    :param csv_fname: a CSV file into which outputs will be dumped
    :param debug: print stuff out during running
    :return: None
    """

    out_fields = ['title', 'subjects', 'url', 'wms_layer_for_record', 'only_1_choice', 'match_dist',
                  'wms_timed_out', 'error_during_wms_parsing']

    with open(csv_fname, 'w') as outpf:
        my_writer = csv.writer(outpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        my_writer.writerow(out_fields)
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
                                    res = search_ogc_service_for_record_title(url, title)
                                    wms_layer_for_record = res[0]
                                    match_dist = res[1]
                                    only_1_choice = res[2]
                                    wms_timed_out = res[3]
                                    error_during_wms_parsing = res[4]
                                    my_writer.writerow([
                                            title,
                                            subjects,
                                            url,
                                            wms_layer_for_record,
                                            only_1_choice,
                                            match_dist,
                                            wms_timed_out,
                                            error_during_wms_parsing
                                        ])
                                    if debug:
                                        print('title: ', title)
                                        print('subjects: ', subjects)
                                        print('ogc_url: ', url)
                                        print('wms_layer_for_record: ', wms_layer_for_record)
                                        print('only_1_choice: ', only_1_choice)
                                        print('match_dist: ', match_dist)
                                        print('wms_timed_out: ', wms_timed_out)
                                        print('error_during_wms_parsing: ', error_during_wms_parsing)
            start_pos += max_record_default


def main():
    # search_csw_for_ogc_endpoints(
    #     csw_url='https://ckan.publishing.service.gov.uk/csw?request=GetCapabilities&service=CSW&version=2.0.2',
    #     search_term='Greenspace',
    #     limit_count=1000,
    #     csv_fname='/home/james/geocrud/ogc_endpoints_greenspace.csv'
    # )

    # TODO: clickify?
    search_csw_for_ogc_endpoints(
        csw_url='https://ckan.publishing.service.gov.uk/csw?request=GetCapabilities&service=CSW&version=2.0.2',
        limit_count=200,
        ogc_srv_type='WMS:GetCapabilties',
        csv_fname='/home/james/geocrud/wms_layers.csv'
    )


if __name__ == "__main__":
    main()

