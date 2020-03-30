import csv
from owslib.csw import CatalogueServiceWeb
from owslib.fes import PropertyIsEqualTo, PropertyIsLike, BBox


def get_ogc_type(url):
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

# TODO use threading rather than multiprocessing
#  https://blog.floydhub.com/multiprocessing-vs-threading-in-python-what-every-data-scientist-needs-to-know/

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
                            ogc_urls.append([ogc_url_type, url])
                    if len(ogc_urls) > 0:
                        for u in ogc_urls:
                            out_records.append([r_idx, title, subjects, u[0], u[1]])
                    else:
                        out_records.append([r_idx, title, subjects, 'NotFound', 'NotFound'])
        start_pos += max_record_default

    return out_records


def build_catalog(csw_url, search_term=None, limit_count=0):
    ogc_endpoints = search_csw_for_ogc_endpoints(csw_url, search_term=search_term, limit_count=limit_count)

    with open('/home/james/Desktop/ogc_endpoints.csv', 'w') as outpf:
        my_writer = csv.writer(outpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        my_writer.writerow(['record_id', 'record_title', 'subjects', 'ogc_url_type', 'ogc_url'])
        for e in ogc_endpoints:
            my_writer.writerow([e[0], e[1], e[2], e[3], e[4]])


if __name__ == "__main__":
    # build_catalog(
    #     csw_url='https://ckan.publishing.service.gov.uk/csw?request=GetCapabilities&service=CSW&version=2.0.2',
    #     search_term='Greenspace',
    #     limit_count=1000
    # )

    build_catalog(
       csw_url='https://ckan.publishing.service.gov.uk/csw?request=GetCapabilities&service=CSW&version=2.0.2'
    )
