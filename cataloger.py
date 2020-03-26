import csv
from owslib.csw import CatalogueServiceWeb
from owslib.fes import PropertyIsEqualTo, PropertyIsLike, BBox


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
                # clean up the title
                title = r.title.replace("\n", "")
                # convert the list of subjects to a string. Sometimes the list has a None, so filter these off
                subjects = ', '.join(list(filter(None, r.subjects)))
                references = r.references
                ogc_urls = []
                for ref in references:
                    url = ref['url']
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


def main():
    u = 'https://ckan.publishing.service.gov.uk/csw?request=GetCapabilities&service=CSW&version=2.0.2'
    #ogc_endpoints = search_csw_for_ogc_endpoints(csw_url=u, search_term='Greenspace', limit_count=1000)
    ogc_endpoints = search_csw_for_ogc_endpoints(csw_url=u, limit_count=1000)

    with open('/home/james/Desktop/ogc_endpoints.csv', 'w') as outpf:
        my_writer = csv.writer(outpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        my_writer.writerow(['record_id', 'record_title', 'subjects', 'ogc_url_type', 'ogc_url'])
        for e in ogc_endpoints:
            my_writer.writerow([e[0], e[1], e[2], e[3], e[4]])


if __name__ == "__main__":
    main()
