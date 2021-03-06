from owslib.csw import CatalogueServiceWeb
from concurrent.futures import ThreadPoolExecutor
import csv


def fetch_titles_from_csw(job_params):
    csw_url = job_params[0]
    start_pos = job_params[1]
    csw = CatalogueServiceWeb(csw_url)
    csw.getrecords2(startposition=start_pos)
    titles = []
    for rec in csw.records:
        r = None
        r = csw.records[rec]
        if r is not None:
            title = r.title
            titles.append(title)

    return titles


def do_processing(threaded=True):
    start_pos = 0
    limit_count = 500
    record_count = 10000000
    all_titles = []

    if limit_count > 0:
        record_count = limit_count

    csw_url = 'https://ckan.publishing.service.gov.uk/csw?request=GetCapabilities&service=CSW&version=2.0.2'
    csw = CatalogueServiceWeb(csw_url)
    max_record_default = int(csw.constraints['MaxRecordDefault'].values[0])

    jobs = [[csw_url, i] for i in range(start_pos, record_count, max_record_default)]

    if threaded:
        print("Using Threading")
        pool = ThreadPoolExecutor(max_workers=10)
        for job in pool.map(fetch_titles_from_csw, jobs):
            titles_in_csw = job
            for i in titles_in_csw:
                all_titles.append(i)
    else:
        print("Threading disabled")
        for job in jobs:
            titles_in_csw = fetch_titles_from_csw(job)
            for i in titles_in_csw:
                all_titles.append(i)

    if threaded:
        out_fn = '/home/james/Desktop/threaded.csv'
    else:
        out_fn = '/home/james/Desktop/non_threaded.csv'

    with open(out_fn, 'w') as outpf:
        my_writer = csv.writer(outpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        my_writer.writerow(['record_id', 'title'])
        r_idx = 0
        for i in sorted(all_titles):
            r_idx += 1
            my_writer.writerow([r_idx, i])


def main():
    do_processing(threaded=False)


if __name__ == "__main__":
    main()

