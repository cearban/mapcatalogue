import csv
import os
from owslib.csw import CatalogueServiceWeb
import traceback


def validate_csw_sources():
    """
    data/csw_catalogue.csv is a list of CSWs taken from https://github.com/geopython/pycsw/wiki/Live-Deployments
    on 280420. This is a mixed bag in terms of which of these actually work. So we do some validation and
    create a 2nd CSW with valid CSW urls in it as data/csw_catalogue_valid.csv

    :return:
    """
    fn = 'data/csw_catalogue.csv'

    csw_ok = []
    csw_not_ok = []

    if os.path.exists(fn):
        with open(fn, 'r') as inpf:
            my_reader = csv.DictReader(inpf)
            with open(fn.replace('.csv', '_valid.csv'), 'w') as outpf:
                my_writer = csv.DictWriter(outpf, fieldnames=my_reader.fieldnames)
                my_writer.writeheader()
                for r in my_reader:
                    csw_name = r['name']
                    csw_url = r['url']
                    csw_loc = r['location']
                    print('csw_name: ', csw_name)
                    print('csw_url: ', csw_url)
                    print('csw_loc: ', csw_loc)
                    try:
                        csw = CatalogueServiceWeb(csw_url)
                    except Exception as ex:
                        print('Could not instantiate CSW')
                        print(traceback.print_exc())
                        csw_not_ok.append(csw_url)
                    else:
                        try:
                            csw.getrecords2()
                        except Exception as ex3:
                            print('Could not retrieve records')
                            print(traceback.print_exc())
                            csw_not_ok.append(csw_url)
                        else:
                            print('Records:')
                            c = 1
                            for rec in csw.records:
                                csw_rec = csw.records[rec]
                                print(c, csw_rec.title)
                                c += 1
                            csw_ok.append(csw_url)
                            my_writer.writerow(r)

    print("CSW OK")
    c = 1
    for i in csw_ok:
        print(c, i)
        c += 1

    c = 1
    print("CSW NOT OK")
    for i in csw_not_ok:
        print(c, i)
        c += 1


def main():
    validate_csw_sources()


if __name__ == "__main__":
    main()




