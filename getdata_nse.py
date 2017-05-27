#!/usr/bin/env python

import os
import sys
import requests
import re
import argparse
import datetime

class Nse:
    def __init__(self, url=None, params=None, headers=None):
      self.url = url
      self.response = requests.get(url, params=params, headers=headers)

    def get_response_object(self, url=None, params=None):
      return requests.get(url, params=params)

    def get_response_text(self):
      return self.response.text

    def get_status_code(self):
      return self.response.status_code

    def is404(self):
      return (404 == self.response.status_code)

    @staticmethod
    def get_symbol_count(scrip):
        url_symbol_count = "https://www.nseindia.com/marketinfo/sym_map/symbolCount.jsp"
        payload = {"symbol": scrip}
        obj = Nse(url=url_symbol_count, params=payload)
        symbol_count = obj.get_response_text().strip()

        return symbol_count

    @staticmethod
    def query_nse(query_parameters):
        baseurl = "https://www.nseindia.com/products/dynaContent/common/productsSymbolMapping.jsp"

        # Url to hit:
        # https://www.nseindia.com/products/dynaContent/common/productsSymbolMapping.jsp?symbol=sbin&segmentLink=3&symbolCount=1&series=ALL&dateRange=12month&fromDate=&toDate=&dataType=PRICEVOLUMEDELIVERABLE

        nse_obj = Nse(url=baseurl, params=query_parameters,
                      headers={'Referer': 'https://www.nseindia.com/products/content/equities/equities/eq_security.htm'})
        response_text = nse_obj.get_response_text()
        regex = re.compile(r'csvContentDiv.*\>(.*)\</div\>', re.MULTILINE)
        csv_data = regex.findall(response_text)
        return csv_data

    @staticmethod
    def get_all_scrips():
        all_scrips_csv = "all_scrips.csv"

        # get the sec_bhavdata_full.csv, which contains
        # all details of all the scrips
        all_scrips = Nse(url="https://www.nseindia.com/products/content/sec_bhavdata_full.csv")
        with open(all_scrips_csv, "w") as fh:
            fh.write(all_scrips.get_response_text())

        return all_scrips_csv

    @staticmethod
    def bootstrap_history(history_range, scrip, scrip_csv):
        overview = "overview.csv"
        trade_debut = False
        print "Bootstrap: working on %s" % scrip

        symbol_count = str(Nse.get_symbol_count(scrip))
        # It is only possible to fetch an year long data at once.
        for year in range(history_range["start"], history_range["end"] + 1):
            print "Year: %s" %year
            get_params = {'symbol': scrip, 'segmentLink': 3,
                          'series': 'ALL', 'fromDate': "01-01-%s" %year,
                          'toDate': "31-12-%s" %year,
                          'dataType': 'PRICEVOLUMEDELIVERABLE', "symbolCount": symbol_count,
                          "dateRange": "+"}

            try :
                csv_data = Nse.query_nse(get_params)
                if csv_data:
                    update_csv(scrip_csv, csv_data)
                    if not trade_debut:
                        with open(overview, "a") as fh:
                            fh.write("%s, %s\n" %(scrip, year))
                    trade_debut = True
                else:
                    print "No data for %s Year: %s)" % (scrip, year)
            except Exception as e:
                print "Error for %s Year: %s: %s" %(scrip, year, e.message)
        print "======================="

def my_mkdir(path):
    if not os.path.isdir(path):
        os.mkdir(path)

    return os.path.abspath(path)

def update_csv(scrip_csv, csv_data):
    with open(scrip_csv, "a") as csvfile:
        if os.path.isfile(scrip_csv):
            # If the file exists, ignore the first row.
            # which is just the table header
            for r in csv_data[0].replace(":", "\n").split("\n"):
                if "Average Price" in r:
                    continue
                csvfile.write(r + "\n")
        else:
            csvfile.write(csv_data[0].replace(":", "\n"))


if __name__ == "__main__":
    dir_for_csv = my_mkdir("csvfiles")
    all_scrips_csv = Nse.get_all_scrips()

    parser = argparse.ArgumentParser(description='Process various inputs to fetch data from NSE for various scrips.')
    parser.add_argument('--bootstrap', action="store_true", help="Bootstraps stocks data since 2007")
    parser.add_argument('--daily_data', action="store_true", help="Fetch daily data for all scrips")
    parser.add_argument('--year_start', dest='year_start', default=2007,
                        help="Bootstraps stocks data since this year. Default, 2007")
    parser.add_argument('--year_end', dest='year_end', action="store", default=datetime.datetime.now().year,
                        help="Bootstraps stocks data until this year. Default, current year.")

    args = parser.parse_args()

    if not args.daily_data or args.bootstrap:
        parser.print_help()

    """
    'symbol=EICHERMOT&segmentLink=3&symbolCount=1&series=ALL&dateRange=+&fromDate=01-01-2009&toDate=31-12-2009&dataType=PRICEVOLUMEDELIVERABLE'

    """
    if args.bootstrap:
        # If we are bootstrapping, then, we fetch the data from nse for the previous
        # 10 years. i.e since 2007.
        # I hope I'm not going to be prevented from doing so.

        # And record the trading since year for each scrip.
        if os.path.isfile("overview.csv"):
            os.remove("overview.csv")

        with open(all_scrips_csv, "r") as all_scrips:
            for line in all_scrips:
                scrip = line.split(",")[0]

                if scrip == "SYMBOL":
                    continue
                scrip_csv = ("%s%s%s.csv" % (dir_for_csv, os.path.sep, scrip))
                Nse.bootstrap_history({"start": args.year_start, "end": args.year_end}, scrip, scrip_csv)
    elif args.daily_data:
        print "Fetching daily data.."
        with open(all_scrips_csv, "r") as all_scrips:
            for line in all_scrips:
                scrip = line.split(",")[0]

                if scrip == "SYMBOL":
                    continue

                print "working on %s" % scrip
                scrip_csv = ("%s%s%s.csv" % (dir_for_csv, os.path.sep, scrip))
                symbol_count = str(Nse.get_symbol_count(scrip))

                get_params = {'symbol': scrip, 'segmentLink': 3,
                              'series': 'ALL', 'fromDate': "", 'toDate': "",
                              'dataType': 'PRICEVOLUMEDELIVERABLE', "symbolCount": symbol_count,
                              'dateRange': "day"}
                try :
                    csv_data = Nse.query_nse(get_params)
                    update_csv(scrip_csv, csv_data)
                except Exception as e:
                    print "Failed to fetch daily data for %s: %s" %(scrip, e.message)