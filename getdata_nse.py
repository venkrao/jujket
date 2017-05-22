#!/usr/bin/env python

import os
import sys
import requests
import re

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

def my_mkdir(path):
    if not os.path.isdir(path):
        os.mkdir(path)

    return os.path.abspath(path)

if __name__ == "__main__":
    dir_for_csv = my_mkdir("csvfiles")

    if not os.path.isfile("sec_bhavdata_full.csv"):
        # Then, get the sec_bhavdata_full.csv, which contains
        # all details of all the scrips
        all_scrips = Nse(url="https://www.nseindia.com/products/content/sec_bhavdata_full.csv")
        with open("sec_bhavdata_full.csv", "w") as fh:
            fh.write(all_scrips.get_response_text())

    with open("sec_bhavdata_full.csv", "r") as all_scrips:
        for line in all_scrips:
            scrip = line.split(",")[0]
            if scrip == "SYMBOL":
                continue
            print "working on %s" %scrip
            url_symbol_count = "https://www.nseindia.com/marketinfo/sym_map/symbolCount.jsp"
            payload = {"symbol": scrip }
            obj = Nse(url=url_symbol_count, params=payload)
            symbol_count = obj.get_response_text().strip()

            baseurl = "https://www.nseindia.com/products/dynaContent/common/productsSymbolMapping.jsp"

            scrip_csv = ("%s%s%s.csv" %(dir_for_csv, os.path.sep, scrip))

            get_params = {'symbol': scrip, 'segmentLink': 3,
                          'series': 'ALL', 'fromDate': "", 'toDate': "",
                          'dataType': 'PRICEVOLUMEDELIVERABLE', "symbolCount": str(symbol_count)}

            if os.path.isfile(scrip_csv):
                get_params['dateRange'] = 'day'
            else:
                get_params['dateRange'] = '3month'

            # Url to hit:
            # https://www.nseindia.com/products/dynaContent/common/productsSymbolMapping.jsp?symbol=sbin&segmentLink=3&symbolCount=1&series=ALL&dateRange=12month&fromDate=&toDate=&dataType=PRICEVOLUMEDELIVERABLE

            try:
                nse_obj = Nse(url=baseurl, params=get_params, headers={'Referer': 'https://www.nseindia.com/products/content/equities/equities/eq_security.htm'})
                response_text = nse_obj.get_response_text()
                regex = re.compile(r'csvContentDiv.*\>(.*)\</div\>', re.MULTILINE)
                csv_data = regex.findall(response_text)
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

                print "==========="
             
            except Exception as e:
                print "Failure for %s" %scrip
                print e.message