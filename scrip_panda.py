#!/usr/bin/env python

"""
Raw data
https://www.nseindia.com/products/dynaContent/common/productsSymbolMapping.jsp?symbol=heritage+&segmentLink=3&symbolCount=1&series=ALL&dateRange=3month&fromDate=&toDate=&dataType=PRICEVOLUMEDELIVERABLE

"""
import pandas as pd
import sys, os, glob
import datetime
import argparse
import argparse

def price_increased(dataframe, percentage_change, output_csv, change_type):
    pass


def price_decreased(scrip_csv=None, by=None, output_csv=None):
    print output_csv

    with open(output_csv, "a") as watchout:
        try:
            scrip_dataframe = pd.read_csv(scrip_csv)
            # print scrip_dataframe["Series"].loc("EQ")

            #print scrip_dataframe.query(scrip_dataframe.Series == "EQ")

            pct_change = get_pct_change(scrip_dataframe)
            if  pct_change < int(by):
                print "%s : WATCH OUT" % scrip_csv
                watchout.write("%s, %s\n" %(scrip_csv.split("/")[1].split(".")[0], pct_change))
            else:
                print "%s hasn't changed to less than %s" %(scrip_csv, by)
            print "================="
        except Exception as e:
            print "%s is broken: %s" %(scrip_csv, e.message)

def get_pct_change(scrip_dataframe):
    avg_change = scrip_dataframe.groupby("Series")["Close Price"].apply(lambda x: x.div(x.iloc[0]).subtract(1).mul(100))

    # .get_group("EQ").
    scrip_dataframe['avg_change'] = avg_change
    pct_change = \
    scrip_dataframe[["Date", "Series", "avg_change", "Close Price"]].loc[scrip_dataframe['Series'] == "EQ"][
        "avg_change"][scrip_dataframe.index[-1]]

    return pct_change

def my_mkdir(path):
    if not os.path.isdir(path):
        os.mkdir(path)

    return os.path.abspath(path)

def penny_stocks(value):
    print "Penny stocks: Max price: %s" %value
    penny_csv = "penny_lessthan_%s_as_on_%s.csv" %(value, datetime.datetime.today().strftime('%m-%d-%Y'))
    with open(penny_csv, "w") as fh_penny_csv:
        for scrip_csv in glob.glob("csvfiles/*.csv*"):
            try:
                scrip_dataframe = pd.read_csv(scrip_csv)
                # print scrip_dataframe["Series"].loc("EQ")

                # print scrip_dataframe.query(scrip_dataframe.Series == "EQ")
                price_now = scrip_dataframe["Close Price"].iloc[-1]

                if price_now < value:
                    print "Penny stock: %s"  %(scrip_csv)
                    fh_penny_csv.write("%s, %s\n" %(scrip_csv.split("/")[1].split(".")[0], price_now))
                    print "================="
            except Exception as e:
                print "%s is broken: %s" % (scrip_csv, e.message)

    print "List of stocks whose price as on today is less than: %s: %s" %(value, penny_csv)

if __name__ == "__main__":
    if not os.path.isdir("result_csv"):
        my_mkdir("result_csv")

    parser = argparse.ArgumentParser(description='Run analysis on various scrips.')
    parser.add_argument('--penny_stocks', action="store_true", help="Collect stocks with nomnal price")
    parser.add_argument('--penny_stock_price', dest="penny_stock_price", help="Price which to you is penny")

    parser.add_argument('--price_decreased_by', action="decreased_by", help="Collect stocks whose price decreased by"
                                                                            "the given value")
    parser.add_argument('--duration', action="duration", help="duration since today to refer to calculate the "
                                                                            "price decrease by given value",
                        default="22") # Default is 22 days. ie one month.

    args = parser.parse_args()
    if not args.penny_stocks and not args.price_decreased_by:
        parser.print_help()
        sys.exit(0)

    if args.penny_stocks:
        if not args.penny_stock_price:
            print "--penny_stock_price required."
            sys.exit()
        penny_stocks(args.penny_stock_price)

    if args.price_decreased_by:
        output_csv = "decreasedby_-%s_pct_as_on_%s.csv" %(args.price_decreased_by,
                                                          datetime.datetime.today().strftime('%m-%d-%Y'))
        for scrip_csv in glob.glob("csvfiles/*.csv*"):
            price_decreased(scrip_csv=scrip_csv, by="-%s" %args.price_decreased_by, output_csv=output_csv)

        print "Reports of stocks whose price decreased by %s: %s" %(output_csv, args.price_decreased_by)

