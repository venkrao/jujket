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


def price_change_metrics(scrip_csv=None, by=None, change_type=None, duration=None, output_csv=None):
    print output_csv

    with open(output_csv, "a") as watchout:
        try:
            scrip_dataframe = pd.read_csv(scrip_csv)
            # print scrip_dataframe["Series"].loc("EQ")
            duration = int(duration)
            scrip_dataframe_for_duration = scrip_dataframe.iloc[-duration:]
            #print scrip_dataframe.query(scrip_dataframe.Series == "EQ")

            pct_change = get_pct_change(scrip_dataframe_for_duration)
            if change_type == "decreased":
                if  pct_change < -int(by):
                    print "%s : WATCH OUT" % scrip_csv
                    watchout.write("%s, %s\n" %(scrip_csv.split("/")[1].split(".")[0], pct_change))
                else:
                    print "%s hasn't changed to less than %s" %(scrip_csv, by)

            if change_type == "increased":
                if pct_change > int(by):
                    print "%s : WATCH OUT" % scrip_csv
                    watchout.write("%s, %s\n" % (scrip_csv.split("/")[1].split(".")[0], pct_change))
                else:
                    print "%s hasn't changed to less than %s" % (scrip_csv, by)
            print "================="
        except Exception as e:
            print "%s is broken: %s" %(scrip_csv, e.message)

def get_pct_change(scrip_dataframe):
    avg_change = scrip_dataframe.groupby("Series")["Close Price"].apply(lambda x: x.div(x.iloc[0]).subtract(1).mul(100))

    # .get_group("EQ").
    scrip_dataframe.loc[:, ('avg_change')] = avg_change
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

def subset_dataframe_pattern(input_dataframe=None, pattern=None):

    try:
        df = input_dataframe[input_dataframe["Date"].str.contains(pattern)]
    except Exception as e:
        print "Could not find given pattern %s: %s" %(pattern, e.message)
        return None, False

    return df, True

def yoy_metrics(scrip_csv=None, output_csv=None):
    input_dataframe = pd.read_csv(scrip_csv)

    scrip = scrip_csv.replace("csvfiles/", "").replace(".csv", "")

    with open(output_csv, "a") as fh_output_csv:
        fh_output_csv.write("%s," %scrip)
        for year in range(2007, datetime.datetime.now().year+1):
            subset_dataframe, notnull = subset_dataframe_pattern(input_dataframe=input_dataframe, pattern="-%s" %year)
            # for this year, the company was trading.
            if notnull:
                try:
                    yoy_pct_change = get_pct_change(subset_dataframe)
                except Exception as e:
                    print "Failed to calculate yoy pct change, %s, %s: %s" %(scrip, year, e.message)
                    fh_output_csv.write("UNKNOWN,")
                    continue
                # alles gut. write pct change into the csv.
                fh_output_csv.write("%s," %yoy_pct_change)
                print "%s: total traded days in year %s: %s" %(scrip, year, len(subset_dataframe))
            else:
                print "%s: total traded days in year: %s 0" % (scrip, year)
                fh_output_csv.write("WAS_NOT_TRADING,")

        fh_output_csv.write("\n")

if __name__ == "__main__":
    if not os.path.isdir("result_csv"):
        my_mkdir("result_csv")

    parser = argparse.ArgumentParser(description='Run analysis on various scrips.')
    parser.add_argument('--penny_stocks', action="store_true", help="Collect stocks with nomnal price")
    parser.add_argument('--penny_stock_price', dest="penny_stock_price", help="Price which to you is penny")

    parser.add_argument('--price_decreased_by', dest="price_decreased_by", help="Collect stocks whose price decreased by"
                                                                            "the given value")
    parser.add_argument('--price_increased_by', dest="price_increased_by",
                        help="Collect stocks whose price increased by"
                             "the given value")
    parser.add_argument('--duration', dest="duration", help="duration since today to refer to calculate the "
                                                                            "price decrease by given value",
                        default="22") # Default is 22 days. ie one month.
    parser.add_argument("--yoy_price_metric", action="store_true",
                        help="Get the year on year price change for all scrips")

    parser.add_argument("--scrip", dest="scrip", required=False, default=None,
                        help="Limit whatever the action to just this scrip")


    args = parser.parse_args()
    if not args.penny_stocks and not args.price_decreased_by and \
            not args.price_increased_by and not args.yoy_price_metric:
        parser.print_help()
        sys.exit(0)

    if args.penny_stocks:
        if not args.penny_stock_price:
            print "--penny_stock_price required."
            sys.exit()
        penny_stocks(args.penny_stock_price)

    if args.price_decreased_by:
        output_csv = "decreasedby_%s_pct_as_on_%s.csv" %(args.price_decreased_by,

                                                       datetime.datetime.today().strftime('%m-%d-%Y'))
        if os.path.isfile(output_csv):
            os.remove(output_csv)

        for scrip_csv in glob.glob("csvfiles/*.csv*"):
            price_change_metrics(scrip_csv=scrip_csv, by="%s" %args.price_decreased_by, duration=args.duration,
                            output_csv=output_csv, change_type="decreased")

        print "List of companies whose stock price fell by %s: %s" %(output_csv, args.price_decreased_by)


    if args.price_increased_by:
        output_csv = "increasedby_%s_pct_as_on_%s.csv" %(args.price_increased_by,
                                               datetime.datetime.today().strftime('%m-%d-%Y'))
        if os.path.isfile(output_csv):
            os.remove(output_csv)

        for scrip_csv in glob.glob("csvfiles/*.csv*"):
            price_change_metrics(scrip_csv=scrip_csv, by="%s" %args.price_increased_by, duration=args.duration,
                            output_csv=output_csv, change_type="increased")


        print "List of companies whose stock price increased by %s: %s" %(output_csv, args.price_increased_by)

    if args.yoy_price_metric:
        """
        Price change year on year.

        If the company is listed public for/since say 10 years, then
        on how many occasions has the year on year price increased, and
        on how many occasions has it decreased.

        This is perhaps a reasonable metric to shortlist potential investment candidate.

        """
        print "Year on year price change metrics"
        if args.scrip:
            output_csv = "yoy_metrics_%s.csv" % (args.scrip)
        else:
            output_csv = "yoy_metrics_ALL.csv"

        with open(output_csv, "w") as fh:
            fh.write("Scrip, ")
            for year in range(2007, datetime.datetime.now().year + 1):
                fh.write("%s," %year)

            fh.write("\n")

        if args.scrip:
            yoy_metrics(scrip_csv="csvfiles/%s.csv" %args.scrip, output_csv=output_csv)
        else:
            for scrip_csv in glob.glob("csvfiles/*.csv*"):
                yoy_metrics(scrip_csv=scrip_csv, output_csv=output_csv)

        print "Yoy metrics in file: %s" %(output_csv)
