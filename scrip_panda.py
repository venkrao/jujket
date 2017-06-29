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
from getdata_nse import Nse

def price_increased(dataframe, percentage_change, output_csv, change_type):
    pass


def price_change_metrics(scrip_csv=None, by=None, change_type=None, duration=None, output_csv=None):

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
    scrip_dataframe = scrip_dataframe.assign(avg_change = avg_change)
    pct_change = \
    scrip_dataframe[["Date", "Series", "avg_change", "Close Price"]]["avg_change"][scrip_dataframe.index[-1]]
    return pct_change

def my_mkdir(path):
    if not os.path.isdir(path):
        os.mkdir(path)

    return os.path.abspath(path)

def penny_stocks(value):
    print "Penny stocks: Max price: %s" %value
    penny_csv = "penny_lessthan_%s_as_on_%s.csv" %(value, datetime.datetime.today().strftime('%m-%d-%Y'))
    with open(penny_csv, "w") as fh_penny_csv:
        for scrip in get_equity_only_scrips():
            scrip_csv = ("%s%s%s.csv" % (dir_for_csv, os.path.sep, scrip))
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
    # Along with the yoy price change, I need a ratio/percentage of
    # times during which the price increased.
    # i.e during a 10 year period, if the price has increased 6 times,
    # then, the increase_decrease_pct value will be: 60%

    count_increased = 0
    count_total = 0

    try:
        input_dataframe = pd.read_csv(scrip_csv)
    except Exception as e:
        print "Failed to read csv file %s: %s" %(scrip_csv, e.message)
        return

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
                count_total += 1
                if yoy_pct_change > 0:
                    count_increased += 1
            else:
                print "%s: total traded days in year: %s 0" % (scrip, year)
                fh_output_csv.write("WAS_NOT_TRADING,")
        increase_decrease_pct = 0
        # write the increase_decrease_pct now.
        if count_increased > 0:
            increase_decrease_pct = 100 * (float(count_increased)/float(count_total))

        try:
            closing_price = input_dataframe["Close Price"].iloc[-1]
        except Exception as e:
            print "Couldn't get closing price."
            closing_price = "UNKNOWN"

        fh_output_csv.write("%s," %increase_decrease_pct)
        fh_output_csv.write("%s," %closing_price)
        fh_output_csv.write("\n")

def mom_metrics(scrip_csv=None, output_csv=None):
    # Along with the month on month price change, I need a ratio/percentage of
    # times during which the price increased.
    # i.e during a 1 year period, if the close price on 7 months was greater than
    # opening price,
    # then, the increase_decrease_pct value will be: 70%

    # Sum of pct_change on each month is what is performance_index to me.
    count_increased = 0
    count_total = 0
    performance_index = 0
    try:
        input_dataframe = pd.read_csv(scrip_csv)
    except Exception as e:
        print "Failed to read csv file %s: %s" %(scrip_csv, e.message)
        return

    scrip = scrip_csv.replace("csvfiles/", "").replace(".csv", "")
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" ]
    with open(output_csv, "a") as fh_output_csv:
        fh_output_csv.write("%s," %scrip)
        for year in range(2007, datetime.datetime.now().year+1):
            for month in months:
                pattern = "%s-%s" %(month, year)
                subset_dataframe, notnull = subset_dataframe_pattern(input_dataframe=input_dataframe, pattern=pattern)
                # for this year, and month, the company was trading.
                if notnull:
                    try:
                        mom_pct_change = get_pct_change(subset_dataframe)
                    except Exception as e:
                        print "Failed to calculate yoy pct change, %s, %s: %s" %(scrip, year, e.message)
                        fh_output_csv.write("UNKNOWN,")
                        continue
                    # alles gut. write pct change into the csv.
                    fh_output_csv.write("%s," %mom_pct_change)
                    performance_index += mom_pct_change
                    print "%s: total traded days in year %s, month %s: %s" %(scrip, year, month, len(subset_dataframe))
                    count_total += 1
                    if mom_pct_change > 0:
                        count_increased += 1
                else:
                    print "%s: total traded days in year: %s 0" % (scrip, year)
                    fh_output_csv.write("WAS_NOT_TRADING,")
        increase_decrease_pct = 0
        # write the increase_decrease_pct now.
        if count_increased > 0:
            increase_decrease_pct = 100 * (float(count_increased)/float(count_total))

        try:
            closing_price = input_dataframe["Close Price"].iloc[-1]
        except Exception as e:
            print "Couldn't get closing price."
            closing_price = "UNKNOWN"
        fh_output_csv.write("%s," %increase_decrease_pct)
        fh_output_csv.write("%s," %performance_index)
        fh_output_csv.write("%s," % closing_price)
        fh_output_csv.write("\n")

def get_equity_only_scrips():
    all_scrips_csv = "all_scrips.csv"
    if not os.path.isfile("all_scrips.csv"):
        # download it if it is deleted.
        print "Download %s" %all_scrips_csv
        all_scrips_csv = Nse.get_all_scrips()

    df = pd.read_csv(all_scrips_csv)
    eq_only = df[df[' SERIES'] == " EQ"]['SYMBOL']
    return eq_only


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

    parser.add_argument("--mom_price_metric", action="store_true",
                        help="Get the month on month price change for all scrips")

    parser.add_argument("--scrip", dest="scrip", required=False, default=None,
                        help="Limit whatever the action to just this scrip")


    args = parser.parse_args()
    dir_for_csv = "csvfiles"
    if not args.penny_stocks and not args.price_decreased_by and \
            not args.price_increased_by and not args.yoy_price_metric \
            and not args.mom_price_metric:
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

        for scrip in get_equity_only_scrips():
            scrip_csv = ("%s%s%s.csv" % (dir_for_csv, os.path.sep, scrip))
            price_change_metrics(scrip_csv=scrip_csv, by="%s" %args.price_decreased_by, duration=args.duration,
                            output_csv=output_csv, change_type="decreased")

        print "List of companies whose stock price fell by %s: %s" %(output_csv, args.price_decreased_by)


    if args.price_increased_by:
        output_csv = "increasedby_%s_pct_as_on_%s.csv" %(args.price_increased_by,
                                               datetime.datetime.today().strftime('%m-%d-%Y'))
        if os.path.isfile(output_csv):
            os.remove(output_csv)

        for scrip in get_equity_only_scrips():
            scrip_csv = ("%s%s%s.csv" % (dir_for_csv, os.path.sep, scrip))
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

            fh.write("increase_decrease_pct,")
            fh.write("performance_index,")
            fh.write("current_price,")
            fh.write("\n")

        if args.scrip:
            yoy_metrics(scrip_csv="csvfiles/%s.csv" %args.scrip, output_csv=output_csv)
        else:
            for scrip in get_equity_only_scrips():
                scrip_csv = ("%s%s%s.csv" % (dir_for_csv, os.path.sep, scrip))
                yoy_metrics(scrip_csv=scrip_csv, output_csv=output_csv)

        print "Yoy metrics in file: %s" %(output_csv)


    if args.mom_price_metric:
        """
        Price change month on month.
        """
        print "Month on month price change metrics"
        if args.scrip:
            output_csv = "mom_metrics_%s.csv" % (args.scrip)
        else:
            output_csv = "mom_metrics_ALL.csv"

        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        with open(output_csv, "w") as fh:
            fh.write("Scrip, ")
            for year in range(2007, datetime.datetime.now().year + 1):
                for month in months:
                    fh.write("%s-%s," %(month, year))

            fh.write("increase_decrease_pct,")
            fh.write("performance_index,")
            fh.write("current_price,")
            fh.write("\n")

        if args.scrip:
            mom_metrics(scrip_csv="csvfiles/%s.csv" %args.scrip, output_csv=output_csv)
        else:
            for scrip in get_equity_only_scrips():
                scrip_csv = ("%s%s%s.csv" % (dir_for_csv, os.path.sep, scrip))
                mom_metrics(scrip_csv=scrip_csv, output_csv=output_csv)

        print "month on month metrics in file: %s" %(output_csv)
