"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:47
 	 LastEditTime: 2023-09-11 10:37:24
 	 FilePath: /codes/pandas_q1.py
 	 Description: 
"""
import pandas as pd
import ray
import typing


def pandas_q1(time: str, lineitem:pd.DataFrame) -> float:
    # TODO: your codes begin
    time = pd.to_datetime(time, format='%Y-%m-%d')
    if isinstance(lineitem['l_shipdate'].iloc[0], str):
        lineitem['l_shipdate'] = pd.to_datetime(lineitem['l_shipdate'])
    filtered_lineitem = lineitem[
        (lineitem['l_shipdate'] >= time) &
        (lineitem['l_shipdate'] < time + pd.offsets.DateOffset(years=1)) &
        (lineitem['l_discount'].between(.06 - 0.01, .06 + 0.010001)) &
        (lineitem['l_quantity'] < 24)
    ]
    filtered_lineitem['revenue_increase'] = filtered_lineitem['l_extendedprice'] * filtered_lineitem['l_discount']
    total_revenue = filtered_lineitem['revenue_increase'].sum()
    return total_revenue
    # end of your codes




if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()

    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']

    # run the test
    result = pandas_q1("1994-01-01", lineitem)
    try:
        assert abs(result - 123141078.2283) < 0.01
        print("*******************pass**********************")
    except Exception as e:
        logger.error("Exception Occurred:" + str(e))
        print(f"*******************failed, your incorrect result is {result}**************")
