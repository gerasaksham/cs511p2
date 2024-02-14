"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-10 21:45:33
 	 FilePath: /codes/pandas_q2.py
 	 Description:
"""
import pandas as pd
import numpy as np
import ray
import typing
import util.judge_df_equal
import tempfile


@ray.remote
def calculate_chunks(chunk):
    return chunk.groupby(['l_returnflag', 'l_linestatus']).agg(
        sum_qty=('l_quantity', 'sum'),
        sum_base_price=('l_extendedprice', 'sum'),
        sum_disc_price=('l_extendedprice', lambda x: (x * (1 - chunk['l_discount'])).sum()),
        sum_charge=('l_extendedprice', lambda x: (x * (1 - chunk['l_discount']) * (1 + chunk['l_tax'])).sum()),
        avg_disc=('l_discount', 'mean'),
        count_order=('l_quantity', 'count')
    ).reset_index()

def ray_q2(timediff:int, lineitem:pd.DataFrame) -> pd.DataFrame:
    #TODO: your codes begin
    cutoff_date = pd.to_datetime("1998-12-01") - pd.DateOffset(days=timediff)
    filtered_lineitem = lineitem[pd.to_datetime(lineitem['l_shipdate']) <= cutoff_date]
    chunks = np.array_split(filtered_lineitem, 4)
    futures = [calculate_chunks.remote(chunk) for chunk in chunks]
    filtered_data = pd.concat(ray.get(futures))
    grouped_data = filtered_data.groupby(['l_returnflag', 'l_linestatus']).agg(
        sum_qty=('sum_qty', 'sum'),
        sum_base_price=('sum_base_price', 'sum'),
        sum_disc_price=('sum_disc_price', 'sum'),
        sum_charge=('sum_charge', 'sum'),
        avg_disc=('avg_disc', 'mean'),
        count_order=('count_order', 'sum')
    ).reset_index()
    grouped_data['avg_qty'] = grouped_data['sum_qty'] / grouped_data['count_order']
    grouped_data['avg_price'] = grouped_data['sum_base_price'] / grouped_data['count_order']
    columns_order = ['l_returnflag', 'l_linestatus', 'sum_qty', 'sum_base_price', 'sum_disc_price',
                           'sum_charge', 'avg_qty', 'avg_price', 'avg_disc', 'count_order']
    sorted_data = grouped_data[columns_order].sort_values(by=['l_returnflag', 'l_linestatus'])
    return sorted_data



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
    result = ray_q2(90, lineitem)
    # result.to_csv("correct_results/pandas_q2.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/ray_q2.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")


