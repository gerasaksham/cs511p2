"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-11 10:03:13
 	 FilePath: /codes/pandas_q3.py
 	 Description:
"""
import tempfile

import pandas as pd
import ray
import typing
import util.judge_df_equal


@ray.remote
def group_data(data:pd.DataFrame):
    result = data.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority']).agg(
        revenue=('l_extendedprice', lambda x: (x * (1 - data['l_discount'])).sum())
    ).reset_index()
    return result


def ray_q3(segment: str, customer: pd.DataFrame, orders: pd.DataFrame, lineitem: pd.DataFrame) -> pd.DataFrame:
    #TODO: your codes begin
    merged_data = pd.merge(customer, orders, left_on='c_custkey', right_on='o_custkey')
    merged_data = pd.merge(merged_data, lineitem,left_on='o_orderkey', right_on='l_orderkey')

    filtered_data = merged_data[(merged_data['c_mktsegment'] == segment) &
                                (merged_data['o_orderdate'] < '1995-03-15') &
                                (merged_data['l_shipdate'] > '1995-03-15')]

    num_chunks = 4
    chunk_size = len(filtered_data) // num_chunks
    chunks = [filtered_data[i*chunk_size:(i+1)*chunk_size] for i in range(num_chunks)]
    chunks.append(filtered_data[num_chunks*chunk_size:])
    chunk_ids = [ray.put(chunk) for chunk in chunks]
    grouped_data = [group_data.remote(chunk_id) for chunk_id in chunk_ids]

    grouped_data = ray.get(grouped_data)
    grouped_data = pd.concat(grouped_data, ignore_index=True)
    sorted_data = grouped_data.sort_values(by=['revenue', 'o_orderdate'], ascending=[False, True]).head(10)
    return sorted_data
    #end of your codes


if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()
    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    orders = pd.read_csv("tables/orders.csv", header=None, delimiter="|")
    customer = pd.read_csv("tables/customer.csv", header=None, delimiter="|")


    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
    customer.columns = ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
                        'c_comment']
    orders.columns = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
                      'o_clerk', 'o_shippriority', 'o_comment']

    # run the test
    result = ray_q3('BUILDING', customer, orders, lineitem)
    # result.to_csv("correct_results/pandas_q3.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/ray_q3.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")
