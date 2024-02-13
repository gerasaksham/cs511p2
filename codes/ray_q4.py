"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-10 21:45:33
 	 FilePath: /codes/pandas_q2.py
 	 Description:
"""
import tempfile
import pandas as pd
import ray
import typing
import util.judge_df_equal


@ray.remote
def calculate_filtered_data(data:pd.DataFrame):
    grouped_data = data.groupby(['o_orderpriority', 'o_orderkey']).first().reset_index()
    filtered_data = grouped_data[grouped_data['l_commitdate'] < grouped_data['l_receiptdate']]
    return filtered_data

def ray_q4(time: str, orders: pd.DataFrame, lineitem: pd.DataFrame) -> pd.DataFrame:
    #TODO: your codes begin
    
    time = pd.to_datetime(time)
    if isinstance(orders['o_orderdate'].iloc[0], str):
        orders['o_orderdate'] = pd.to_datetime(orders['o_orderdate'])
    filtered_orders = orders[
        (orders['o_orderdate'] >= time) &
        (orders['o_orderdate'] < time + pd.DateOffset(months=3))
    ]

    filtered_data = pd.merge(filtered_orders, lineitem, how='inner', left_on='o_orderkey', right_on='l_orderkey')

    num_chunks = 4
    chunk_size = len(filtered_data) // num_chunks
    chunks = [filtered_data[i*chunk_size:(i+1)*chunk_size] for i in range(num_chunks)]
    chunks.append(filtered_data[num_chunks*chunk_size:])  # Include any remaining rows in the last chunk

    ray.init()

    chunk_ids = [ray.put(chunk) for chunk in chunks]
    tasks = [calculate_filtered_data.remote(chunk_id) for chunk_id in chunk_ids]


    priority_orders = ray.get(tasks)
    priority_orders = pd.concat(priority_orders, ignore_index=True).groupby('o_orderpriority').size().reset_index(name='order_count')
    priority_orders_sorted = priority_orders.sort_values(by='o_orderpriority')
    ray.shutdown()
    return priority_orders_sorted
    #end of your codes



if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()
    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    orders = pd.read_csv("tables/orders.csv", header=None, delimiter="|")
    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']

    orders.columns = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
                      'o_clerk', 'o_shippriority', 'o_comment']

    # run the test
    result = ray_q4("1993-7-01",orders,lineitem)
    # result.to_csv("correct_results/pandas_q4.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/ray_q4.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")
