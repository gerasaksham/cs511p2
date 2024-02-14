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
import numpy as np


@ray.remote
def calculate_revenue(chunk: pd.DataFrame) -> float:
    filtered_chunk = chunk[
        (chunk['l_shipdate'] >= pd.to_datetime('1994-01-01')) &
        (chunk['l_shipdate'] < pd.to_datetime('1994-01-01') + pd.DateOffset(years=1)) &
        (chunk['l_discount'].between(0.06 - 0.01, 0.06 + 0.010001)) &
        (chunk['l_quantity'] < 24)
    ]
    revenue = (filtered_chunk['l_extendedprice'] * filtered_chunk['l_discount']).sum()
    return revenue

def ray_q1(time: str, lineitem: pd.DataFrame) -> float:
    if isinstance(lineitem['l_shipdate'].iloc[0], str):
        lineitem['l_shipdate'] = pd.to_datetime(lineitem['l_shipdate'])
    num_chunks = 4
    chunk_size = len(lineitem) // num_chunks
    chunks = [lineitem[i*chunk_size:(i+1)*chunk_size] for i in range(num_chunks)]
    chunks.append(lineitem[num_chunks*chunk_size:])  # Include any remaining rows in the last chunk
    
    ray.init()
    
    chunk_ids = [ray.put(chunk) for chunk in chunks]
    actor_results = [calculate_revenue.remote(chunk_id) for chunk_id in chunk_ids]
    total_revenue = sum(ray.get(actor_results))
    
    return total_revenue



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
    result = ray_q1("1994-01-01", lineitem)
    try:
        assert abs(result - 123141078.2283) < 0.01
        print("*******************pass**********************")
    except Exception as e:
        logger.error("Exception Occurred:" + str(e))
        print(f"*******************failed, your incorrect result is {result}**************")
