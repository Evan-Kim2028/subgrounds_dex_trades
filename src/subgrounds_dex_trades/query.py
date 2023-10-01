import os

# from dataclasses import dataclass
from subgrounds import Subgrounds
from subgrounds_dex_trades.utils import *
from dotenv import load_dotenv

load_dotenv()

sg_rollerdex = subgrounds_rollerdex


sg = Subgrounds.from_pg_key(os.getenv("PG_API_KEY"))


for key in subgrounds_rollerdex:
    deploy_id = subgrounds_rollerdex[key]
    print(f"Key: {key}, deploy_id: {deploy_id}")
    endpoint = f"https://api.playgrounds.network/v1/proxy/deployments/id/{deploy_id}"
    subgraph = sg.load_subgraph(endpoint)

    swaps_query = subgraph.Query.swaps(
        first=10,
        where=[
            subgraph.Swap.timestamp > 1693588285,
            subgraph.Swap.timestamp < 1693588285 + 86400,
        ],
    )
    df = sg.query_df(
        [
            # - swaps_ hash, timestamp, to, from, blockNumber, tokenIn_id, amountIn, tokenOut_id, amountOut, amountInUSD, amountOutUSD, pool_id
            swaps_query._select("hash"),
            swaps_query._select("timestamp"),
            swaps_query._select("to"),
            swaps_query._select("from"),
            swaps_query._select("blockNumber"),
            swaps_query._select("tokenIn")._select("id"),
            swaps_query._select("tokenIn")._select("symbol"),
            swaps_query._select("amountIn"),
            swaps_query._select("tokenOut")._select("id"),
            swaps_query._select("tokenOut")._select("symbol"),
            swaps_query._select("amountOut"),
            swaps_query._select("amountInUSD"),
            swaps_query._select("amountOutUSD"),
            swaps_query._select("pool")._select("id"),
        ]
    )

    print(df)
