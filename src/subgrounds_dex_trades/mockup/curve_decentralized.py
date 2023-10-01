import os
from subgrounds import Subgrounds

from dotenv import load_dotenv

load_dotenv()


sg = Subgrounds.from_pg_key(os.getenv("PG_API_KEY"))
deployment_id = "QmSiwiGxgCL7jP3g6HcafaFdowP2RbkohqoHgv5Q2p9How"

subgraph = sg.load_subgraph(
    # https://thegraph.com/explorer/subgraphs/GAGwGKc4ArNKKq9eFTcwgd1UGymvqhTier9Npqo1YvZB?view=Overview&chain=mainnet
    f"https://api.playgrounds.network/v1/proxy/deployments/id/{deployment_id}"
)

swaps_query = subgraph.Query.swaps(first=10)
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

print(df.columns)

print(df)
