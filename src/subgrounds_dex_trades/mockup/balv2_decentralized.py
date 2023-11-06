import os
from subgrounds import Subgrounds

from dotenv import load_dotenv

load_dotenv()


sg = Subgrounds.from_pg_key(os.getenv("PLAYGROUNDS_API_KEY"))
deployment_id = "QmSvVWWgni9dVDxFDySDuDFC7jH8PdBy3guKc7x9rRgGXv"

subgraph = sg.load_subgraph(
    # https://thegraph.com/explorer/subgraphs/Ei5typKWPepPSgqkaKf3p5bPhgJesnu1RuRpyt69Pcrx?view=Overview&chain=mainnet
    f"https://api.playgrounds.network/v1/proxy/deployments/id/{deployment_id}"
)

swaps_query = subgraph.Query.swaps(first=10)
df = sg.query_df(
    [
        # - swaps_ hash, timestamp, to, from, blockNumber, tokenIn_id, amountIn, tokenOut_id, amountOut, amountInUSD, amountOutUSD, pool_id
        swaps_query.hash,
        swaps_query.timestamp,
        swaps_query.to,
        swaps_query._select("from"),
        swaps_query.blockNumber,
        swaps_query.tokenIn._select("id"),
        swaps_query.tokenIn._select("symbol"),
        swaps_query.amountIn,
        swaps_query.tokenOut._select("id"),
        swaps_query.tokenOut._select("symbol"),
        swaps_query.amountOut,
        swaps_query.amountInUSD,
        swaps_query.amountOutUSD,
        swaps_query.pool._select("id"),
    ]
)

print(df.columns)

print(df)
