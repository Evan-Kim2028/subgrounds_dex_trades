import os
from subgrounds import Subgrounds

from dotenv import load_dotenv

load_dotenv()


sg = Subgrounds.from_pg_key(os.getenv("PLAYGROUNDS_API_KEY"))

# sg = Subgrounds()
deployment_id = "QmQJovmQLigEwkMWGjMT8GbeS2gjDytqWCGL58BEhLu9Ag"

subgraph = sg.load_subgraph(
    # univ3 substreams subgraph
    # https://thegraph.com/explorer/subgraphs/HUZDsRpEVP2AvzDCyzDHtdc64dyDxx8FQjzsmqSg4H3B?view=Overview&chain=arbitrum-one
    f"https://api.playgrounds.network/v1/proxy/deployments/id/{deployment_id}"
)

# END_BLOCK = 18473464
# START_BLOCK = END_BLOCK - 7200  # 7200 blocks in a day (12blocks/second)

swaps_query = subgraph.Query.swaps(
    first=6500,
    # block={"number": END_BLOCK},
    # where={"_change_block": {"number_gte": START_BLOCK}},
)

df = sg.query_df(
    # swaps_query
    [
        swaps_query.transaction._select("id"),
        swaps_query.timestamp,
        swaps_query.logIndex,
        swaps_query._select("sender"),
        swaps_query._select("recipient"),
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

df.rename(
    columns={
        "swaps_transaction_id": "swaps_hash",
        "swaps_sender": "swaps_from",
        "swaps_recipient": "swaps_to",
    },
    inplace=True,
)

print(df.columns, df.shape)

# print(
#     df[
#         [
#             "swaps_blockNumber",
#             "swaps_logIndex",
#             "swaps_tokenIn_symbol",
#             "swaps_tokenOut_symbol",
#         ]
#     ]
# )
