from subgrounds import AsyncSubgrounds
from datetime import datetime, timedelta
import aiofiles
import asyncio
import pandas as pd
import sys
import time
import os

from dotenv import load_dotenv

load_dotenv()


# Get command line date parameters
start_date = sys.argv[1]
date_range = int(sys.argv[2])

# Convert the start date from string to datetime object
start_date = datetime.strptime(start_date, "%Y-%m-%d")

# Create date ranges based on the provided start date and number of days
date_ranges = [
    (start_date, start_date + timedelta(days=1))
    for start_date in [start_date + timedelta(days=i) for i in range(0, date_range, 1)]
]

protocol_name = "UniswapV3"
# Create a data folder if it doesn't exist
if not os.path.exists(f"data/{protocol_name}"):
    os.makedirs(f"data/{protocol_name}")


async def save_to_file(df: pd.DataFrame, output_filename: str):
    async with aiofiles.open(output_filename, "w") as file:
        await file.write(df.to_csv(output_filename))


async def run_query(date_range: list) -> pd.DataFrame:
    async with AsyncSubgrounds() as sg:
        sg = AsyncSubgrounds.from_pg_key(os.getenv("PLAYGROUNDS_API_KEY"))
        deployment_id: str = "QmZBbTLGEGNjep6X9KUXZWvRvZpybpcZH7zVVhtq9Un2fw"

        subgraph = await sg.load_subgraph(
            # https://thegraph.com/explorer/subgraphs/4cKy6QQMc5tpfdx8yxfYeb9TLZmgLQe44ddW1G7NwkA6?view=Overview&chain=arbitrum-one
            f"https://api.playgrounds.network/v1/proxy/deployments/id/{deployment_id}"
        )

        t0 = time.perf_counter()
        start_date, end_date = date_range
        swaps_qp = subgraph.Query.swaps(
            first=5000,
            where=[
                subgraph.Swap.timestamp > int(start_date.timestamp()),
                subgraph.Swap.timestamp < int(end_date.timestamp()),
            ],
        )
        print(f"Query for {start_date.date()} started")
        df: pd.DataFrame = await sg.query_df(
            [
                swaps_qp.hash,
                swaps_qp.timestamp,
                swaps_qp.account._select("id"),
                swaps_qp.blockNumber,
                swaps_qp.tokenIn._select("id"),
                swaps_qp.tokenIn._select("symbol"),
                swaps_qp.amountIn,
                swaps_qp.tokenOut._select("id"),
                swaps_qp.tokenOut._select("symbol"),
                swaps_qp.amountOut,
                swaps_qp.amountInUSD,
                swaps_qp.amountOutUSD,
                swaps_qp.pool._select("id"),
            ]
        )

        print(df.shape)
        t1 = time.perf_counter()
        print(f"Query for {start_date.date()} completed in {t1-t0:0.2f}s")

        df.insert(3, "from", None)
        df.insert(0, "protocol", protocol_name)
        df.rename(columns={"swaps_account_id": "to"}, inplace=True)
        # Save the DataFrame to a CSV file
        # Save the DataFrame using aiofiles
        filename = f"data/{protocol_name}/swaps_{start_date.date()}.csv"
        await save_to_file(df, filename)

        return df


async def main():
    t0 = time.perf_counter()

    tasks = [run_query(date_range) for date_range in date_ranges]
    await asyncio.gather(*tasks)

    t1 = time.perf_counter()
    print(f"Async Queries completed in {t1-t0:0.2f}s ")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
