from subgrounds import AsyncSubgrounds
from datetime import datetime, timedelta
import asyncio
import pandas as pd
import time
import os

from dotenv import load_dotenv

load_dotenv()

date_ranges = [
    (start_date, start_date + timedelta(days=1))
    for start_date in [
        datetime(2023, 10, 1) + timedelta(days=i) for i in range(0, 5, 1)
    ]
]

protocol_name = "Curve"

# Create a data folder if it doesn't exist
if not os.path.exists(f"data/{protocol_name}"):
    os.makedirs(f"data/{protocol_name}")


async def run_query(date_range: list):
    async with AsyncSubgrounds() as sg:
        sg = AsyncSubgrounds.from_pg_key(os.getenv("PLAYGROUNDS_API_KEY"))
        deployment_id = "QmSGDNPW2iAwwgh4He5eeBTaSLeTJpWKzuNfP2McAdBVq1"

        subgraph = await sg.load_subgraph(
            # https://thegraph.com/explorer/subgraphs/3fy93eAT56UJsRCEht8iFhfi6wjHWXtZ9dnnbQmvFopF?view=Overview&chain=arbitrum-one
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
                swaps_qp.to,
                swaps_qp._select("from"),
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
        df.insert(0, "protocol", protocol_name)

        # Save the DataFrame to a CSV file
        filename = f"data/{protocol_name}/swaps_{start_date.date()}.csv"
        df.to_csv(filename, index=False)

        t1 = time.perf_counter()
        print(
            f"Query for {start_date.date()} completed in {t1-t0:0.2f}s and saved to {filename}"
        )


async def main():
    t0 = time.perf_counter()

    tasks = [run_query(date_range) for date_range in date_ranges]
    await asyncio.gather(*tasks)

    t1 = time.perf_counter()
    print(f"Async Queries completed in {t1-t0:0.2f}s ")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
