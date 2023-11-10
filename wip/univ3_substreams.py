from subgrounds import AsyncSubgrounds
from datetime import datetime, timedelta
import aiofiles
import asyncio
import pandas as pd
import sys
import time
import os

# load PG decentralized APi key
from dotenv import load_dotenv

load_dotenv()

# Get command line date parameters (e.g. 2031-10-05 10 means get 10 days worth of data starting at 2023-10-05)
start_date = sys.argv[1]
date_range = int(sys.argv[2])

# Convert the start date from string to datetime object
start_date = datetime.strptime(start_date, "%Y-%m-%d")

# Create date ranges based on the provided start date and number of days
date_ranges = [
    (start_date, start_date + timedelta(days=1))
    for start_date in [start_date + timedelta(days=i) for i in range(0, date_range, 1)]
]

# Create a data folder if it doesn't exist
protocol_name = "UniswapV3_streamingfast"
if not os.path.exists(f"data/{protocol_name}"):
    os.makedirs(f"data/{protocol_name}")


async def save_to_file(df: pd.DataFrame, output_filename: str):
    """
    asynchronously write files to storage
    """
    async with aiofiles.open(output_filename, "w") as file:
        await file.write(df.to_csv(output_filename))


async def run_query(date_range: list):
    """
    Query a subgraph asynchronously
    """
    async with AsyncSubgrounds() as sg:
        sg = AsyncSubgrounds.from_pg_key(os.getenv("PLAYGROUNDS_API_KEY"))
        deployment_id: str = "QmQJovmQLigEwkMWGjMT8GbeS2gjDytqWCGL58BEhLu9Ag"

        subgraph = await sg.load_subgraph(
            # https://thegraph.com/explorer/subgraphs/HUZDsRpEVP2AvzDCyzDHtdc64dyDxx8FQjzsmqSg4H3B?view=Overview&chain=arbitrum-one
            f"https://api.playgrounds.network/v1/proxy/deployments/id/{deployment_id}"
        )

        t0 = time.perf_counter()
        start_date, end_date = date_range
        swaps_qp = subgraph.Query.swaps(
            first=25000,
            where=[
                subgraph.Swap.timestamp > int(start_date.timestamp()),
                subgraph.Swap.timestamp < int(end_date.timestamp()),
            ],
        )
        print(f"Query for {start_date.date()} started")
        df: pd.DataFrame = await sg.query_df(swaps_qp)
        print(df.shape)
        t1 = time.perf_counter()
        print(f"Query for {start_date.date()} completed in {t1-t0:0.2f}s")

        # insert custom data features
        df.insert(0, "protocol", protocol_name)

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
