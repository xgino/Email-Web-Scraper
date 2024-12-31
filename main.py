import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import logging
from get_mail import GetMail  # Ensure this imports the correct class

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

PROCESSORS = 10  # Number of processors
CHUNK_SIZE = None  # Optional, computed dynamically

def process_chunk(chunk, position, output_file, max_workers, proxy_list):
    """
    Wrapper function to initialize GetMail for each chunk.
    """
    tqdm.set_lock(tqdm.get_lock())  # Ensure tqdm works correctly across processes
    GetMail(chunk, output_file=output_file, position=position, max_workers=max_workers, proxy_list=proxy_list)

if __name__ == "__main__":
    # Read the input CSV
    df = pd.read_csv("data/data.csv")
    df = df.drop_duplicates()
    df = df[df['website'].notna()]
    if 'filtered_emails' in df.columns:
        df = df[df['filtered_emails'].isna()]
    df['website'] = "https://www." + df['website'].str.strip()

    logging.info(f"Data size: {len(df)} rows")
    logging.info(f"Using {PROCESSORS} processors")

    # Split the DataFrame into chunks
    chunk_size = len(df) // PROCESSORS
    chunks = [df.iloc[i * chunk_size: (i + 1) * chunk_size] for i in range(PROCESSORS)]

    # Handle any leftover rows
    if len(df) % PROCESSORS != 0:
        chunks.append(df.iloc[PROCESSORS * chunk_size:])

    logging.info(f"Number of chunks: {len(chunks)}")

    # Proxy list
    proxy_list = [
        "http://91.217.179.174:8080",
        "http://91.150.189.122:30389",
        "http://157.66.85.32:8080"
    ]

    # Output file
    output_file = "data/output.csv"

    # Maximum workers per thread
    max_workers = 5

    # Run the scraper for each chunk using ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=PROCESSORS) as executor:
        executor.map(
            partial(
                process_chunk,
                output_file=output_file,
                max_workers=max_workers,
                proxy_list=proxy_list
            ),
            chunks,
            range(len(chunks))  # Pass positions as 0, 1, 2, ...
        )
