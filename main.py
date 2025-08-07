import os
import multiprocessing
import logging
from get_mail import GetMail
from tqdm import tqdm
import pandas as pd 

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

PROCESSORS = 10  # Number of processors

def split_dataframe(df, n_splits):
    """
    Splits a DataFrame into n_splits parts.
    Any leftover rows are added to the last chunk.
    
    Args:
        df (pd.DataFrame): The DataFrame to split.
        n_splits (int): The number of splits.
    
    Returns:
        list of pd.DataFrame: List of split DataFrames.
    """
    chunks = []
    chunk_size = len(df) // n_splits
    remainder = len(df) % n_splits

    for i in range(n_splits):
        start = i * chunk_size + min(i, remainder)  # Add remainder to early chunks
        end = start + chunk_size + (1 if i < remainder else 0)
        chunks.append(df.iloc[start:end])
    
    # Print the number of rows in each split
    chunk_sizes = [len(chunk) for chunk in chunks]
    print(f"Chunk sizes: {chunk_sizes}")
    
    return chunks

def main():
    df = pd.read_csv("data/data.csv")
    df = df.drop_duplicates()
    df = df[df['website'].notna()]
    if 'emails' in df.columns:
        df = df[df['emails'].isna()]
    df['website'] = "https://www." + df['website'].str.strip()
    print(f"Data Size:{len(df)}")

    logging.info(f"Using {PROCESSORS} processors")
    chunks = split_dataframe(df, PROCESSORS)


    # Proxy list # Shit proxy not working 
    proxy_list = [
        "http://91.217.179.174:8080",
        "http://91.150.189.122:30389",
        "http://157.66.85.32:8080"
    ]

    output_file = "data/email_list.csv"
    logging.info(f"========= Start Scraping =========")


    # Run scrapers in parallel for each keyword file
    processes = []
    for i, chunk in enumerate(chunks):
        process = multiprocessing.Process(
            target=GetMail,
            args=(chunk, output_file, i, 5, None,)  
        )
        processes.append(process)

    # Start all processes
    for process in processes:
        process.start()

    # Wait for all processes to complete
    for process in processes:
        process.join(timeout=300)

    print("All scrapers have completed.")

    # Consolidate results and drop the 'scraped_emails' column
    if os.path.exists(output_file):
        final_df = pd.read_csv(output_file)
        if 'scraped_emails' in final_df.columns:
            final_df.drop(columns=['scraped_emails'], inplace=True)
        final_df.to_csv(output_file, index=False)
        print(f"Final data saved to {output_file} without 'scraped_emails' column.")


if __name__ == "__main__":
    main()

    
