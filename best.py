from playwright.sync_api import sync_playwright
import pandas as pd
import random
import os
import re
from tqdm import tqdm

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class EmailScraper:
    def __init__(self, output_file, proxy_file='socks5.txt'):
        self.output_file = output_file
        self.proxy_file = proxy_file

    def _load_and_get_proxy(self):
        try:
            with open(self.proxy_file, "r") as f:
                proxies = [line.strip() for line in f if line.strip()]
            if not proxies:
                raise Exception("No proxies available.")
            return random.choice(proxies)
        except (FileNotFoundError, Exception) as e:
            logging.error(f"Error loading proxy: {e}")
            return None
    
    def _accept_cookies(self, page):
        """
        Look for and click the 'Accept Cookies' button using text or specific class name.

        Args:
            page: The Playwright page object.

        Returns:
            bool: True if a cookie button was found and clicked, False otherwise.
        """
        # List of words to match text-based cookie banners
        cookie_words = [
            "Accepteren", "Akkoord", "Cookies Accepteren", "Accept Cookies", 
            "Allow Cookies", "Agree", "OK", "I Agree", "Accept All"
        ]
        
        # Check for buttons based on text
        for word in cookie_words:
            try:
                button = page.locator(f"//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{word.lower()}')]")
                if button.count() > 0:
                    button.first.click()
                    page.wait_for_timeout(1000)  # Wait for any overlay to disappear
                    logging.info(f"Clicked cookie button with text: {word}")
                    return True
            except Exception as e:
                logging.debug(f"Failed to click cookie button for word '{word}': {e}")

        # Check for buttons using specific class name
        try:
            button = page.locator('//button[contains(@class, "VfPpkd-LgbsSe")]')
            if button.count() > 0:
                button.first.click()
                page.wait_for_timeout(1000)  # Wait for any overlay to disappear
                logging.info("Clicked cookie button with specific class name.")
                return True
        except Exception as e:
            logging.debug(f"Failed to click cookie button by class name: {e}")

        logging.info("No cookie acceptance button found.")
        return False

    def scrap(self, df, position=0):
        websites = df['website']
        total_results = 0

        # Estimate total number of batches
        min_batch_size, max_batch_size = 40, 50
        estimated_batches = len(websites) // ((min_batch_size + max_batch_size) // 2) + (
            1 if len(websites) % ((min_batch_size + max_batch_size) // 2) else 0
        )

        batch_progress = tqdm(total=estimated_batches, desc=f"Scraper:{position} - Total Batches", position=position, leave=True)

        for i in range(0, len(websites), random.randint(min_batch_size, max_batch_size)):
            current_batch = websites[i:i + random.randint(min_batch_size, max_batch_size)]

            with sync_playwright() as p:
                browser = p.chromium.launch(headless=False)
                context = None

                for attempt in range(7):  # Retry up to 7 times
                    proxy = self._load_and_get_proxy()
                    try:
                        context = browser.new_context(proxy={"server": proxy} if proxy else None, java_script_enabled=False)
                        break
                    except Exception as e:
                        proxy = None
                        if context:
                            context.close()
                        logging.warning(f"Proxy failed, retrying. Error: {e}")

                # Fallback to no proxy after retries
                if not context:
                    context = browser.new_context(java_script_enabled=False)

                page = context.new_page()

                data_batch = []
                for website in current_batch:
                    try:
                        # Attempt to visit the website with adjusted timeout and wait condition
                        page.goto(website, timeout=120000, wait_until="domcontentloaded")
                        
                        # Optional: Log the successful visit
                        print(f"=========================== Visited {website}")

                        # Look at the HTML content and find email
                        # Example: Extract emails using regex
                        html_content = page.content()
                        emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', html_content)
                        
                        # If emails are found, store them in the batch
                        if emails:
                            data_batch.append({"website": website, "emails": emails})

                    except Exception as e:
                        logging.error(f"Error scraping {website}: {e}")
                        # Optionally, you can retry or log the website for further inspection later


                # Save the batch to the output file
                if data_batch:
                    pd.DataFrame(data_batch).to_csv(
                        self.output_file, mode="a", header=not pd.io.common.file_exists(self.output_file), index=False
                    )

                context.close()
                browser.close()

            # Update batch progress
            batch_progress.update(1)

        batch_progress.close()
        logging.info(f"Total results scraped: {total_results}")


if __name__ == "__main__":
    df = pd.read_csv("data/data1.csv")
    df = df.drop_duplicates().dropna(subset=['website'])
    df['website'] = "https://www." + df['website']

    scraper = EmailScraper(output_file='./data/best.csv')
    scraper.scrap(df)