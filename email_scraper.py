import logging
from playwright.sync_api import sync_playwright
import pandas as pd
import random
import os
import re
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    filename="email_scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class EmailScraper:
    def __init__(self, df, output_file, proxy_file='socks5.txt', max_retries=3):
        self.df = df
        self.output_file = output_file
        self.proxy_file = proxy_file
        self.proxies = self.load_proxies()
        self.max_retries = max_retries
        tqdm.pandas()
        self.run_scraper()

    def load_proxies(self):
        """Load proxies from the given file."""
        try:
            with open(self.proxy_file, "r") as f:
                proxies = [line.strip() for line in f if line.strip()]
            if proxies:
                logging.info(f"Loaded {len(proxies)} proxies.")
            else:
                logging.error("No proxies available.")
            return proxies
        except FileNotFoundError:
            logging.error(f"Proxy file '{self.proxy_file}' not found.")
            return []

    def _accept_cookies(self, page):
        """Accept cookies if the button is found."""
        cookie_words = ["Accepteren", "Akkoord", "Accept Cookies", "Allow Cookies", "Agree", "OK"]
        for word in cookie_words:
            try:
                button = page.locator(f"//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{word.lower()}')]")
                if button.count() > 0:
                    button.first.click()
                    page.wait_for_timeout(1000)
                    logging.info(f"Clicked cookie button: {word}")
                    return True
            except Exception as e:
                logging.debug(f"Failed to click cookie button '{word}': {e}")
        return False

    def run_scraper(self):
        """Run the email scraper."""
        self.df['emails'] = self.df['website'].progress_apply(self.extract_emails)
        self.df['filtered_emails'] = self.df['emails'].apply(self.filter_unusual_emails)
        self.df.to_csv(self.output_file, mode='a' if os.path.exists(self.output_file) else 'w', index=False, header=not os.path.exists(self.output_file))

    def extract_emails(self, url):
        """Extract emails from the given URL."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)  # Set to True for headless mode
            proxy = random.choice(self.proxies) if self.proxies else None
            context = browser.new_context(proxy={"server": proxy} if proxy else {})
            page = context.new_page()

            for attempt in range(self.max_retries):
                try:
                    logging.info(f"Attempting to scrape {url} (Retry {attempt + 1}/{self.max_retries})")
                    page.goto(url, timeout=60000)  # Longer timeout to ensure the page loads
                    self._accept_cookies(page)
                    page.wait_for_selector("body", timeout=30000)  # Wait for page load

                    emails = self.find_emails(page.content())
                    if emails:
                        logging.info(f"Emails found on {url}: {emails}")
                        return emails

                    # Try the contact page
                    contact_url = f"{url.rstrip('/')}/contact"
                    logging.info(f"Attempting to scrape contact page {contact_url}")
                    page.goto(contact_url, timeout=30000)
                    page.wait_for_selector("body", timeout=30000)
                    
                    emails = self.find_emails(page.content())
                    if emails:
                        logging.info(f"Emails found on contact page of {url}: {emails}")
                        return emails

                except Exception as e:
                    logging.error(f"Error scraping {url}: {e}")
                    if attempt < self.max_retries - 1:
                        page.wait_for_timeout(5000)  # Retry delay
                        continue
            logging.warning(f"Failed to scrape {url} after {self.max_retries} retries.")
            return []

    @staticmethod
    def find_emails(content):
        """Find emails from page content."""
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'
        emails = list(set(re.findall(email_pattern, content)))
        return emails

    @staticmethod
    def filter_unusual_emails(emails):
        """Filter out emails that are unusual or suspicious."""
        def is_unusual(email):
            local_part = email.split('@')[0]
            return bool(re.search(r'\d{4,}', local_part) or 
                        re.search(r'[a-zA-Z]+\d+[a-zA-Z]+', local_part) or 
                        all(len(s) < 3 or not s.isalpha() for s in re.split(r'[._\-]', local_part)))
        return [email for email in emails if not is_unusual(email)] if emails else []

if __name__ == "__main__":
    df = pd.read_csv("data/data1.csv")
    df = df.drop_duplicates().dropna(subset=['website'])
    df['website'] = "https://www." + df['website']
    EmailScraper(df, "data/output.csv", proxy_file="socks5.txt")
