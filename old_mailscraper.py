import requests
from bs4 import BeautifulSoup
import re
from tqdm import tqdm
import pandas as pd
import time
from fake_useragent import UserAgent
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor
import random

class GetMail:
    def __init__(self, df, output_file, max_workers=5):
        self.df = df
        self.output_file = output_file
        self.ua = UserAgent()  # User-Agent rotation
        self.proxies = []  # Proxy pool (if available)
        tqdm.pandas()  # Use tqdm with pandas apply
        self.run_scraper(max_workers)

    def run_scraper(self, max_workers):
        # Process websites concurrently with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            self.df['emails'] = list(tqdm(executor.map(self.extract_emails, self.df['website']), total=len(self.df)))
        self.df['filtered_emails'] = self.df['emails'].progress_apply(self.filter_unusual_emails)
        self.save_results()

    def extract_emails(self, url):
        try:
            session = self.get_session()
            if not self.check_robots_txt(url):
                return None

            # Fetch main page and parse emails
            response = session.get(url, timeout=5)
            soup = BeautifulSoup(response.content, 'html.parser')
            emails = self.find_emails(soup)

            if not emails:
                # Attempt "/contact" page or linked contact pages
                contact_links = soup.find_all('a', text=re.compile(r'Contact', re.IGNORECASE))
                if contact_links:
                    contact_url = self.resolve_url(url, contact_links[0]['href'])
                    emails = self.fetch_contact_page_emails(contact_url, session)
                else:
                    emails = self.fetch_contact_page_emails(url.rstrip('/') + '/contact', session)
            return emails or None
        except Exception as e:
            return [f"Error scraping {url}: {str(e)}"]

    def fetch_contact_page_emails(self, contact_url, session):
        try:
            response = session.get(contact_url, timeout=5)
            contact_soup = BeautifulSoup(response.content, 'html.parser')
            return self.find_emails(contact_soup)
        except Exception:
            return None

    @staticmethod
    def resolve_url(base_url, link):
        if not link.startswith('http'):
            return base_url.rstrip('/') + '/' + link.lstrip('/')
        return link

    def get_session(self):
        # Create a session with retry and User-Agent/proxy rotation
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update({'User-Agent': self.ua.random})  # Random User-Agent
        if self.proxies:
            session.proxies.update({'http': self.get_random_proxy(), 'https': self.get_random_proxy()})
        return session

    def get_random_proxy(self):
        return self.proxies[random.randint(0, len(self.proxies) - 1)]

    @staticmethod
    def find_emails(soup):
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'
        email_matches = soup.find_all(string=re.compile(email_pattern))
        emails = []
        for match in email_matches:
            emails.extend(re.findall(email_pattern, match))
        return list(set(emails))  # Deduplicate emails

    @staticmethod
    def check_robots_txt(url):
        try:
            base_url = '/'.join(url.split('/')[:3])  # Extract base domain
            robots_url = f"{base_url}/robots.txt"
            response = requests.get(robots_url, timeout=5)
            if response.status_code == 200:
                robots_content = response.text
                disallowed_paths = re.findall(r'Disallow: (.+)', robots_content)
                if any('/contact' in path for path in disallowed_paths):
                    return False
            return True
        except Exception:
            return False

    @staticmethod
    def filter_unusual_emails(emails):
        def is_unusual_email(email):
            local_part = email.split('@')[0]
            if re.search(r'\d{4,}', local_part): return True
            if re.search(r'[a-zA-Z]+\d+[a-zA-Z]+', local_part) or re.search(r'\d+[a-zA-Z]+\d+', local_part): return True
            segments = re.split(r'[._\-]', local_part)
            if all(len(segment) < 3 or not re.match(r'[a-zA-Z]+', segment) for segment in segments): return True
            return False

        if isinstance(emails, list):
            return [email for email in emails if not is_unusual_email(email)] or None
        return None

    def save_results(self):
        self.df.to_csv(self.output_file, index=False)
        print(f"Processing complete. Results saved to '{self.output_file}'.")


if __name__ == "__main__":
    df = pd.read_csv("data/data.csv")
    df = df[df['website'].notna()]
    df['website'] = "https://www." + df['website']
    GetMail(df, "output.csv", max_workers=5)
