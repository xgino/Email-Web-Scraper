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
import logging

# Turn off Warning Log
logging.getLogger("urllib3").setLevel(logging.ERROR)

class GetMail:
    def __init__(self, df, output_file, position=0, max_workers=5, proxy_list=None, batch_size=10):
        self.df = df
        self.output_file = output_file
        self.ua = UserAgent()  # User-Agent rotation
        self.proxies = proxy_list if proxy_list else []  # Proxy pool (if available)
        self.batch_size = batch_size  # Batch size for saving
        tqdm.pandas()  # Use tqdm with pandas apply
        self.run_scraper(max_workers, position)

    def run_scraper(self, max_workers, position):
        total_items = len(self.df)
        with tqdm(total=total_items, desc=f"Scraper {position}: ", position=position, leave=True) as progress_bar:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for i in range(0, total_items, self.batch_size):
                    batch = self.df.iloc[i:i + self.batch_size].copy()
                    batch = batch[batch['website'].apply(self._validate_url)]

                    # Set a timeout for scraping each URL
                    futures = {executor.submit(self._extract_emails, url): url for url in batch['website']}
                    results = []
                    for future in futures:
                        try:
                            results.append(future.result(timeout=30))  # Timeout for individual tasks
                        except Exception:
                            results.append(None)

                    batch['scraped_emails'] = [res['emails'] if res else None for res in results]
                    batch['emails'] = batch['scraped_emails'].apply(self._filter_unusual_emails)

                    batch['title'] = [res['title'] if res else None for res in results]
                    batch['site_name'] = [res['site_name'] if res else None for res in results]
                    batch['description'] = [res['description'] if res else None for res in results]
                    batch['keywords'] = [res['keywords'] if res else None for res in results]
                    batch['category'] = [res['category'] if res else None for res in results]
                    batch['business_category'] = [res['business_category'] if res else None for res in results]
                    batch['locale'] = [res['locale'] if res else None for res in results]

                    self._save_results(batch)
                    progress_bar.update(len(batch))


    def _extract_emails(self, url):
        session = self._get_session()
        if not self._validate_url(url):
            return None
        try:
            response = session.get(url, timeout=10)
            if response.status_code == 403:
                #print(f"Access forbidden: {url}")
                return None  # Handle blocked access
            soup = BeautifulSoup(response.content, 'html.parser')
            emails = self._find_emails(soup)

            # Extract business info (title, site_name, description, keywords, category, business_category, locale)
            title, site_name, description, keywords, category, business_category, locale = self._extract_business_info(soup)

            # Check possible pages for contact
            if not emails:
                contact_paths = [
                    "/contact", "/contact/", 
                    "/contact-us", "/contact-us/", 
                    "/contact_us", "/contact_us/", 
                    "/contactus", "/contactus/", 
                    "/get-in-touch", "/get-in-touch/", 
                    "/reach-us", "/reach-us/", 
                    "/connect", "/connect/", 
                    "/team", "/team/", 
                    "/info", "/info/", 
                ]

                valid_paths = []  # Store working URLs
                for path in contact_paths:
                    contact_url = url.rstrip('/') + path
                    contact_response = session.get(contact_url, timeout=5)
                    
                    # If page exists (status code 200), add to valid paths
                    if contact_response.status_code == 200:
                        valid_paths.append(contact_url)

                # Now extract emails from all valid contact pages
                for contact_url in valid_paths:
                    emails.update(self._find_emails(BeautifulSoup(session.get(contact_url).content, 'html.parser')))

            return {"emails": emails or None, "title": title or None, "site_name": site_name or None, "description": description or None, "keywords": keywords or None, "category": category or None, "business_category": business_category or None, "locale": locale or None}
        except requests.RequestException:
            return None

    def _extract_business_info(self, soup):
        # Extract Open Graph (OG) meta tags
        def get_meta_content(prop_name, attr="property"):
            tag = soup.find("meta", attrs={attr: prop_name})
            return tag["content"].strip() if tag and "content" in tag.attrs else ''

        title = get_meta_content("og:title")
        site_name = get_meta_content("og:site_name")
        description = get_meta_content("og:description")
        keywords = get_meta_content("og:keywords")
        category = get_meta_content("business:category")  # Some sites use this
        locale = get_meta_content("og:locale")  # Language

        # Extract standard meta tags (some sites use these instead of OG tags)
        std_description = get_meta_content("description", "name")
        std_keywords = get_meta_content("keywords", "name")

        # Extract Schema.org JSON-LD (if available)
        schema_data = soup.find("script", type="application/ld+json")
        business_category = ""
        if schema_data:
            import json
            try:
                json_data = json.loads(schema_data.string)
                if isinstance(json_data, list):  # Sometimes JSON-LD is an array
                    json_data = json_data[0]
                business_category = json_data.get("@type", "")
            except json.JSONDecodeError:
                pass

        # Prioritize standard description if OG is missing
        if not description and std_description:
            description = std_description

        # Prioritize standard keywords if OG is missing
        if not keywords and std_keywords:
            keywords = std_keywords

        return title, site_name, description, keywords, category, business_category, locale
    

    def _fetch_contact_page_emails(self, contact_url, session):
        retries = 3  # Number of retries
        for attempt in range(retries):
            try:
                response = session.get(contact_url, timeout=10)
                contact_soup = BeautifulSoup(response.content, 'html.parser')
                return self._find_emails(contact_soup)
            except requests.exceptions.ReadTimeout:
                if attempt == retries - 1:
                    #print(f"ReadTimeoutError: Skipping contact page {contact_url} after {retries} retries.")
                    return None
                time.sleep(1)  # Backoff before retrying
            except Exception:
                return None
        return None


    def _resolve_url(self, base_url, link):
        if not link.startswith('http'):
            return base_url.rstrip('/') + '/' + link.lstrip('/')
        return link

    def _validate_url(self, url):
        return url.startswith('http://') or url.startswith('https://')

    def _get_session(self):
        # Create a session with retry and User-Agent/proxy rotation
        session = requests.Session()
        retry = Retry(total=7, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update({'User-Agent': self.ua.random})  # Random User-Agent

        if self.proxies:
            proxy = self._get_random_proxy()
            session.proxies.update({'http': proxy, 'https': proxy})
            print(f"Proxy applied: {proxy}") 
        else:
            pass
            #print(f"No proxy applied.") 
        return session

    def _get_random_proxy(self):
        return self.proxies[random.randint(0, len(self.proxies) - 1)]

    def _find_emails(self, soup):
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'
        email_matches = soup.find_all(string=re.compile(email_pattern))
        emails = []
        for match in email_matches:
            emails.extend(re.findall(email_pattern, match))
        return list(set(emails))  # Deduplicate emails

    def _check_robots_txt(self, url):
        try:
            base_url = '/'.join(url.split('/')[:3])  # Extract base domain
            robots_url = f"{base_url}/robots.txt"
            response = requests.get(robots_url, timeout=10)
            if response.status_code == 200:
                robots_content = response.text
                disallowed_paths = re.findall(r'Disallow: (.+)', robots_content)
                if any('/contact' in path for path in disallowed_paths):
                    return False
            return True
        except Exception:
            return False

    def _filter_unusual_emails(self, emails):
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

    def _save_results(self, batch):
        batch.to_csv(self.output_file, mode='a', header=not pd.io.common.file_exists(self.output_file), index=False)
        #print(f"Batch processed and saved to '{self.output_file}'.")


if __name__ == "__main__":
    proxy_list = ["http://91.217.179.174:8080", "http://91.150.189.122:30389", "http://157.66.85.32:8080"]  # Add your proxies here
    df = pd.read_csv("./data/data.csv")
    df = df[df['website'].notna()]

    df['website'] = "https://www." + df['website']
    GetMail(df, "getmail.csv", max_workers=5, position=0, proxy_list=None)
