# Email Web Scraper

The Email Scraper is a Python-based tool that extracts publicly available email addresses from websites.


## Key Features

- **Proxy Rotation**: Uses your own proxy for scraping. `No proxy rotation`.
- **Batch Processing**: Runs multiple scripts simultaneously, drastically `reducing processing time`.
- **Progress Tracking**: Each process displays an `individual progress bar` for clear and real-time updates.


## Installation

1. Clone the Repository

   ```bash
   git clone https://github.com/xgino/Email-Web-Scraper.git  
   cd Email-Web-Scraper
   ```

2. Install Dependencies

   ```bash
   pip install -r requirements.txt  
   ```

## Usage
1. Run the Scraper

   ```bash 
   python main.py  
   ```


## Scraper Output  
The scraped data is saved in `./data/email_list.csv` with the following columns:

- **emails**: Emails


## Support  
I spent days trying to implement proxy rotation, but unfortunately, I couldn't get it to work with BeautifulSoup (BS4). While proxies work fine with Playwright, I found it wasn't a critical feature for this tool. Proxy rotation is most useful when scraping the same website multiple times, but since this tool is designed for scraping multiple different websites, it doesn’t present a major issue.

If you find this tool useful, consider supporting me with a coffee on [Ko-fi](https://ko-fi.com/xgino). Every sip helps fuel a new line of code. Thank you for your support, and keep coding!


## Disclaimer
This tool is designed to scrape **publicly visible email addresses** from websites while fully respecting the `robots.txt` guidelines. It ensures that scraping is done in a **timely manner**, avoiding overloading websites, especially since most websites may not have robust servers. This approach minimizes the risk of causing any disruption.

Since the email addresses are **publicly visible** on these websites, collecting them is generally allowed. However, you must be mindful of regulations such as **GDPR** and other data protection laws, especially when handling **personal data**. 

It is essential to:
- **Use the collected email addresses responsibly**.
- **Never share or misuse** the data.

In addition, take care in **storing this data securely** to prevent leaks or unauthorized access. Always ensure that any email addresses collected are used **only for lawful and ethical purposes**.

Before using this tool:
- **Check your local laws** to ensure full compliance.
- Always **scrape responsibly**, respecting both the websites you scrape from and the privacy of individuals.