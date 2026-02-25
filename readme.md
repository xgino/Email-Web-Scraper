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

- **emails**: Email(s)
- **title**: Meta title
- **site_name**: Meta site_name
- **description**: Meta description
- **keywords**: Meta keywords
- **category**: Meta category
- **business_category**: Meta business_category
- **locale**: Meta locale


## Support  
I spent days trying to implement proxy rotation, but unfortunately, I couldn't get it to work with BeautifulSoup (BS4). While proxies work fine with Playwright, I found it wasn't a critical feature for this tool. Proxy rotation is most useful when scraping the same website multiple times, but since this tool is designed for scraping multiple different websites, it doesn’t present a major issue.

If you find this tool useful, consider supporting me with a coffee on [Ko-fi](https://ko-fi.com/xgino). Every sip helps fuel a new line of code. Thank you for your support, and keep coding!

## Legal Disclaimer & Usage
**Educational & Research Use Only.** This tool is provided "as-is" by **xgino**. By using this software, you agree to the following:

* **Minimizing Impact:** This tool is designed to be "polite." It respects `robots.txt` and uses delays to avoid server strain. Users must ensure their scraping frequency does not disrupt target website operations.
* **Data Privacy (GDPR/CCPA):** While emails may be publicly visible, collecting them often counts as processing **Personal Data**. This is restricted in many jurisdictions (e.g., EU/ZZP) depending on your intent. **Check your local laws before use.**
* **Anti-Spam:** Harvesting emails for unsolicited outreach may violate the CAN-SPAM Act or GDPR. You are responsible for obtaining necessary consent before contact.
* **No Liability:** The author assumes **zero responsibility** for legal consequences, IP bans, or misuse. You use this tool at your **own risk** and agree to indemnify the author against all claims.
