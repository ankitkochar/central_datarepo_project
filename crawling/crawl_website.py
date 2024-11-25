import requests
from bs4 import BeautifulSoup, Comment
from markdownify import markdownify
from collections import deque
from urllib.parse import urljoin, urlparse
import time
from .llm_integrator import LLMIntegrator
import json
import logging
import os
from typing import Dict, List, Tuple
from playwright.async_api import async_playwright
import threading
from playwright.sync_api import sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from utils.s3_utils import save_pdf_to_s3, upload_html_to_s3
from uuid import uuid4
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re


load_dotenv()
azure_api_key = os.getenv("AZURE_4OMINI_KEY")
log_files_folder = os.environ.get("LOG_FILES_FOLDER")
es_user = os.getenv("ELASTICSEARCH_USER")
es_password = os.getenv("ELASTICSEARCH_PASSWORD")
es_host = os.getenv("ELASTIC_SEARCH_HOST")

es = Elasticsearch(es_host, basic_auth=(es_user, es_password))


try:
    log_file_path = os.path.join(log_files_folder, "download.log")
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format="%(asctime)s - %(module)s - %(funcName)s - %(levelname)s - %(message)s",
    )
    logging.info("Logging started successfully.")
except Exception as e:
    print(f"Failed to set up logging: {e}")


class WebScraper:
    def __init__(
        self,
        start_url,
        inst_id,
        institute_name,
        input_json=None,
        max_depth=4,
        rate_limit=1,
        llm_api_key=azure_api_key,
    ):
        self.inst_id = inst_id
        self.start_url = start_url
        self.domain = urlparse(start_url).netloc
        self.url_queue = deque([(start_url, 0)])  # (url, depth)
        self.visited_urls = set()
        self.max_depth = max_depth
        self.rate_limit = rate_limit
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=True)
        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            ignore_https_errors=True,
        )

        self.llm_integrator = (
            LLMIntegrator(
                api_key=llm_api_key,
                base_domain=start_url,
                institute_name=institute_name,
            )
            if llm_api_key
            else None
        )
        self.json_data = input_json if input_json else {}  # Initialize empty JSON data
        self.empty_fields = self.get_empty_fields()
        self.json_lock = threading.Lock()
        self.queue_lock = threading.Lock()
        self.visited_lock = threading.Lock()
        self.scrape_data = []
        self.downloaded_pdf = set()

    def get_current_json_data(self):
        with self.json_lock:
            return self.json_data.copy(), self.empty_fields.copy()

    def add_scrape_data(self):
        url_added = []

        for data in self.scrape_data:
            actual_url = data["actual_url"]
            if actual_url not in url_added:
                es.index(index="scraper_info", body=data)
                url_added.append(actual_url)

    def replace_relative_links(self, soup, base_url):
        for a_tag in soup.find_all("a", href=True):
            if not a_tag["href"].startswith(("http://", "https://", "//")):
                a_tag["href"] = urljoin(base_url, a_tag["href"])

        for img_tag in soup.find_all("img", src=True):
            if not img_tag["src"].startswith(("http://", "https://", "//")):
                img_tag["src"] = urljoin(base_url, img_tag["src"])

        return soup

    def download_pdf(self, url):
        filename = os.path.join(url.split("/")[-1])
        if url not in self.downloaded_pdf:
            session = requests.Session()

            # Set up retry strategy
            retries = Retry(
                total=3, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504]
            )
            session.mount("https://", HTTPAdapter(max_retries=retries))

            # Set custom headers
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Referer": "https://www.google.com/",
            }

            try:
                response = session.get(
                    url,
                    headers=headers,
                    stream=True,
                    verify=False,
                )
                response.raise_for_status()

                # Check if the content type is PDF
                content_type = response.headers.get("Content-Type", "").lower()
                if "application/pdf" not in content_type:
                    logging.error(
                        f"URL {url} does not point to a PDF file. Content-Type: {content_type}"
                    )
                    return None
                # Check the first few bytes of the file
                # pdf_signature = b"%PDF-"
                # first_bytes = next(response.iter_content(len(pdf_signature)))
                # if not first_bytes.startswith(pdf_signature):
                #     logging.error(f"File from {url} does not have a valid PDF signature")
                #     return None
                # content = first_bytes + response.raw.read()

                content = response.content
                if len(content) < 100:
                    logging.error(
                        f"PDF from {url} is less than 100 bytes in size. Skipping."
                    )
                    return None

                s3_link = save_pdf_to_s3(url, self.inst_id, content)
                doc = {
                    "institute_id": self.inst_id,
                    "actual_url": url,
                    "s3_url": s3_link,
                    "title": url.split("/")[-1].lower().split(".")[0],
                    "status": True,
                    "file_type": "pdf",
                }
                self.scrape_data.append(doc)
                self.downloaded_pdf.add(self.normalize_url(url))
                return doc
            except requests.exceptions.RequestException as e:
                logging.error(f"Error downloading PDF {url}: {e}")
                self.downloaded_pdf.add(self.normalize_url(url))
                return None
        else:
            logging.info(f"PDF already downloaded: {url}")
            return None

    def update_json_data(self, updated_fields):
        with self.json_lock:
            self.json_data.update(updated_fields)
            self.update_empty_fields()

    def fetch_page(self, url):
        try:
            page = self.context.new_page()
            try:
                page.goto(url, wait_until="networkidle", timeout=30000)
            except PlaywrightTimeoutError:
                # If 30 seconds pass before networkidle, we'll end up here
                pass
            content = page.content()
            page.close()
            return content
        except Exception as e:
            logging.error(f"Error fetching {url}: {e}")
            return None

    def get_empty_fields(self) -> List[str]:
        empty_fields = []
        for key, value in self.json_data.items():
            if isinstance(value, str) and not value:
                empty_fields.append(key)
            elif isinstance(value, list) and not value:
                empty_fields.append(key)
            elif isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    if not sub_value:
                        empty_fields.append(f"{key}.{sub_key}")
        return empty_fields

    def update_empty_fields(self):
        self.empty_fields = self.get_empty_fields()

    def parse_html(self, html):
        soup = BeautifulSoup(html, "lxml")
        for table in soup.find_all("table"):
            table.insert_before(soup.new_string("[TABLE]"))
            table.insert_after(soup.new_string("[/TABLE]"))
        return soup

    def remove_comments(self, soup):
        for element in soup.findAll(string=lambda text: isinstance(text, Comment)):
            element.extract()

    def clean_soup(self, soup):

        [s.decompose() for s in soup("script")]
        # fmt: off
        # fmt: off
        for tag in soup(
            ["script", "style", "link", "meta", "input", "form", "noscript", "img", "svg", "button", "aside", "figure", "fielset", "details", "textarea", "fieldset"]
        ):
            tag.decompose()
        # fmt: on
        # fmt: on
        for element in soup.find_all(string=lambda text: isinstance(text, Comment)):
            element.extract()
        for div in soup.find_all("div", style="display:none;"):
            div.decompose()

    def html_to_markdown(self, soup, current_url):
        try:
            markdown_content = markdownify(str(soup), heading_style="ATX")
            markdown_content = re.sub(r"(\n\s*){3,}", "\n", markdown_content)

            return f"Source URL: {current_url}\n\n{markdown_content}"
        except Exception as e:
            logging.error(f"Error converting HTML to Markdown: {e}")
            return soup.get_text()

    def normalize_url(self, url):
        parsed = urlparse(url)
        netloc = parsed.netloc.removeprefix("www.")
        return f"{netloc}{parsed.path}"

    def add_to_queue(self, url, depth):
        with self.queue_lock:
            self.url_queue.append((url, depth))

    def add_to_visited(self, url):
        with self.visited_lock:
            self.visited_urls.add(url)

    def is_visited(self, url):
        with self.visited_lock:
            return url in self.visited_urls

    def get_next_url(self):
        with self.queue_lock:
            return self.url_queue.popleft() if self.url_queue else None

    def add_to_scraper_info_and_s3(self, url, metadata, filetype, content):
        pass

    def remove_useless_elements(self, soup):
        # fmt: off
        for tag in soup.find_all(['script', 'style', 'nav', 'header', 'footer', 'form', 'iframe', 'aside', 'menu', 'navigate']):
            tag.decompose()
        useless_classes = ['footer', 'footnav', 'site-footer', 'page-footer', 'footer-container', 'footer-content', 'footer-links', 'header', 'head', 'site-header', 'page-header', 'header-container', 'top-header', 'navbar', 'nav', 'navigation', 'menu', 'site-nav', 'main-navigation']
        useless_ids = ['footer', 'foot', 'page-footer', 'header', 'head', 'top', 'navbar', 'nav', 'menu']
        # fmt: on

        for cls in useless_classes:
            for div in soup.find_all("div", class_=cls):
                div.decompose()

        for id_val in useless_ids:
            footer_div = soup.find("div", id=id_val)
            if footer_div:
                footer_div.decompose()

    def download_html(self, url, soup, depth, metadata):
        logging.info(f"Downloading HTML: {url}")
        inst_id = self.inst_id
        if depth != 0:
            self.remove_useless_elements(soup)
        try:
            filename = (
                f"{urlparse(url).netloc}_{urlparse(url).path.replace('/', '_')}.html"
            )
        except:
            filename = f"{inst_id}_{uuid4()}.html"
        try:
            page_title = soup.title.text if soup.title else url.split("/")[-1]
            s3_link = upload_html_to_s3(inst_id, str(soup), filename)
            doc = {
                "institute_id": inst_id,
                "actual_url": url,
                "s3_url": s3_link,
                "title": page_title,
                "status": True,
                "file_type": "html",
                "metadata": metadata,
            }
            self.scrape_data.append(doc)
        except Exception as e:
            logging.error(f"Error writing file {filename}: {e}")

    def scrape_url(self, url, depth):
        normalized_url = self.normalize_url(url)
        if self.is_visited(normalized_url):
            return None

        self.add_to_visited(normalized_url)
        logging.info(f"Visiting Url: {normalized_url}")
        html = self.fetch_page(url)
        if html is None:
            return None

        soup = self.parse_html(html)
        self.clean_soup(soup)
        soup = self.replace_relative_links(soup, url)
        logging.info(f"Length of soup: {len(str(soup))}")
        markdown = self.html_to_markdown(soup, url)
        soup_upload = BeautifulSoup(html, "lxml")
        self.remove_comments(soup_upload)
        if self.llm_integrator:
            current_json, current_empty_fields = self.get_current_json_data()

            # Process markdown with the current data
            updated_fields, new_urls, pdf_urls = (
                self.llm_integrator.process_markdown_scraper(
                    markdown, current_json, current_empty_fields
                )
            )
            updated_fields, metadata = self.llm_integrator.process_markdown_details(
                markdown, current_json, current_empty_fields
            )
            new_urls = [
                (
                    urljoin(url, new_url)
                    if not new_url.startswith(("http://", "https://", "//"))
                    else new_url
                )
                for new_url in new_urls
                if not new_url.lower().rstrip("/").endswith(".pdf")
            ]

            pdf_urls = [
                (
                    urljoin(url, pdf_url)
                    if not pdf_url.startswith(("http://", "https://", "//"))
                    else pdf_url
                )
                for pdf_url in pdf_urls
            ]
            self.download_html(url, soup_upload, depth, metadata)
            for pdf_url in pdf_urls:
                if not self.normalize_url(pdf_url) in self.downloaded_pdf:
                    self.download_pdf(pdf_url)

            self.update_json_data(updated_fields)

            # Only add URLs provided by the LLM to the queue
            if depth < self.max_depth:
                for new_url in new_urls:
                    if not self.is_visited(self.normalize_url(new_url)):
                        self.add_to_queue(new_url, depth + 1)

        time.sleep(self.rate_limit)  # Rate limiting
        return normalized_url, markdown

    def run(self, max_pages=10, resume=False):

        scraped_data = {}
        while True:
            if len(scraped_data) >= max_pages:
                logging.info("Reached max pages")
                break

            next_url = self.get_next_url()
            if not next_url:
                logging.info("No more links to process")
                break
            url, depth = next_url
            if not self.is_visited(url):
                result = self.scrape_url(url, depth)
                if result:
                    scraped_url, markdown = result
                    scraped_data[scraped_url] = markdown
                    logging.info(f"Scraped: {scraped_url}")
                    logging.info(f"Current queue size: {len(self.url_queue)}")

            if len(scraped_data) >= max_pages:
                logging.info("Reached max pages")
                break
        self.add_scrape_data()
        logging.info(
            f"Scraping Complete. Scraped {len(scraped_data)} pages. Current queue size: {len(self.url_queue)}"
        )
        if self.llm_integrator:
            document = {
                "inst_id": self.inst_id,
                "doctorate_degrees": self.json_data.get("doctorate_degrees", []),
                "diploma_degrees": self.json_data.get("diploma_degrees", []),
                "postgraduate_specializations": self.json_data.get(
                    "postgraduate_specializations", []
                ),
                "postgraduate_degrees": self.json_data.get("postgraduate_degrees", []),
                "undergraduate_specializations": self.json_data.get(
                    "undergraduate_specializations", []
                ),
                "undergraduate_degrees": self.json_data.get(
                    "undergraduate_degrees", []
                ),
            }
            try:
                # Index the document
                response = es.index(index="scraper_degrees", body=document)
                print(f"Document indexed successfully: {response['result']}")
            except Exception as e:
                print(f"Error indexing document: {str(e)}")

            ##TODO SAVE THESE TO A NEW INDEX IN ELASTICSEARCH

            # total_tokens = self.llm_integrator.get_total_tokens_used()
            # input_tokens = self.llm_integrator.get_input_tokens_used()
            # output_tokens = self.llm_integrator.get_output_tokens_used()
            # output_cost = (output_tokens / 1_000_000) * 0.600
            # input_cost = (input_tokens / 1_000_000) * 0.150
            # total_cost = output_cost + input_cost

            # logging.info(f"Total tokens used: {total_tokens}")
            # print(f"Total tokens used: {total_tokens}")
            # print(f"Input tokens used: {input_tokens}")
            # print(f"Output tokens used: {output_tokens}")

            # # Print costs
            # print(f"Output cost: ${output_cost:.6f}")
            # print(f"Input cost: ${input_cost:.6f}")
            # print(f"Total cost: ${total_cost:.6f}")

        return scraped_data, self.json_data
