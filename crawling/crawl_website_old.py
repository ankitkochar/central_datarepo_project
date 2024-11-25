import asyncio
import os
from bs4 import BeautifulSoup, Comment
from urllib.parse import urlparse, urljoin, urlunparse, quote
import pandas as pd
import re
from datetime import datetime
import difflib
from elasticsearch import Elasticsearch
from collections import deque
from aiohttp_retry import RetryClient, ExponentialRetry
from uuid import uuid4
import logging
from dotenv import load_dotenv
from playwright.async_api import async_playwright
import tldextract
from utils.s3_utils import save_pdf_to_s3, upload_html_to_s3


load_dotenv()
temp_dw_folder = os.environ.get("TEMP_DW_FOLDER")
log_files_folder = os.environ.get("LOG_FILES_FOLDER")
es_host = os.getenv("ELASTIC_SEARCH_HOST")
auto_run_index = os.environ.get("AUTO_RUN_INDEX")
es_user = os.getenv("ELASTICSEARCH_USER")
es_password = os.getenv("ELASTICSEARCH_PASSWORD")

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


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}
# fmt: off
keywords = [ "brochure", "mandatory disclosure", "accreditation", "ranking", "iqac", "aqar", "nirf", "course", "class", "curriculum", "syllabus", "programme", "program", "fees", "tuition", "financial", "cost", "placement", "career", "internship", "employment", "job opportunities", "prospectus", "disclosure", "admission", "eligibility", "scholarship", "fee structure", "fee",]
# fmt: on
keyword_regex = re.compile(r"(" + "|".join(keywords) + r")", re.IGNORECASE)

negative_keywords = ["tender", "fest"]

negative_keywords_regex = re.compile(
    r"(" + "|".join(negative_keywords) + r")", re.IGNORECASE
)
# fmt: off
year_pattern = re.compile(r"20(00|0[1-9]|1[0-9])")
ignored_extensions = { ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg", ".tif", ".tiff", ".ico", ".webp", ".heif", ".heic", ".raw", ".psd", ".indd", ".ai", ".eps", ".pdf", ".mp4", ".m4a", ".mov", ".avi", ".wmv", ".flv", ".mkv", ".mpeg", ".mpg", ".webm", ".vob", ".ogv", ".m4v", ".3gp", ".3g2", ".f4v", ".f4p", ".f4a", ".f4b", ".mp3", ".wav", ".aac", ".ogg", ".wma", ".flac", ".alac", ".aiff", ".au", ".m4b", ".m4p", ".mp2", ".mpa", ".amr", ".pcm", ".mid", ".midi", ".oga", ".opus", ".exe", ".bat", ".cmd", ".sh", ".bin", ".app", ".vb", ".vbs", ".jar", ".py", ".pl", ".rb", ".swf", ".gadget", ".msi", ".scr", ".htaccess", ".dll", ".so", ".dylib", ".apk", ".pif", ".gadget", ".wsf", ".rar", ".zip", ".7z", ".7zip", ".tar", ".sit", ".arc", ".arj", ".bz2", ".cab", ".gz", ".iso", ".lha", ".lzh", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".odt", ".ods", ".odp", ".rtf", ".txt", ".ppsx",}  # Extendable list of file extensions to ignore
# fmt: on
download_file_types = [".pdf", ".doc", ".docx"]


async def is_relevant_content(text):
    match = keyword_regex.search(text)
    return match.group(0) if match else None


async def check_negative_keywords(text):
    match = negative_keywords_regex.search(text)
    return True if match else False


def add_scrape_data(scrape_data):
    url_added = []

    for data in scrape_data:
        actual_url = data["actual_url"]
        if actual_url not in url_added:
            es.index(index="scraper_info", body=data)
            # print(f"Added {actual_url} to DB.")
            url_added.append(actual_url)


async def normalize_url(url):
    """
    Normalizes the given URL to ensure consistency and standardization. This includes making sure the URL uses HTTPS,
    starts with 'www.', and that the path is URL-encoded to handle special characters and spaces.

    Args:
    url (str): The URL to normalize.

    Returns:
    str: The normalized URL.
    """
    parsed_url = urlparse(url)
    scheme = parsed_url.scheme if parsed_url.scheme else "https"
    netloc = parsed_url.netloc.lower()

    if not netloc and parsed_url.path:
        netloc, _, path = parsed_url.path.partition("/")
        path = "/" + path
    else:
        path = parsed_url.path

    if not netloc.startswith("www."):
        netloc = "www." + netloc

    path = quote(
        path
    )  # Ensure the path is URL-encoded to handle spaces and special characters

    if path.endswith("/"):
        path = path.strip("/")  #  consistency

    normalized_url = parsed_url._replace(scheme=scheme, netloc=netloc, path=path)
    return urlunparse(normalized_url)


async def download_file(
    client, pdf_url, inst_id, current_depth, url, downloaded, scrape_data, retry=False
):
    """
    Attempts to download a file from the specified URL if it's not already downloaded or outdated. Adds the file
    information to `pdf_details` if the download is successful.

    Args:
    client (aiohttp.ClientSession): The HTTP client session object.
    pdf_url (str): URL of the PDF to download.
    inst_id (str): Identifier for the institution.
    pdf_folder (str): Directory path to save the downloaded PDFs.
    pdf_details (list): List to store details of downloaded files.
    current_depth (int): Current depth in the link exploration.
    url (str): The URL from where the file link was extracted.
    downloaded (set): Set of URLs that have been already downloaded.
    """
    logging.info(f"Downloading file: {pdf_url}")
    file_name = f"{inst_id}_" + os.path.basename(pdf_url)

    # pdf_name_match = keyword_regex.search(pdf_name)
    if year_pattern.search(file_name) or year_pattern.search(pdf_url):
        return
    if pdf_url.removeprefix("https://").removeprefix("http://") in downloaded:
        return

    file_type = pdf_url.strip("/").split(".")[-1].lower()
    file_save_name = f"{inst_id}_{uuid4()}.{file_type}"
    try:
        async with client.get(pdf_url) as response:
            if response.status == 200:

                content = await response.read()
                downloaded.add(pdf_url.removeprefix("https://").removeprefix("http://"))

                last_modified = response.headers.get("Last-Modified", None)
                if last_modified:
                    try:
                        last_modified_date = datetime.strptime(
                            last_modified, "%a, %d %b %Y %H:%M:%S %Z"
                        )
                        if last_modified_date < datetime(2018, 1, 1):
                            return
                    except:
                        pass

                content_length = response.headers.get("Content-Length")
                s3_link = save_pdf_to_s3(pdf_url, inst_id, content)
                doc = {
                    "institute_id": inst_id,
                    "actual_url": pdf_url,
                    "s3_url": s3_link,
                    "title": pdf_url.split("/")[-1].lower().split(".")[0],
                    "status": True,
                    "file_type": "pdf",
                }
                scrape_data.append(doc)

    except Exception as e:
        # TODO add logging
        logging.error(
            f"Failed to download file {pdf_url}: {e}  \n file found on : {url}"
        )
        if retry == False:
            await download_file(
                client,
                pdf_url,
                inst_id,
                current_depth,
                url,
                downloaded,
                scrape_data,
                retry=True,
            )
        # print(f"Failed to download file {pdf_url}: {e}  \n file found on : {url}")


async def remove_comments(soup):
    for element in soup.findAll(string=lambda text: isinstance(text, Comment)):
        element.extract()


async def get_unique_content(current_text, base_text):
    # Find unique content using Difflib
    differ = difflib.Differ()
    diff = list(differ.compare(base_text.splitlines(), current_text.splitlines()))
    unique_lines = [
        line[2:]
        for line in diff
        if line.startswith("+ ") and not line.startswith("+ -")
    ]
    return " ".join(unique_lines)


async def process_url(
    client,
    browser,
    url,
    homepage_text,
    inst_id,
    current_depth,
    max_depth,
    queue,
    visited,
    downloaded,
    base_domain,
    enable_javascript,
    scrape_data,
):
    if current_depth > max_depth:
        logging.info(f"Max depth reached for URL {url}")
        return
    try:
        if enable_javascript:

            page = await browser.new_page()
            try:
                try:
                    await page.goto(url, wait_until="load", timeout=25000)
                    response = await page.content()
                    await page.close()
                except:
                    response = await page.content()
                    await page.close()

                soup = BeautifulSoup(response, "html.parser")
            except Exception as e:
                await page.close()
                logging.error(f"Error processing URL {url}: {e}")
                return
            finally:
                await page.close()
        else:
            async with client.get(url, headers=headers) as response:
                if response.status != 200:
                    logging.error(
                        f"Failed to download page {url}. Status code: {response.status}"
                    )
                    return
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
        await remove_comments(soup)
        await download_html(url, soup, inst_id, current_depth, scrape_data)

        current_text = soup.get_text()
        if current_depth != 1:
            unique_text = await get_unique_content(current_text, homepage_text)
        else:
            unique_text = current_text

        page_title = soup.title.text if soup.title else ""

        if year_pattern.search(
            page_title
        ):  # or negative_keywords_regex_title.search(page_title):
            # Optionally print a message or log this event
            logging.info(f"Skipping page '{url}' because it matches the year pattern.")
            return
        if current_depth < max_depth:
            for link in soup.find_all("a", href=True):
                href = urljoin(url, link["href"])
                href = await normalize_url(href)
                # print(f"Found link: {href}")
                if (
                    not any(href.lower().endswith(ext) for ext in ignored_extensions)
                    and "javascript:" not in href
                    and "mailto:" not in href
                    and "#" not in href
                    and href not in visited
                    and href not in queue
                ):
                    domain_base = tldextract.extract(base_domain).registered_domain
                    domain_current = tldextract.extract(href).registered_domain
                    if domain_base == domain_current:
                        logging.info(f"Adding link to queue: {href}")
                        queue.append((href, current_depth + 1))
                    else:

                        logging.info(
                            f"Skipping external link: {href}. Base domain: {domain_base} current: {domain_current}"
                        )
                        pass
                        # print(f"Skipping external link: {href}. Base domain: {base_domain} netloc: {urlparse(href).netloc}")
        text_match = await is_relevant_content(unique_text)
        title_match = await is_relevant_content(page_title)
        webpage_match = text_match or title_match

        for link in soup.find_all("a", href=True):
            href = urljoin(url, link["href"])
            href = await normalize_url(href)
            pdf_name = href.strip("/").split("/")[-1]
            pdf_match = await is_relevant_content(pdf_name)
            negative_keywords_check = await check_negative_keywords(
                pdf_name
            ) or await check_negative_keywords(href)
            if (webpage_match or pdf_match) and not negative_keywords_check:
                await process_file_link(
                    client, href, inst_id, current_depth, url, downloaded, scrape_data
                )
        for iframe in soup.find_all("iframe"):
            iframe_src = iframe.get("src")
            iframe_src = await normalize_url(iframe_src)
            pdf_name = iframe_src.strip("/").split("/")[-1]
            pdf_match = await is_relevant_content(pdf_name)
            negative_keywords_check = await check_negative_keywords(
                pdf_name
            ) or await check_negative_keywords(iframe_src)

            if (webpage_match or pdf_match) and not negative_keywords_check:
                await process_file_link(
                    client,
                    iframe_src,
                    inst_id,
                    current_depth,
                    url,
                    downloaded,
                    scrape_data,
                )

    except Exception as e:
        # TODO Add logging
        logging.error(f"Error processing URL {url}: {e}")

        # print(f"Error processing URL {url}: {e}")


async def process_file_link(
    client, link, inst_id, current_depth, url, downloaded, scrape_data
):
    ###TODO More filters, js etc etc remove. consider cases where "?"  present after .<file_ext>
    link = link.split("?", 1)[0]

    # logging.info(f"Processing file link: {link}")
    if (
        link
        and any(link.lower().endswith(ext) for ext in download_file_types)
        and "#" not in link
        and "?" not in link
        and "mailto:" not in link
        and link not in downloaded
    ):
        logging.info(f"File passed check: {link}")
        link = await normalize_url(link)
        await download_file(
            client,
            link,
            inst_id,
            current_depth,
            url,
            downloaded=downloaded,
            scrape_data=scrape_data,
        )


async def remove_useless_elements(soup):
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


async def download_html(url, soup, inst_id, depth, scrape_data):
    logging.info(f"Downloading HTML: {url}")
    if depth != 0:
        await remove_useless_elements(soup)
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
        }
        scrape_data.append(doc)
    except Exception as e:
        logging.error(f"Error writing file {filename}: {e}")


async def main_operation(
    client,
    browser,
    start_url,
    inst_id,
    max_depth,
    max_concurrent_tasks,
    queue,
    visited,
    base_domain,
    downloaded,
    enable_javascript,
    scrape_data,
):
    tasks = []
    homepage_response = await client.get(start_url)
    homepage_soup = BeautifulSoup(await homepage_response.text(), "html.parser")
    await remove_comments(homepage_soup)
    homepage_text = homepage_soup.get_text()
    while queue or tasks:
        while queue and len(tasks) < max_concurrent_tasks:
            current_url, current_depth = queue.popleft()
            if current_url not in visited:
                visited.add(current_url)
                if current_depth <= max_depth:
                    logging.info(f"crawling {current_url} at depth {current_depth}")

                    task = asyncio.create_task(
                        process_url(
                            client,
                            browser,
                            current_url,
                            homepage_text,
                            inst_id,
                            current_depth,
                            max_depth,
                            queue,
                            visited,
                            downloaded=downloaded,
                            base_domain=base_domain,
                            enable_javascript=enable_javascript,
                            scrape_data=scrape_data,
                        )
                    )
                    tasks.append(task)

        if not queue or len(tasks) >= max_concurrent_tasks:
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            tasks = list(
                pending
            )  # Reassign tasks to pending tasks after some have completed
    if tasks:
        await asyncio.gather(*tasks)  # Ensure all remaining tasks are completed


async def download_documents_htmls_async_bfs(
    url,
    inst_id,
    root_files_folder=temp_dw_folder,
    enable_javascript=True,
    max_depth=4,
    max_concurrent_tasks=10,
):
    start_url = await normalize_url(url)
    nor_url = await normalize_url(url)
    base_domain = urlparse(nor_url).netloc
    retry_options = ExponentialRetry(attempts=3)
    scrape_data = []

    async with RetryClient(
        raise_for_status=False, retry_options=retry_options
    ) as client, async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(ignore_https_errors=True)
        queue = deque([(start_url, 0)])
        visited = set()
        downloaded = set()
        try:
            await asyncio.wait_for(
                main_operation(
                    client,
                    context,
                    start_url,
                    inst_id,
                    max_depth,
                    max_concurrent_tasks,
                    queue,
                    visited,
                    base_domain,
                    downloaded,
                    enable_javascript,
                    scrape_data,
                ),
                timeout=1200,
            )
        except asyncio.TimeoutError:
            logging.info(
                f"Reached 20 minutes timeout for instid {inst_id}, saving data and exiting."
            )
            # add_scrape_data(scrape_data)
        except Exception as e:
            logging.error(f"An error occurred for instid {inst_id}: {str(e)}")

        finally:
            await browser.close()
            add_scrape_data(scrape_data)

    return True
