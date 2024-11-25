from markdownify import markdownify as md
from urllib.parse import urlparse
from bs4 import BeautifulSoup, Comment
from playwright.async_api import  TimeoutError as PlaywrightTimeoutError
import nest_asyncio
import re
nest_asyncio.apply()
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from openai import AzureOpenAI
import json
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from requests.exceptions import Timeout, ConnectionError
import logging
import os
from dotenv import load_dotenv

load_dotenv()


m_list = list

class AzureOpenAIClient:
    def __init__(self, api_key: str, endpoint: str, api_version: str):
        self.client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=endpoint,
            api_version=api_version,
        )

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((Timeout, ConnectionError)),
        reraise=True
    )
    def chat_completions(self, model: str, messages: list, temperature: float = 0) -> dict:
        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
                timeout=120
            )
            return response
        except Exception as e:
            logging.error(f"Error sending request to LLM: {e}")
            raise

    def chat_completions_with_retry(self, model: str, messages: list, temperature: float = 0) -> dict:
        try:
            return self.chat_completions(model, messages, temperature)
        except Exception as e:
            logging.error(f"Max retries reached. Unable to get response from LLM. Error: {e}")
            return None

api_key = os.getenv("AZURE_4OMINI_KEY")
gpt_4O_mini_url = os.getenv("AZURE_4OMINI_ENDPOINT")

endpoint, model, api_version = gpt_4O_mini_url, "gpt-4o-mini", "2023-03-15-preview"
client = AzureOpenAIClient(api_key=api_key, endpoint=endpoint, api_version=api_version)



def search_ddg_with_markdown(query):
    ignored_names = [
        "www.shiksha.com", "justdial", "sulekha", 'collegedekho', "collegedunia", "facebook", "zollege", "entrancezone", "collegewiki",
        "twitter", "linkedin", "youtube", "instagram", "wikipedia", "quora", "glassdoor", "indianexpress", "indcareer",
        "mouthshut", "careers360", "collegesearch", "collegespace", "collegesniper", "getmyuni", "mycindia",
        "firstuni", "google", "pharmaadmission", "careers360", "collegesearch", "collegebatch",
        "indiastudychannel", "mykollege", "collegetour", "free-apply", "collegeindia", "teachersbadi",
        "educationdunia.com","thehindu","universitydunia","vidyavision","campusoption","targetadmission",
        "careerurl","studyclap", "getadmission", "vidyatime", "entranceindia", "estudentbook", "getadmision",
        "educationonclick", "bharateducation", "youth4work", "jagranjosh", "click4college", "facebook","sedo.com","educrib",
        "indoafrica","aptcareercounsellors","indiatoday","best_colleges","pureboardingschools","collegeadmission",
        "aajtak","edufever","targetstudy.com","colleges-india","careerindia","education.kerala.gov.in","highereducationinindia.com",
        "universitykart","getmyuni","thehighereducationreview","educationforallinindia","advocatekhoj","studybscnursinginbangalore","unipune", "cnn.com", "contactout.com","deccanherald", "cbsnews", "indeed.com", "duckduckgo.com"
    ]
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(ignore_https_errors=True)
            
            page.goto('https://duckduckgo.com')
            page.fill('input[name="q"]', query)
            page.press('input[name="q"]', 'Enter')
            page.wait_for_selector('.react-results--main')
            links = page.eval_on_selector_all('.react-results--main a', 'elements => elements.map(el => el.href)')
            filtered_links = [link for link in links if not any(ignored_name in link for ignored_name in ignored_names)]
            
            clean_link = []
            for link in filtered_links:
                if link not in clean_link:
                    clean_link.append(link)
            l = min(3, len(clean_link))
            first_three_links = clean_link[:l]
            browser.close()
            return first_three_links
    except Exception as e:
        logging.error(f"Failed to find any link on DUCKDUCKGO for institute {query}, {e}")


def get_first_clean_url(query):
    ignored_names = [
        "www.shiksha.com", "justdial", "sulekha", 'collegedekho', "collegedunia", "facebook", "zollege", "entrancezone", "collegewiki",
        "twitter", "linkedin", "youtube", "instagram", "wikipedia", "quora", "glassdoor", "indianexpress", "indcareer",
        "mouthshut", "careers360", "collegesearch", "collegespace", "collegesniper", "getmyuni", "mycindia",
        "firstuni", "google", "pharmaadmission", "careers360", "collegesearch", "collegebatch",
        "indiastudychannel", "mykollege", "collegetour", "free-apply", "collegeindia", "teachersbadi",
        "educationdunia.com","thehindu","universitydunia","vidyavision","campusoption","targetadmission",
        "careerurl","studyclap", "getadmission", "vidyatime", "entranceindia", "estudentbook", "getadmision",
        "educationonclick", "bharateducation", "youth4work", "jagranjosh", "click4college", "facebook","sedo.com","educrib",
        "indoafrica","aptcareercounsellors","indiatoday","best_colleges","pureboardingschools","collegeadmission",
        "aajtak","edufever","targetstudy.com","colleges-india","careerindia","education.kerala.gov.in","highereducationinindia.com",
        "universitykart","getmyuni","thehighereducationreview","educationforallinindia" ,"advocatekhoj","studybscnursinginbangalore","unipune", "cnn.com", "contactout.com", "deccanherald", "cbsnews", "indeed.com"
    ]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(ignore_https_errors=True)

        try:
            page.goto(f"https://www.google.com/search?q={query}", wait_until='networkidle')
            results = page.query_selector_all('div.yuRUbf')
            for result in results:
                a_tag = result.query_selector('a')
                url = a_tag.get_attribute('href')
                if url and not any(name in url for name in ignored_names):
                    domain = urlparse(url).scheme + "://" + urlparse(url).netloc
                    return domain

            return None
        except Exception as e:
            logging.error(f"Failed to Find {query} on Goggle Search, {e}")
            return None

        finally:
            browser.close()


def get_html_of_url(url, user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"):
    """
    Fetches the HTML content of a page using Playwright with retries on failure.

    :param url: The URL of the page to fetch.
    :param user_agent: Custom user agent to mimic a browser. Default is set to Chrome 91.
    :return: The HTML content of the page or None if an error occurs after retries.
    """
    logging.info(f"Downloading URL:- {url}")
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=user_agent, ignore_https_errors=True)
            page = context.new_page()
            try:
                page.goto(url, wait_until="networkidle", timeout=80000, )
            except PlaywrightTimeoutError:
                logging.info(f"Timeout occurred when trying to fetch {url}")
                return None
            content = page.content()
            page.close()
            browser.close()
            logging.info(f"Done downloading URL {url}")
            return content
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")



def create_soup_and_markdown(soup):
    [s.decompose() for s in soup("script")]
    for tag in soup(
        ["script", "style", "link", "meta", "input", "form", "noscript", "img", "svg", "button", "aside", "figure", "fielset", "details", "textarea", "fieldset"]
    ):
        tag.decompose()
    for element in soup.find_all(string=lambda text: isinstance(text, Comment)):
        element.extract()
    for div in soup.find_all("div", style="display:none;"):
        div.decompose()

    for element in soup(['script', 'style', 'nav', 'footer']):
        element.decompose()

    for div in soup.find_all("div", style="display:none;"):
            div.decompose()  
    
    if soup:
        return md(str(soup), heading_style="ATX")
    else: return ""


def analyze_with_gpt4_j(query, content):

    
    example = '''
    {
        "https://example1.edu": {
            "is_correct_website": true,
            "relevance_score": 0.95
        },
        "https://example2.edu": {
            "is_correct_website": false,
            "relevance_score": 0.3
        },
        "https://example3.edu": {
            "is_correct_website": false,
            "relevance_score": 0.1
        }
    }
    '''

    prompt = f"""You are an intelligent assistant responsible for evaluating website content to identify the official website of a specific college or university. Given the following content separated by '\n||\n', analyze each website and determine if it is the official website of the institute identified by the placeholder {query}.

    The content format is as follows:
    - The first line of each section contains the URL of the website.
    - The subsequent lines provide descriptive content about that website.

    Your tasks are:
    1. Assess whether each URL belongs to the official website of the institute specified by {query}.
    2. Ensure the website is neither a government aggregator nor a third-party aggregator site.
    3. Assign a relevance score between 0 and 1 to each URL based on its accuracy and relevance to the institute.

    Provide the results in JSON format with the following structure:
    - Use the URL as the outermost key.
    - Each URL must contain:
    - "is_correct_website": a boolean indicating whether this is the official website of the institute.
    - "relevance_score": a float representing the relevance score.

    For example, the output should look like this:
    {example}

    """

    messages = [
        {"role": "system", "content": "You are a helpful assistant that analyzes web content."},
        {"role": "user", "content": prompt},
        {"role": "user", "content": content}
    ]

    response = client.chat_completions(model=model, messages=messages, temperature=0)
    if response:
        try:
            json_content = response.choices[0].message.content
            json_content = json_content.strip("```json").strip("```").strip()
            json_content = json.loads(json_content)
            json_content['tokens'] = json_content
            return json_content
        except Exception as e:
            logging.error("Failed to get result from LLM", e)
            return {}

    else:
        return {"error": "Failed to get a response from the model."}



def extract_official_url(json_content):
    logging.info(json_content)
    
    best_url = ''
    max_relevance_score = 0

    for url, data in json_content.items():
        if url != 'tokens' and data.get('is_correct_website'):
            relevance_score = data.get('relevance_score', 0)
            if relevance_score > max_relevance_score:
                max_relevance_score = relevance_score
                best_url = url

    return best_url


def url_recommend_gpt_ask(name, city, state):
    try:
        prompt = f"""Provide the official website link for a college based on the following details:\nCollege Name: {name}\nCity: {city}\nState: {state}. Ensure the link is accurate and corresponds to the correct institution. 
        **NOTE**: Don't write anything else except the correct website link."""

        response = client.chat_completions(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an intelligent assistant that helps users find official website links for colleges in India. When given a college's name, city, and state, you must return the official website link. Ensure accuracy by only providing valid and trusted URLs that correspond to the college mentioned."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        url = response.choices[0].message.content
        return url
    except:
        logging.error(f"Failed To Find URL for Institute {name} using GPT")


def url_recommended(name, city, state):
    college_name = name
    city_name = city
    state_name = state
    clean_url3 = []

    query=f"{college_name},{city_name}, {state_name}".replace("&", "and")
    
    clean_url3=search_ddg_with_markdown(query) or []
    google_url=get_first_clean_url(query)
    gpt_url = url_recommend_gpt_ask(name, city, state)
    clean_url3.append(google_url)
    clean_url3.append(gpt_url)
    while None in clean_url3:
        clean_url3.remove(None)
    mylist = []
    for link in clean_url3:
        if "/"==link[-1]:
            link = link[:-1]
            mylist.append(link)
        else:
            mylist.append(link)

    clean_url3 = m_list(set(mylist))

    logging.info(f"Final URLS LIST: {clean_url3}")
    
    if clean_url3==[]:
        logging.info(f"No URLs found for: {query}")
        return [],""
    else:
        results=[]
        for url in clean_url3:
            result = process_url(url)
            results.append(result)
        list = [result for result in results if result]
        result = '\n||\n'.join(list)
        if result:
            json_content = analyze_with_gpt4_j(query, result)
            return clean_url3, extract_official_url(json_content)
        return clean_url3, ""

def process_url(url):
    html = get_html_of_url(url)
    if html is None:
        logging.error(f"Failed to url_recommend_retrieve content for URL: {url}")
        return None

    html = BeautifulSoup(html, 'html.parser')
    html = create_soup_and_markdown(html)
    markdown = re.sub(r'\n{2,}', '\n', html)
    markdown = re.sub(r"\|", "", markdown)

    return f"WEBSITE NO:\n\nCONTENT FIELD: \n Source URL: {url}\n \n{markdown}"

