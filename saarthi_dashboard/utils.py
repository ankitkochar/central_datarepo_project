# Library
import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from elasticsearch import Elasticsearch
from datetime import datetime
from dotenv import load_dotenv
import logging
import re
import json
from markdownify import markdownify
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright
from openai import AzureOpenAI
import requests
from urllib.parse import quote
import pandas as pd

# Initialization
load_dotenv()
es_user = os.getenv("ELASTICSEARCH_USER")
es_password = os.getenv("ELASTICSEARCH_PASSWORD")
es_host = os.getenv("ELASTIC_SEARCH_HOST")
es = Elasticsearch(es_host, basic_auth=(es_user, es_password))
azure_endpoint = os.getenv("AZURE_4OMINI_ENDPOINT")
azure_key = os.getenv("AZURE_4OMINI_KEY")

client = AzureOpenAI(
        api_key=azure_key,
        azure_endpoint=azure_endpoint,
        api_version="2023-03-15-preview",
)


# Helper functions
async def make_function_async(function_to_call, data=None):
    with ThreadPoolExecutor() as pool:
        result = await asyncio.get_running_loop().run_in_executor(
            pool, function_to_call, data
        )
    return result


def update_fields_in_es(index_name, doc_id, key, value):
    update_body = {"doc": {key: value}}

    es.update(index=index_name, id=doc_id, body=update_body)
    logging.info(f"Update {key} to {value} in index: {index_name}")


def fetch_from_inst_master_course(id):
    query = {"query": {"match": {"inst_id": id}}}
    res = es.search(index="inst_master_courses", body=query)
    data = res["hits"]["hits"]
    return data


def fetch_from_inst_specific_course(id):
    query = {"query": {"match": {"inst_course_id": id}}}
    res = es.search(index="inst_specific_courses", body=query)
    data = res["hits"]["hits"]
    return data


def add_prompt(prompt_obj):
    doc = {
        **prompt_obj,
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
    }
    response = es.index(index="prompts", body=doc)
    return response


def get_all_prompts():

    response = es.search(index="prompts", size=10000)
    hits = response["hits"]["hits"]

    prompts = []

    for hit in hits:
        prompt = hit["_source"]
        id = hit["_id"]
        prompts.append({"id": id, **prompt})

    return prompts


def delete_prompt(doc_id):
    es.delete(index="prompts", id=doc_id)
    return f"Successfully Delete Prompt with _id: {doc_id}"


def update_prompt(doc_id, updated_obj):
    update_body = {
        "doc": {
            **updated_obj,
            "updated_at": datetime.now(),
        }
    }
    es.update(index="prompts", id=doc_id, body=update_body)
    return f"Successfully updated Prompt of _id: {doc_id} with {update_body}"

def fetch_all_cld_ids(index_name):
    query = {
        "_source": ["cld_id"],
        "query": {
            "match_all": {}
        }
    }

    cld_ids = []
    try:
        response = es.search(index=index_name, body=query, scroll='2m', size=1000)
        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']

        while hits:
            for hit in hits:
                cld_ids.append(hit["_source"]["cld_id"])
            response = es.scroll(scroll_id=scroll_id, scroll='2m')
            scroll_id = response['_scroll_id']
            hits = response['hits']['hits']

        es.clear_scroll(scroll_id=scroll_id)
        
        return cld_ids

    except Exception as e:
        logging.error(f"Error fetching cld_ids from institute index: {e}")
        return []

def fetch_for_recommended_run(index_name):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"recommended_run": False}}, 
                    {"term": {"status": True}}
                ]
            }
        }
    }

    all_rows = []
    try:
        response = es.search(index=index_name, body=query, scroll='2m', size=1000)
        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']

        while hits:
            for hit in hits:
                all_rows.append(hit["_source"])  # Fetch all the document fields
            response = es.scroll(scroll_id=scroll_id, scroll='2m')
            scroll_id = response['_scroll_id']
            hits = response['hits']['hits']

        es.clear_scroll(scroll_id=scroll_id)
        
        return all_rows

    except Exception as e:
        logging.error(f"Error fetching documents from {index_name} index: {e}")
        return []


def update_institute_fields(index_name, institute_id, institute_name, institute_url):
    try:
        # Define the query to find documents with the matching cld_id
        query = {
            "query": {
                "term": {
                    "cld_id": institute_id
                }
            }
        }

        # Define the update body with the fields to update
        update_body = {
            "script": {
                "source": """
                    ctx._source.name = params.institute_name;
                    ctx._source.url = params.institute_url;
                    ctx._source.downloaded = params.downloaded;
                    ctx._source.embedding_generated = params.embedding_generated;
                    ctx._source.prompt_output_generated = params.prompt_output_generated;
                    ctx._source.status = params.status;
                    ctx._source.updated_at = params.updated_at;
                """,
                "lang": "painless",
                "params": {
                    "institute_name": institute_name,
                    "institute_url": institute_url,
                    "downloaded": False,
                    "embedding_generated": False,
                    "prompt_output_generated": False,
                    "status": True,
                    "updated_at": datetime.now()
                }
            }
        }

        # Perform the update by query operation
        response = es.update_by_query(index=index_name, body={"query": query["query"], "script": update_body["script"]})
        logging.info(f"Updated {response['updated']} documents in the {index_name} index for institute Id {institute_id}.")

    except Exception as e:
        logging.error(f"Error updating institute index for institute Id {institute_id}: {e}")


def update_institute_field_for_reccomended_url_run(index_name, institute_id, institute_name, institute_url, institute_city, institute_state):
    try:
        query = {
            "query": {
                "term": {
                    "cld_id": institute_id
                }
            }
        }

        update_body = {
            "script": {
                "source": """
                    ctx._source.name = params.name;
                    ctx._source.url = params.url;
                    ctx._source.city = params.city;
                    ctx._source.state = params.state;
                    ctx._source.status = params.status;
                    ctx._source.recommended_run = params.recommended_run;
                    ctx._source.all_urls = params.all_urls;
                    ctx._source.final_url = params.final_url;
                    ctx._source.updated_at = params.updated_at;
                """,
                "lang": "painless",
                "params": {
                    "name": institute_name,
                    "url": institute_url,
                    "city": institute_city,
                    "state": institute_state,
                    "status": True,
                    "recommended_run": False,
                    "all_urls": [],
                    "final_url": "",
                    "updated_at": datetime.now()
                }
            }
        }

        response = es.update_by_query(index=index_name, body={
            "query": query["query"],
            "script": update_body["script"]
        })
        logging.info(f"Updated {response['updated']} documents in the {index_name} index for institute Id {institute_id}.")

    except Exception as e:
        logging.error(f"Error updating institute index for institute Id {institute_id}: {e}")

def update_urls_for_cld_id_for_recommended_url_institute(cld_id, all_urls, final_url, index_name='recommended_url_institute'):
    try:
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"cld_id": cld_id}}
                    ]
                }
            }
        }
        
        response = es.search(index=index_name, body=query)
        hits = response['hits']['hits']

        if not hits:
            logging.error(f"No document found with cld_id: {cld_id} and recommended_run: True in {index_name}")
            return False
        
        doc_id = hits[0]['_id']

        update_body = {
            "doc": {
                "all_urls": all_urls,
                "final_url": final_url,
                "recommended_run": True,
                "updated_at": datetime.now()
            }
        }

        es.update(index=index_name, id=doc_id, body=update_body)
        logging.info(f"Successfully updated cld_id: {cld_id} with new URLs and status")

        return True

    except Exception as e:
        logging.error(f"Error updating document with cld_id: {cld_id} in {index_name}: {e}")
        return False


def update_fields_in_institute_table(inst_id, downloaded, embedding_generated, prompt_output_generated=False):
    try:

        # Define the update script (use painless scripting)
        update_script = {
            "script": {
            "source": """
                ctx._source['embedding_generated'] = params.embedding_generated;
                ctx._source['downloaded'] = params.downloaded;
                ctx._source['prompt_output_generated'] = params.prompt_output_generated;
                """,
                "params": {
                    "embedding_generated": embedding_generated,
                    "downloaded": downloaded,
                    "prompt_output_generated" : prompt_output_generated
                }
            },
            "query": {
                "match": {
                    "cld_id": inst_id
                }
            }
        }

        # Execute the update by query request
        response = es.update_by_query(index="institute", body=update_script)
        return "Success"
    except Exception as e:
        print(f"Error updating institute status: {e}")
        return f"Failure: {e}"

def is_valid_institute_url(url):
    pattern = re.compile(
        r'^(?:http|ftp)s?://'
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'
        r'localhost|'
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|'
        r'\[?[A-F0-9]*:[A-F0-9:]+\]?)'
        r'(?::\d+)?'
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(pattern, url) is not None


def is_valid_institute_name(name):
    pattern = r"^[A-Za-z0-9 .,'&()-]+$"
    return bool(re.match(pattern, name))

def is_valid_institute_id(institute_id):
    institute_id = str(institute_id)
    pattern = r"^\d+$"
    return bool(re.match(pattern, institute_id))

def add_latest_news(inst_id, latest_news):
    existing_news= fetch_existing_news(inst_id)
    for news in latest_news:
        try:
            if not news['title'] in existing_news:
                doc = {
                    "inst_id" : inst_id,
                    "title" : news["title"],
                    "description" : news['description'],
                    "ref_link" : news['page_link'],
                    "source_url" : news['url'],
                    "tags" : ','.join(news['category']),
                    "status" : True,
                    "created_at" : datetime.now(),
                    "updated_at" : datetime.now(),
                }
                es.index(index='latest_news', body=doc)
                print(f"Added news: {news['title']} in DB")
            else:
                print(f"News Already Exists: {news['title']}")
        except Exception as e:
            print(e)

def fetch_existing_news(inst_id):
    query = {
        'query' : {
            'match' : {
                'inst_id' : inst_id
            }
        }
    }
    res = es.search(index="latest_news", body=query, size=10000)
    
    if len(res['hits']['hits'])> 0:
        existing_news = []
        data = res['hits']['hits']
        for d in data:
            existing_news.append(d['_source']['title'])

        return existing_news
    else:
        return []


def fetch_existing_refund_policy_links(inst_id):
    query = {
        'query' : {
            'match' : {
                'inst_id' : inst_id
            }
        }
    }
    res = es.search(index="refund_policy", body=query, size=10000)
    
    if len(res['hits']['hits'])> 0:
        existing_links = []
        data = res['hits']['hits']
        for d in data:
            existing_links.append(d['_source']['link'])

        return existing_links
    else:
        return []


def add_refund_policy_links(inst_id, refund_policy_links, inst_name):
    existing_refund_policy_links = fetch_existing_refund_policy_links(inst_id)
    for refund_policy in refund_policy_links:
        try:
            if not refund_policy in existing_refund_policy_links:
                doc = {
                    "inst_id" : inst_id,
                    "link" : refund_policy,
                    "inst_name" : inst_name,
                    "status" : True,
                    "created_at" : datetime.now(),
                    "updated_at" : datetime.now(),
                }
                es.index(index='refund_policy', body=doc)
                print(f"Added news: {refund_policy} in DB")
            else:
                print(f"Refund Policy Link Already Exists: {refund_policy}")
        except Exception as e:
            print(e)


def validate_url(url):
    try:
        encoded_url = quote(url, safe=':/?=&')
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(encoded_url, headers=headers, timeout=10, allow_redirects=True)
        if response.status_code == 200:
            return True
        else:
            return False
    except Exception as e:
        print(e)
        return False

def fetch_inst_url_and_name(inst_id):
    query = {
            "query": {
                "match": {
                    "cld_id": inst_id
                }
            }
    }
    response = es.search(index="institute", body=query)

    institute_data = response['hits']['hits'] 
    if len(institute_data) > 0:
        inst_url = institute_data[0]['_source']['url']
        inst_name = institute_data[0]['_source']['name']

        return inst_name, inst_url
    else:
        return "", ""

def html_to_markdown(html):
    soup = BeautifulSoup(html, "lxml")
    for table in soup.find_all("table"):
        table.insert_before(soup.new_string("[TABLE]"))
        table.insert_after(soup.new_string("[/TABLE]"))

    markdown_content = markdownify(str(soup), heading_style="ATX")
    markdown_content = re.sub(r"(\n\s*){3,}", "\n\n", markdown_content)

    return markdown_content


def ask_gpt(prompt, assistant_context):
    model = "gpt-4o-mini"
    input_tokens_used = 0
    output_tokens_used = 0

    response = client.chat.completions.create(
                    model=model,
                    messages=[
                        {
                            "role": "system",
                            "content": assistant_context,
                        },
                        {"role": "user", "content": prompt},
                    ],
                )

    input_tokens_used += response.usage.prompt_tokens
    output_tokens_used += response.usage.completion_tokens
    
    return response.choices[0].message.content

def get_links(markdown_content, institute_name):
    prompt = f"""
        Identity: "You are mimicking a human who is trying to get Latest Updates/News information regarding {institute_name} from their website.  You will be provided a markdown representation of a webpage.
        1. Your job is to: Identify any new URLs that should be visited/may contain information regarding the datapoints required pertaining to {institute_name}. Be very selective and conservative. Absolutely avoid links that have a low chance of containing information regarding the datapoints and {institute_name}.
        2. Datapoints for identifying URLs: Latest News, Latest Updates, Recent Updates, Latest News, Announcements
        If you find any URLs that pertain to {institute_name} and the datapoints provided, return them in the JSON response, even if they belong to a subdomain as long as they pertain to {institute_name}
        Also focus on individual degree/course urls.
        
        A few examples of how the output structure should look like like are listed below:
        ## BEGIN EXAMPLE OUTPUT:
        
        ### EXAMPLE OUTPUT:
        {{
        "new_urls": [
            "https://www.manipal.edu/mit/news-events",
            "https://happenings.lpu.in/unforgettable-magic-darshan-raval-concert-at-lpu",
            "https://iusikkim.edu.in/admissions/important-notices",
            "https://www.indiatoday.in/education-today/news",
            "https://blog.iiitd.ac.in/huntit-by-the-cultural-council-of-iiit-delhi/"
        ]
        }},
        ### END EXAMPLE OUTPUT

        ##END EXAMPLE OUTPUT
        
        ### INPUT MARKDOWN START ### : 
        
        {markdown_content}
        
        ### INPUT MARKDOWN END ###

    Do not create new fields except the ones shown to you in the example outputs. Be selective with the URLs you provide and focus on the datapoints listed above.
    """

    return ask_gpt(prompt,"You are a helpful assistant that extracts information from markdown content and updates JSON data. You only respond in JSON, without any extra lines outside of it.")

def get_refund_links(markdown_content, institute_name):

    prompt = f'''
        Identity: "You are mimicking a human who is trying to get Refund Policy/Process information regarding {institute_name} from their website.  You will be provided a markdown representation of a webpage.
        1. Your job is to: Identify any links/pdf's that should be visited/may contain information regarding the datapoints required pertaining to {institute_name}. Be very selective and conservative. Absolutely avoid links that have a low chance of containing information regarding the datapoints and {institute_name}.
        2. DataPoints for identifying links/pdf's: refund policy, refund process, hostel fee refund, online refund
        If you find any links/pdf's that pertain to {institute_name} and the datapoints provided, return them in the JSON response, even if they belong to a subdomain as long as they pertain to {institute_name}
        Also focus on individual degree/course urls.
        
        A few examples of how the output structure should look like like are listed below:
        ## BEGIN EXAMPLE OUTPUT:
        ### EXAMPLE OUTPUT 1:
        {{
        "links": [
            "https://www.manipal.edu/mu/admission/fees.html",
            "https://www.manipal.edu/content/dam/manipal/mu/documents/2024%20Prospectus.pdf",
        ]
        }},
        ### END EXAMPLE OUTPUT 1
        ### EXAMPLE OUTPUT 2:
        {{
        "links:: [
            "https://www.unigoa.ac.in/a/fee-refund.html",
            "https://www.unigoa.ac.in/academics/a/fee-refund.html",
            "https://www.unigoa.ac.in/about-us/a/fee-refund.html,
            ]
        }},
        ### END EXAMPLE OUTPUT 2
        ##END EXAMPLE OUTPUT 
        
        ### INPUT MARKDOWN START ### : 
        
        {markdown_content}
        
        ### INPUT MARKDOWN END ###

    Do not create new fields except the ones shown to you in the example outputs. Be selective with the links/pdf's you provide and focus on the datapoints listed above.
    '''

    return ask_gpt(prompt,"You are a helpful assistant that extracts information from markdown content and updates JSON data. You only respond in JSON, without any extra lines outside of it.")

def get_data(markdown, institute):

    prompt = f'''
    You are mimicking a person that went through a markdown representation of {institute} webpage and need to identify:
      1.1 Any Latest Updates/News/Announcements.
      1.2 The webpage link/pdf to which Latest Updates/News/Announcements is linked.
      1.3 Categories it can come under.
    
    If Any Updates/News/Announcements is given on the Texts Provided, You need to provide that information in a JSON format which is given below format field:

    Format :
    [
        {{
            "title" : "Today we have launched Saarthi",
            "description" : "CollegeDekho Launches SaarthiGPT: India's First Ever AI-Powered Guide for Higher Education.",
            "page_link" : "https://www.aninews.in/news/business/collegedekho-launches-saarthigpt-indias-first-ever-ai-powered-guide-for-higher-education20240912184323/",
            "category" : ["COURSE", "FEES"]
        }}
    ]

    Categorize the update/news/announcements according the datapoints given below:
    datapoints : COURSE, FEES, SCHOLARSHIP, ADMISSION, CUTOFF, RANKING, PLACEMENTS, EXAMS, AWARDS, AGREEMENTS, ACHIEVEMENTS

    title : Its should be a one liner text specifying what is the Exact news/update/announcement.
    description : Its is a short 2-3 line of description about the news/update/announcement. Keep it descriptive.
    page_link : Link or PDF that The news/update/announcement is linked to or referring to. DON'T INCLUDE TEXTS OR IMAGE LINKS.
    category : Category of the news/update/announcement which falls under the datapoints filed provided above.

    NOTE: IF page_link IS NOT AVAILABLE GIVE EMPTY STRING IN page_link FIELD.

    Below are some example output:

    ## BEGIN EXAMPLE OUTPUT:

    ### OUTPUT Example:
    [
        {{
            "title" : "Got Best 30 on 30 university Award",
            "description" : "Our University has been awarded best 30 on 30 university by the University concel",
            "page_link" : "https://iiitd.ac.in/sites/default/files/docs/admissions/2024/Merit%20List%20for%20CSE%20Non-Gate%202024.pdf",
            "category" : ["FEES"]
        }},
        {{
            "title" : "Launched New 5 Year integrated course",
            "description" : "We have launched a New 5 Year integrated course of B.Tech + Artificial Intelligence.",
            "page_link" : "https://iiitd.ac.in/sites/default/files/docs/admissions/2024/Special%20SPOT%20Round%20for%20ECE%20and%20CB%202024.pdf"
            "category" : ["RANK"]
        }}
    ]

    ##END EXAMPLE OUTPUT

    Give give me the output in the above explain format. DON'T Hallucinate and Give response yourself. If Data is not present then just give empty String as response
    
    ### INPUT MARKDOWN START ### : 
    
    {markdown}

    ### INPUT MARKDOWN END ### : 
    '''

    return ask_gpt(prompt,"You are a helpful assistant that extracts information from markdown content and updates JSON data. You only respond in JSON, without any extra lines outside of it.")


def find_latest_updates(inst_url,inst_name):
    print(f"Extraction Started for {inst_name}...")

    total_links = []
    data = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(inst_url)

        page.wait_for_timeout(5000)

        content = page.content()

        html = content

        mk = html_to_markdown(html)
        links = get_links(mk, inst_name)


        links = re.sub(r"```json\s*|\s*```", "", links).strip()
        links = json.loads(links)


        total_links.append(inst_url)
        for l in links['new_urls']:
            total_links.append(l)

    print("Total Links:", len(total_links))

    for i in total_links:

        if "http" not in i:
            i = urljoin(inst_url, i)

        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(i)
                page.wait_for_timeout(5000)

                content = page.content()
                html_content = content
                
                mk = html_to_markdown(html_content)
                res = get_data(mk, inst_name)

                res = re.sub(r"```json\s*|\s*```", "", res).strip()
                res = json.loads(res)

                for news in res:
                    page_link = news['page_link']
                    if page_link and "http" not in page_link:
                        page_link = f"{i}/{page_link}"

                    is_url_valid = False

                    if page_link:
                        is_url_valid = validate_url(page_link)

                    obj = {
                        ** news,
                        "url" : i,
                        "page_link" : page_link if is_url_valid else ""
                    }
                    data.append(obj)

        except Exception as e:
            print(e)
    return data


def find_refund_policy(inst_url,inst_name):
    print(f"Extraction Started for {inst_name}...")

    total_links = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(inst_url)

        page.wait_for_timeout(5000)

        content = page.content()

        html = content

        mk = html_to_markdown(html)
        links = get_refund_links(mk, inst_name)

        links = re.sub(r"```json\s*|\s*```", "", links).strip()
        links = json.loads(links)

        for l in links['links']:
            total_links.append(l)

    return total_links


def validate_sheet(row, index, validation_type):
    error_messages = {}
    institute_id = row["inst_id"]
    course_id = row["course_id"]

    if validation_type == "INSTITUTE_MASTER_COURSE":
        if (
            pd.isna(institute_id)
            or pd.isna(course_id)
            or institute_id == ""
            or course_id == ""
        ):
            return f"Failure: One or more fields are empty or 'nan'."

        if not is_valid_institute_id(institute_id):
            return f"Failure: Invalid institute ID {institute_id}"

        if not is_valid_institute_id(course_id):
            return f"Failure: Invalid Course ID {course_id}"

    else:
        inst_course_id = row["inst_course_id"]
        inst_course_name = row["inst_course_name"]

        if (
            pd.isna(institute_id)
            or pd.isna(course_id)
            or pd.isna(inst_course_id)
            or pd.isna(inst_course_name)
            or institute_id == ""
            or course_id == ""
            or inst_course_id == ""
            or inst_course_name == ""
        ):
            return f"Failure: One or more fields are empty or 'nan'."

        if not is_valid_institute_id(institute_id):
            return f"Failure: Invalid institute ID {institute_id} at row {index + 1}."

        if not is_valid_institute_id(inst_course_id):
            return f"Failure: Invalid Institute Course Id {inst_course_id} at row {index + 1}."

        if not is_valid_institute_id(course_id):
            return f"Failure: Invalid Course ID {course_id} at row {index + 1}."


    return error_messages


def populate_inst_master_course(csv_file):
    inst_course_mapping = {}
    result = {}
    df = pd.read_csv(csv_file)

    for index, row in df.iterrows():
        error_messages = validate_sheet(row, index, "INSTITUTE_MASTER_COURSE")

        if error_messages:
            result[f"Row: {index+1}"] = error_messages
        else:
            institute_id = row["inst_id"]
            course_id = row["course_id"]

            if institute_id not in inst_course_mapping:
                inst_course_mapping[institute_id] = [course_id]
            else:
                inst_course_mapping[institute_id].append(course_id)

    for inst_id in inst_course_mapping:
        try:
            course_id = inst_course_mapping[inst_id]
            inst_id = int(inst_id)

            data = fetch_from_inst_master_course(inst_id)
            if len(data) > 0:
                logging.info("Already Exists", inst_id, len(data))
                doc_id = data[0]["_id"]
                source = data[0]["_source"]
                data = source["cld_course_ids"]

                for id in course_id:
                    if id in data:
                        logging.info("Course Id Already Exists")
                    else:
                        data.append(id)

                update_fields_in_es(
                    "inst_master_courses", doc_id, "cld_course_ids", data
                )
                logging.info(f"Done!", inst_id)
                result[inst_id] = "Institute Already Exists. Updated with New Course Ids"
            else:
                logging.info("Need to Create New Entry", inst_id)
                doc = {
                    "cld_course_ids": course_id,
                    "inst_id": inst_id,
                    "created_at": datetime.now(),
                    "updated_at": datetime.now(),
                }
                es.index(index="inst_master_courses", body=doc)
                logging.info(f"Done!", inst_id)

                result[inst_id] = "Success"
        except Exception as e:
            result[inst_id] = str(e)

    return result


def populate_inst_specific_course(csv_file):

    df = pd.read_csv(csv_file)
    result = {}

    for index, row in df.iterrows():
        try:
            error = validate_sheet(row, index, "inst_specific_course")
            if error:
                result[f"Row: {index+1}"] = error
            else:
                data = fetch_from_inst_specific_course(row["inst_course_id"])
                if len(data) > 0:
                    logging.info("Already Exists", row["inst_course_id"], len(data))
                    result[f"Row: {index+1}"] = "Already Exists"
                else:
                    logging.info("Need to Create New Entry")
                    doc = {
                        "inst_id": row["inst_id"],
                        "course_id": row["course_id"],
                        "inst_course_id": row["inst_course_id"],
                        "inst_course_name": row["inst_course_name"],
                        "created_at": datetime.now(),
                        "updated_at": datetime.now(),
                    }
                    es.index(index="inst_specific_courses", body=doc)
                    logging.info(f"Done!", row["inst_id"])

                    result[f"Row: {index+1}"] = "Success"
        except Exception as e:
            result[f"Row: {index+1}"] = str(e)

    return result
