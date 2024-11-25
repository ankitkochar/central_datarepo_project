from openai import AzureOpenAI
import json
from typing import Dict, List, Tuple
import re
import openai
import time
import random
from pydantic import BaseModel
import tiktoken

# class DegreeInformation(BaseModel):
#     undergraduate_degrees: List[str] = []
#     postgraduate_degrees: List[str] = []
#     doctorate_degrees: List[str] = []
#     links_to_visit: List[str] = []
#     pdf_links_to_download: List[str] = []


class LLMIntegrator:
    def __init__(
        self,
        base_domain: str,
        api_key: str,
        institute_name: str,
        model: str = "gpt-4o-mini",
        max_retries: int = 5,
    ):
        self.client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint="https://saarthi-ai394538790176.openai.azure.com/",
            api_version="2023-03-15-preview",
        )
        self.model = model
        self.institute_name = institute_name
        self.max_retries = max_retries
        self.base_domain = base_domain
        self.input_tokens_used = 0  # Initialize token counter
        self.output_tokens_used = 0

    def construct_prompt_scraper(self, markdown_content: str) -> str:
        prompt = f"""
    Identity: "You are mimicking a human who is trying to get information regarding {self.institute_name} from their website.  You will be provided a markdown representation of a webpage.
	1. Your jobs are:
        1.1 Identify any new URLs that should be visited/may contain information regarding the datapoints required pertaining to {self.institute_name}. Be very selective and conservative. Absolutely avoid links that have a low chance of containing information regarding the datapoints and {self.institute_name}.
        1.2 Identify any pdf links that should be downloaded/may contain information regarding the datapoints provided. be very selective and conservative. DO NOT SEND PDF LINKS TO NEW_URLS.
    2. Datapoints for identifying URLs: Fees of all types, Undergraduate Degrees and Specializations, Postgraduate Degrees and Specializations,  Infrastructure Details, Hostels, Fees, Refund Policy, Admission Process, Administration, Faculty, Doctoral/PhD programs, Diploma Programs, NIRF and AICTE Approvals, Placements, Scholarships, Alumni
    If you find any URLs that pertain to {self.institute_name} and the datapoints provided, return them in the JSON response, even if they belong to a subdomain as long as they pertain to {self.institute_name}.
    Try not to find URLs that are not relevant to {self.institute_name} expecially if they point towards external domains. The base domain is {self.base_domain}.
    Also focus on individual degree/course urls.
    A few examples of how the output structure should look like like are listed below:
    ## BEGIN EXAMPLE OUTPUT:
    ### EXAMPLE OUTPUT 1:
    {{
    "new_urls": [
        "https://www.lpu.in/programmes/all/12th",
        "https://www.lpu.in/programmes/all/Graduation",
        "https://www.lpu.in/programmes/all/Post-Graduation",
        "https://www.lpu.in/programmes/all/Diploma%20or%20Certificate",
        "https://www.lpu.in/programmes/all/10th"
    ],
    "new_pdfs": [
        "https://www.lpu.in/international/downloads/international-booklet.pdf",
        "https://www.lpu.in/admission/Prospectus/booklets/B.Tech-Booklet.pdf",
        "https://www.lpu.in/admission/Prospectus/booklets/New-UG-Combined.pdf
    ]
    }},
    ### END EXAMPLE OUTPUT 1
    
    ### EXAMPLE OUTPUT 2:
    {{
        
    "new_urls": [
    "http://www.amity.edu/programmes.aspx",
    "http://www.amity.edu/admission-procedure-domestic.aspx"
    ],
    "new_pdfs": [
        "https://amity.edu/pdf/admission_prospectus_2024.pdf",
        "https://amity.edu/Admission/admission_prospectus.pdf"
    ]
    }}
    ### END EXAMPLE OUTPUT 2
    
    ### EXAMPLE OUTPUT 3:
    {{
        
    "new_urls": [
    "https://www.bits-pilani.ac.in/academics/integrated-first-degree/b-e-electrical-electronics/",
    "https://www.bits-pilani.ac.in/academics/integrated-first-degree/b-e-mechanical/",
    "https://www.bits-pilani.ac.in/admissions/integrated-first-degree/",
    "https://www.bits-pilani.ac.in/faculty/?campus=pilani&department=biological-sciences",
    "https://www.bits-pilani.ac.in/pilani/biological-sciences/"
    ],
    "new_pdfs": [
        "https://www.bitsadmission.com/bitsat/download/Eligibility_for_Admissions.pdf",
        "https://bits-pilani-wilp.ac.in/sites/default/files/pdf/Pgp1445mlai.pdf"
    ]
    }}
    ### EXAMPLE END OUTPUT 3

    ##END EXAMPLE OUTPUT
    
    ### INPUT MARKDOWN START ### : 
    
    {markdown_content}
    
    ### INPUT MARKDOWN END ###

Do not create new fields except the ones shown to you in the example outputs. Be selective with the URLs you provide and focus on the datapoints listed above.
"""
        return prompt

    def construct_prompt_details(
        self, markdown_content: str, current_json: Dict, empty_fields: List[str]
    ) -> str:
        prompt = f"""
    Identity: "You are mimicking a human who is trying to get information regarding {self.institute_name} from their website.  You will be provided a markdown representation of a webpage.
	1. Your jobs are:
        1.1 Identify Bachelor's/Undergraduate, Master's/Postgraduate, Doctorate Degrees and Specializations/Branches, and Diploma courses offered by {self.institute_name} based on the page currently provided to you. Add the ones found on the page to the list provided to you. Degree refers to things like 'Bachelors of Technology/B.Tech', 'Masters of Commerce/M.Com' etc whereas Specializations refer to "B.Tech in Computer Science", "M.Com in Finance" etc."
        1.2 Provide metadata/tags for the markdown sent to you. They should be in the format of a list of strings. 
        1.3 Specializations MUST include the degree they are associated with. For example, "B.Tech in Computer Science" is a valid specialization, but "Computer Science" is not.
    2. Datapoints for identifying URLs: Undergraduate Degrees, Undergraduate Specializations, Postgraduate Degrees, Postgraduate Specializations, Infrastructure Details, Hostels, Fees, Admission Process, Administration, Faculty, Doctoral/PhD programs, Diploma Programs, NIRF and AICTE Approvals, Placements, Scholarships, Alumni
	3. Undergraduate Degrees Previously Found: {current_json.get("undergraduate_degrees", [])}
	4. Undergraduate Specializations Previously Found: {current_json.get("undergraduate_specializations", [])} 
	5. Postgraduate Degrees Previously Found: {current_json.get("postgraduate_degrees", [])}
	6. Postgraduate Specializations Previously Found: {current_json.get("postgraduate_specializations", [])}
    7. Doctorate Degrees Previously Found: {current_json.get("doctorate_degrees", [])}
    8. Diplomas Previously Found: {current_json.get("diploma_degrees", [])}
    A few examples of how the output structure should look like like are listed below:
    ## BEGIN EXAMPLE OUTPUT:
    ### OUTPUT 1:
    {{
    "undergraduate_degrees": [
        "B.Tech",
        "BA",
        "B.Com",
        "BBA",
        "B.Sc",
        "B.DES",
        "BFA",
        "BHM"
    ],
    "undergraduate_specializations": [
        "B.Tech in Computer Science",
        "B.Tech in Mechanical Engineering",
        "B.Tech in Civil Engineering",
        "B.Tech in Electrical Engineering",
        "B.Tech in Electronics and Communication",
        "B.Tech in Software Engineering",
        "B.Com in General",
        "BBA in Marketing",
        "B.Sc in Agriculture",
        "B.DES in Fashion Design",
        "BFA in Fine Arts",
        "BHM in Hotel Management"
    ],
    "postgraduate_degrees": [
        "M.Tech",
        "MBA",
        "M.Sc",
        "M.A",
        "M.Com",
        "M.DES"
    ],
    "postgraduate_specializations": [
        "M.Tech in Structural Engineering",
        "M.Tech in Computer Science",
        "MBA in Human Resource Management",
        "MBA in Marketing",
        "M.Sc in Environmental Science",
        "M.Sc in Physics",
        "M.A in English",
        "M.Com in Accountancy",
        "M.DES in Interior Design"
    ],
    "doctorate_degrees": [
        "Ph.D",
        "D.Litt"
    ],
    "diploma_degrees": [
        "Diploma in Computer Applications",
        "Diploma in Mechanical Engineering",
    ],
    "metadata": ["LPU", "Lovely Professional University", "Design", "Science", "Engineering", "BTech", "MTech", "Computer Science"]
    }},
    ### END OUTPUT 1
    
    ### OUTPUT 2:
    {{
    "undergraduate_degrees": [
        "B.A.",
        "B.Com.",
        "B.Sc.",
        "B.Tech.",
        "BBA",
        "BCA",
        "LL.B."
    ],
    "undergraduate_specializations": [
        "B.A. (Administration)",
        "B.A. (English)",
        "B.A. (Honours/Honours with Research)",
        "B.A. (Hons) - Business Economics",
        "B.A. (Journalism & Mass Communication)",
        "B.Com. (Honours/Honours with Research)",
        "B.Com. (Hons)",
        "B.Sc. (Clinical Psychology)",
        "B.Tech (Artificial Intelligence & Machine Learning)",
        "B.Tech (Civil Engineering)",
        "B.Tech (Computer Science & Engineering)",
        "B.Tech (Computer Science & Engineering - Cyber Security)",
        "B.Tech (Computer Science & Engineering - Data Science)",
        "B.Tech (Computer Science & Engineering - Internet of Things and Blockchain)",
        "B.Tech (Electronics Engineering - VLSI Design and Technology)",
        "B.Tech (Computer Science Engg. - International)",
        "BBA (Honours/Honours with Research)",
        "BBA (International)",
        "BCA (Honours/Honours with Research)",
        "BCA + MCA (Dual Degree)"
        ],
    "postgraduate_degrees": [
        "M.A.",
        "M.Com.",
        "M.Tech.",
        "MBA",
        "MCA",
        "M.Phil.",
        "Ph.D."
        ],
    "postgraduate_specializations": [
        "M.A. (Administration)",
        "M.A. (English)",
        "M.A. (Journalism & Mass Communication)",
        "M.Tech (Computer Science & Engineering)",
        "MBA (Family Business & Entrepreneurship)",
        "MBA - Executive (for working Professionals)",
        "MBA (International)",
        "M. Phil (Clinical Psychology)",
        ],
    "doctorate_degrees": [
        "Ph.D.",
        "LL.D."
    ],
    "diploma_degrees": [
        "Diploma in Computer Applications",
        "Diploma in Pharmacy",
        
    "metadata": ["Amity", "Amity University", "Design", "Arts", "Management", "MBA", "BBA", "Fees"]
    }}
    ### END OUTPUT 2
    
    ##END EXAMPLE OUTPUT
    
    ### INPUT MARKDOWN START ### : 
    
    {markdown_content}
    
    ### INPUT MARKDOWN END ###

Do not create new fields except the ones shown to you in the example outputs. Be selective with the URLs you provide and focus on the datapoints listed above.
"""
        return prompt

    def truncate_to_100k_tokens_tiktoken(self, prompt):
        encoding = tiktoken.get_encoding("o200k_base")
        tokens = encoding.encode(prompt)
        if len(tokens) <= 100000:
            return prompt
        return encoding.decode(tokens[:100000])

    def send_request_to_llm(self, prompt: str) -> str:
        retries = 0
        while retries < self.max_retries:
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a helpful assistant that extracts information from markdown content and updates JSON data. You only respond in JSON, without any extra lines outside of it.",
                        },
                        {"role": "user", "content": prompt},
                    ],
                    response_format={"type": "json_object"},
                )

                self.input_tokens_used += response.usage.prompt_tokens
                self.output_tokens_used += response.usage.completion_tokens
                return response.choices[0].message.content
            except openai.RateLimitError as e:
                wait_time = (3**retries) + random.random()
                print(
                    f"Rate limit reached. Waiting for {wait_time:.2f} seconds before retrying."
                )
                print(f"Error message: {e}")
                print(prompt)
                time.sleep(wait_time)
                retries += 1
            except openai.BadRequestError as e:
                print(f"Error sending request to LLM: {e}")
                error_message = str(e).lower()
                if "issue with repetitive patterns" in error_message:
                    print(
                        "Repetitive pattern detected. Retrying with truncated prompt."
                    )

                    return {}
                else:
                    wait_time = (3**retries) + random.random()
                    retries += 1
                    prompt = self.truncate_to_100k_tokens_tiktoken(prompt)
                    print(f"Error occured {wait_time:.2f} seconds before retrying.")
                    print(
                        f"Error message: {e} \n reducing length of html prompt to 100000 tokens and retrying"
                    )
            except Exception as e:
                print(f"Error sending request to LLM: {e}")
                print(f"Error message: {e} \n ")
                return {}

        print(
            f"Max retries ({self.max_retries}) reached. Unable to get response from LLM."
        )
        return {}

    def parse_llm_response_scraper(self, response: str) -> Tuple[Dict, List[str]]:
        try:
            # Remove markdown code block formatting if present
            response = re.sub(r"```json\s*|\s*```", "", response).strip()
            # print(f"LLM response:\n{response}")
            parsed_response = json.loads(response)
            new_pdfs = parsed_response.get("new_pdfs", [])
            new_urls = parsed_response.get("new_urls", [])
            # print(f"New Urls: {new_urls}")
            return parsed_response, new_urls, new_pdfs
        except json.JSONDecodeError as e:
            print(f"Error parsing LLM response. Invalid JSON: {e}")
            print(f"Raw response:\n{response}")
            return {}, [], []
        except Exception as e:
            print(f"Error parsing LLM response: {e}")
            print(f"Raw response:\n{response}")
            return {}, [], []

    def parse_llm_response_details(self, response: str) -> Tuple[Dict, List[str]]:
        try:
            # Remove markdown code block formatting if present
            response = re.sub(r"```json\s*|\s*```", "", response).strip()
            # print(f"LLM response:\n{response}")
            parsed_response = json.loads(response)
            metadata = parsed_response.get("metadata", [])
            # print(f"New Urls: {new_urls}")
            return parsed_response, metadata
        except json.JSONDecodeError as e:
            print(f"Error parsing LLM response. Invalid JSON: {e}")
            print(f"Raw response:\n{response}")
            return {}, []
        except Exception as e:
            print(f"Error parsing LLM response: {e}")
            print(f"Raw response:\n{response}")
            return {}, []

    def process_markdown_details(
        self, markdown_content: str, current_json: Dict, empty_fields: List[str]
    ) -> Tuple[Dict, List[str]]:
        prompt_details = self.construct_prompt_details(
            markdown_content, current_json, empty_fields
        )
        llm_response = self.send_request_to_llm(prompt_details)

        if llm_response:
            return self.parse_llm_response_details(llm_response)
        else:
            return {}, []

    def process_markdown_scraper(
        self, markdown_content: str, current_json: Dict, empty_fields: List[str]
    ) -> Tuple[Dict, List[str]]:
        prompt_details = self.construct_prompt_scraper(markdown_content)

        llm_response = self.send_request_to_llm(prompt_details)

        if llm_response:
            return self.parse_llm_response_scraper(llm_response)
        else:
            return {}, [], []

    def get_total_tokens_used(self) -> int:
        return self.input_tokens_used + self.output_tokens_used

    def get_input_tokens_used(self) -> int:
        return self.input_tokens_used

    def get_output_tokens_used(self) -> int:
        return self.output_tokens_used
