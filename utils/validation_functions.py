from nltk.corpus import stopwords
import json
import re
import ast
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np


def validate_names(answer):
    stop_words = set(stopwords.words("english"))
    hallucinated_names = [
        "John Doe",
        "Jane Smith",
        "Alex Johnson",
        "Emily Davis",
        "Michael Brown",
        "Sarah Williams",
        "Robert Wilson",
        "Laura Taylor",
        "Alan Turing",
        "Albert Einstein",
        "Ratan Tata",
        "Narayana Murthy",
        "Sundar Pichai",
        "Steve Jobs",
        "Mark Zuckerberg",
        "Stephen Hawking",
        "Charles Darwin",
        "Isaac Newton",
        "David Miller",
        "Emma Thompson",
    ]

    def is_likely_name(name):
        string = name.replace(".", "")
        words = string.lower().split()
        filtered_words = [
            word for word in words if word not in ["dr", "mr", "mrs", "ms", "prof"]
        ]
        name = " ".join(filtered_words)

        if any(part in stop_words for part in name.split()):
            return False
        elif name.lower() in list(map(str.lower, hallucinated_names)):
            return False
        else:
            return True

    def check_name_length(string):
        string = string.replace(".", "")
        words = string.lower().split()
        # Remove the specified words
        filtered_words = [
            word for word in words if word not in ["dr", "mr", "mrs", "ms", "prof"]
        ]
        return len(filtered_words) < 4

    def filter_names(name_list):
        correct_names = []
        if len(name_list) > 1:
            for name in name_list:
                if is_likely_name(name):
                    if check_name_length(name):
                        correct_names.append(name)
            return correct_names
        else:
            return ["na"]

    def extract_primary_keys(json_str):
        try:
            # Parse the JSON string
            data = json.loads(json_str)
            # Return the pkeys
            return list(data.keys())
        except json.JSONDecodeError:
            # Return an empty list if JSON is invalid
            return []

    answer_new = json.loads(answer)
    name_list = extract_primary_keys(answer)
    validated_names = filter_names(name_list)
    if len(validated_names) > 1:
        result = {key: answer_new[key] for key in validated_names if key in answer_new}
        return json.dumps(result)
    else:
        return ""


def validate_fee(answer):

    def get_len(txt_var):
        return len(str(txt_var))

    def count_negative_phrase(text):
        text = str(text)
        phrases = [
            "contact institution for accurate information",
            "contact institute for accurate information",
            "Fee details available on the website link provided",
            "fee details not provided",
            "not specified",
            "to be announced",
        ]
        sum = 0
        for phrase in phrases:
            sum = sum + len(re.findall(re.escape(phrase), text, re.IGNORECASE))
        return sum

    def is_html_table(text):
        pattern = r"<table\b[^>]*>(.*?)</table>"
        return bool(re.search(pattern, text, re.IGNORECASE | re.DOTALL))

    def count_numbers(string):
        number_pattern = (
            r"\b(?:\d{1,2}(?:,\d{2})*(?:,\d{3})*|\d{1,3}(?:,\d{3})*|\d+)(?:\.\d+)?\b"
        )
        numbers = re.findall(number_pattern, string)
        count = 0
        for num in numbers:
            # Remove commas and convert to integer
            try:
                num_value = int(float(num.replace(",", "")))
            except ValueError:
                num_value = 0
            if num_value > 300 and num_value != 2024:
                count += 1
        return count

    if get_len(answer) < 300:
        return False
    elif count_negative_phrase(answer) > 2:
        return False
    elif count_numbers(answer) < 2:
        return False
    else:
        return True


def validate_infra_list(answer):

    def extract_array(input_string):
        match = re.search(r"\[(.*)\]", input_string)
        if match:
            list_string = match.group(0)
            try:
                result = ast.literal_eval(list_string)
                if isinstance(result, list):
                    return result
            except (ValueError, SyntaxError):
                pass
        return ["na"]

    def get_similar_items(check_list, answer_list, threshold=0.5):
        # Combine both lists
        all_text = check_list + answer_list

        # Create a TfidfVectorizer object
        vectorizer = TfidfVectorizer()

        # Fit and transform the text data
        tfidf_matrix = vectorizer.fit_transform(all_text)

        # Split the matrix back into two
        vector1 = tfidf_matrix[: len(check_list)]
        vector2 = tfidf_matrix[len(check_list) :]

        # Calculate cosine similarity
        similarity = cosine_similarity(vector1, vector2)

        # Find items in list2 that have similarity > threshold with any item in list1
        similar_items = []
        for i, item in enumerate(answer_list):
            if np.any(similarity[:, i] > threshold):
                similar_items.append(item)
        return similar_items

    medical_lists = [
        "x ray",
        "hostels",
        "sports complex",
        "opd outpatient department",
        "medical simulation center",
        "it center",
        "ultrasound",
        "gymnasium",
        "security services",
        "pharmacology lab",
        "faculty offices",
        "lecture halls",
        "molecular biology lab",
        "clinical skills lab",
        "microbiology lab",
        "anatomy dissection hall",
        "histology lab",
        "hospital",
        "genetics lab",
        "seminar hall",
        "parking area",
        "student counseling services",
        "radiology lab",
        "citizen charter",
        "emergency",
        "biochemistry lab",
        "pathology lab",
        "emergency department",
        "medical bookstore",
        "ct scan",
        "blood bank",
        "cafeteria",
        "auditorium",
        "pharmacy",
        "class rooms and labs",
        "anatomy lab",
        "physiology lab",
        "administrative offices",
        "immunology lab",
        "operation theator",
        "mri",
        "wi fi enabled campus",
        "hostel",
        "library",
        "laboratories",
        "community health center",
        "postmortom",
        "research center",
        "blood pressure checkup",
        "transportation services",
        "wards",
    ]
    general_facilities = [
        "math lab",
        "academic zone",
        "ac",
        "av lab",
        "auditorium",
        "atm",
        "boys hostel",
        "canteen",
        "Spacious Classrooms",
        "computer lab",
        "csc",
        "convocation hall",
        "cultural zone",
        "education festival",
        "planning lab",
        "computer center",
        "girls hostel",
        "guest house",
        "gym",
        "gymkhana",
        "hospital",
        "mess",
        "international center",
        "theatre",
        "library",
        "medical facilities",
        "music",
        "placement",
        "sports",
        "grounds",
        "post office",
        "practical labs",
        "r d",
        "residential institute",
        "residential zone",
        "residential faculty zone",
        "shopping",
        "club",
        "swimming pool",
        "trans",
        "video call",
        "wifi",
        "seminar hall",
        "conference room",
        "railway concession",
        "workshops",
        "robotic laboratory",
        "cnc lab",
        "cupboards availability",
        "electrical appliances",
        "laundry",
        "ambulance",
        "tv room",
        "badminton court",
        "tt room",
        "carom board",
        "volleyball ground",
        "sports complex",
        "intercom",
        "recreation room",
        "visitor room",
        "cafeteria",
        "clothing lab",
        "pattern making lab",
        "photography lab",
        "metal lab",
        "terracotta lab",
        "wood lab",
        "textiles lab",
        "mac lab",
        "incubator",
        "studio",
        "laboratories",
        "hostel",
        "scholarship",
        "e classroom",
        "design lab",
        "sewing lab",
        "cad lab",
        "gymnasium",
        "central library",
        "e learning center",
        "incubation centre",
        "health services",
        "247 security",
        "transport services",
        "tech fest",
        "surveillance camera",
        "internet lab",
        "conference halls",
        "seminar halls",
        "techno park",
        "hostels",
        "university block",
        "academic building",
        "sports games",
        "indoor stadium",
    ]

    ### convert answer to list
    answer_list = list(extract_array(answer))

    if len(answer_list) > 2:
        medical_list = get_similar_items(medical_lists, answer_list)
        general_list = get_similar_items(general_facilities, answer_list)
        if len(list(dict.fromkeys(medical_list))) >= len(
            list(dict.fromkeys(general_list))
        ):
            return list(dict.fromkeys(medical_list)), True
        else:
            return list(dict.fromkeys(general_list)), True
    else:
        return [], False
