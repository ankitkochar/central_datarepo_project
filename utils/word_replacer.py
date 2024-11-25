import re
from bs4 import BeautifulSoup
import traceback


synonyms = {
    "annum": "year",
    "duration": "Course duration",
    "Course duration": "duration",
    "Amount": "Fee",
    "Remarks": "Notes",
    "Notes": "Remarks",
    "Due Time": "Timelines",
    "Components": "Fee Type",
    "Course": "Program",
    "Program": "Programme",
    "Programme": "Course",
}


def word_transformation(answer, answer_format: str) -> str:
    global synonyms

    def create_bidirectional_synonyms(synonyms):
        bidirectional = {}
        for key, value in synonyms.items():
            if key not in bidirectional and value not in bidirectional:
                bidirectional[key] = value
                bidirectional[value] = key
        return bidirectional

    def apply_bidirectional_synonyms(text, bidirectional_synonyms):
        def replace_word(match):
            word = match.group(0)
            return bidirectional_synonyms.get(word.lower(), word)

        pattern = (
            r"\b("
            + "|".join(re.escape(word) for word in bidirectional_synonyms.keys())
            + r")\b"
        )
        return re.sub(pattern, replace_word, text, flags=re.IGNORECASE)

    def process_html_table(html_table: str, synonyms: dict) -> str:
        try:
            soup = BeautifulSoup(html_table, "html.parser")
        except Exception as e:
            print("Error in parsing HTML\n")
            print(e)
            return html_table
        try:
            bidirectional_synonyms = create_bidirectional_synonyms(synonyms)

            headers = soup.find_all("th")
            for index, th in enumerate(headers):
                if th.text.strip().lower() in ["s.no", "s. no"]:
                    for row in soup.find_all("tr"):
                        cells = row.find_all(["th", "td"])
                        if len(cells) > index:
                            cells[index].decompose()
                    th.decompose()
                    break  # Assuming there's only one s.no column, we can break after finding it

            # Replace words in column names and first column
            for th in soup.find_all("th"):
                th.string = apply_bidirectional_synonyms(
                    th.text, bidirectional_synonyms
                )

            for tr in soup.find_all("tr"):
                cells = tr.find_all("td")
                if cells:
                    cells[0].string = apply_bidirectional_synonyms(
                        cells[0].text, bidirectional_synonyms
                    )
        except Exception as e:
            print("Error in processing table\n")
            print(e)
        return str(soup)

    def process_string(text: str, synonyms: dict) -> str:
        bidirectional_synonyms = create_bidirectional_synonyms(synonyms)
        return apply_bidirectional_synonyms(text, bidirectional_synonyms)

    if "table" in answer_format.lower():
        return process_html_table(answer, synonyms)
    else:
        return process_string(answer, synonyms)
