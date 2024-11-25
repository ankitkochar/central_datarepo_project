from .elastic import (
    fetch_ip_obj,
    fetch_prompt_obj,
    add_ipa_validation_entry,
    update_validation_run_status,
)
from .validation_functions import validate_names, validate_fee, validate_infra_list
import logging

###TODO Add length check to names etc.


def validation_by_tag_type(tag, answer):
    # Will Return status, valid_answer, comment
    comment = "Worked"
    status = True
    try:
        if tag.strip() == "Alumni":
            answer_validated = validate_names(answer)
            status = len(answer_validated) > 0
            if status == False:
                comment = "Validation Failed because list empty"

        elif tag.strip() == "Postgraduate Courses":
            answer_validated = answer
            if len(answer_validated) <= 2:
                status = False
                comment = "Validation Failed because list < 2"
            else:
                comment = "No Validation Yet"

        elif tag.strip() == "Undergraduate Courses":
            answer_validated = answer
            if len(answer_validated) <= 2:
                status = False
                comment = "Validation Failed because list < 2"
            else:
                comment = "No Validation Yet"

        elif tag.strip() == "Fees of All Courses":
            status = validate_fee(answer)
            if status:
                answer_validated = answer
            else:
                answer_validated = ""
                comment = "Fee Validation Failed"

        elif tag.strip() == "Campus Infrastructure":
            answer_validated, status = validate_infra_list(answer)
            if status == False:
                comment = "Validation Failed because list < 2"

        elif tag.strip() == "Faculty Details":
            answer_validated = validate_names(answer)
            status = len(answer_validated) > 0
            if status == False:
                comment = "Validation Failed because list empty"

        elif tag.strip() == "Hostel Details":
            answer_validated = answer
            if len(answer_validated) <= 2:
                status = False
                comment = "Validation Failed because Answer Json is Empty"
            else:
                comment = "No Validation Yet"

        elif tag.strip().startswith("Campus Facilities"):
            if answer.lower().strip() in ["yes", "no"]:
                status = True
                answer_validated = answer
            else:
                status = False
                answer_validated = ""
                comment = "Hallucination/Not in Yes/No"
        else:  # Default Case
            answer_validated = answer
            comment = "No Validation Yet"

    except Exception as e:
        answer_validated = answer
        status = True
        comment = f"Validation Errored Out. Exception: {e}"
    finally:
        return status, answer_validated, comment


def validation_model(ip_answer_obj, institute_id):
    try:
        ip_id = ip_answer_obj["ip_id"]
        ip_obj = fetch_ip_obj(ip_id,institute_id)
        p_id = ip_obj["prompt_id"]
        prompt_obj = fetch_prompt_obj(p_id)
        degree_specific = prompt_obj["degree_specific"]
        specialization_specific = prompt_obj["specialization_specific"]
        ipa_id = ip_answer_obj["_id"]

        if not degree_specific and not specialization_specific:
            ip_answer = ip_answer_obj["answer"]
            prompt_tags = prompt_obj["tags"]
            status, answer, comment = validation_by_tag_type(prompt_tags, ip_answer)
            ipa_obj = add_ipa_validation_entry(ip_id, ipa_id, status, answer, comment)
            update_validation_run_status(ipa_id)

        else:
            ipa_obj = add_ipa_validation_entry(
                ip_id,
                ipa_id,
                True,
                ip_answer_obj["answer"],
                "No Validation Yet. Not overview level.",
            )
            status = True
            comment = "No Validation Yet. Not overview level."
            update_validation_run_status(ipa_id)

        return status, ipa_obj, ip_obj, prompt_obj

    except Exception as e:
        logging.error(f"Error in Validation MODEL for {institute_id}", e)
