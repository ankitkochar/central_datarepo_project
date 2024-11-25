# Library
import logging
from fastapi import APIRouter, Depends

# Modules
from .interfaces import queryResult, DegreeData
from crawling.Interfaces import InstituteIds
from .utils import make_function_async
from .controller import (
    query_single_prompt,
    generate_data_points,
    generate_data_points_course,
    run_on_all_degrees,
    generate_prompt_output,
    generate_prompt_output_temporary,
    run_on_all_courses
)
from utils.auth_utils import check_token_middleware

# Router
router = APIRouter()


@router.post("/query-prompt", dependencies=[Depends(check_token_middleware)])
async def query_prompt(item: queryResult):
    try:
        prompt = item.prompt
        institute_id = item.institute_id
        logging.info(
            f"Starting to fetch result for query: {prompt} with institute id: {institute_id}"
        )
        result = await make_function_async(query_single_prompt, item)
        return result
    except Exception as e:
        logging.error(f"Error while querying. {e}")


@router.post("/generateCldDataPoints", dependencies=[Depends(check_token_middleware)])
async def generation_data(item: InstituteIds):
    try:
        logging.info(f"Starting data generation for IDs: {item.institute_ids}")
        result = await make_function_async(generate_data_points, item)
        logging.info(f"Data generation completed for IDs: {item.institute_ids}")
        return result
    except Exception as e:
        logging.error(f"Error in generating data for IDs: {item.institute_ids}: {e}")
        return e


@router.post(
    "/generateCourseDataPoints", dependencies=[Depends(check_token_middleware)]
)
async def generation_data(item: InstituteIds):
    try:
        logging.info(f"Starting data generation for IDs: {item.institute_ids}")
        result = await make_function_async(generate_data_points_course, item)
        logging.info(f"Data generation completed for IDs: {item.institute_ids}")
        return result
    except Exception as e:
        logging.error(f"Error in generating data for IDs: {item.institute_ids}: {e}")
        return e


@router.post(
    "/generate-all-degree-data", dependencies=[Depends(check_token_middleware)]
)
async def generation_degree(item: DegreeData):
    try:
        logging.info(
            f"Starting degree data generation for institutes: {item.institute_ids}"
        )
        result = await make_function_async(run_on_all_degrees, item)
        return result
    except Exception as e:
        logging.error(f"Error while generating degree data. {e}")
        return e


@router.get("/auto-output-generation", dependencies=[Depends(check_token_middleware)])
async def auto_output_generation():
    try:
        result = await make_function_async(generate_prompt_output, "")
        return result
    except Exception as e:
        logging.error(f"Error while output generation. {e}")
        return e


@router.get(
    "/auto-output-generation-temporary", dependencies=[Depends(check_token_middleware)]
)
async def auto_output_generation():
    try:
        result = await make_function_async(generate_prompt_output_temporary, "")
        return result
    except Exception as e:
        logging.error(f"Error while output generation. {e}")
        return e


@router.post(
    "/generate-all-course-data", dependencies=[Depends(check_token_middleware)]
)
async def generation_degree(item: DegreeData):
    try:
        logging.info(
            f"Starting degree data generation for institute: {item.institute_ids}"
        )
        result = await make_function_async(run_on_all_courses, item)
        return result
    except Exception as e:
        logging.error(f"Error while generating degree data. {e}")
        return e