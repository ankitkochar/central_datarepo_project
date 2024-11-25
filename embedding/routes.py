# Library
from fastapi import APIRouter, Depends
import logging

# Modules
from crawling.Interfaces import InstituteIds
from .utils import make_function_async
from .controller import extract_and_save_college_data
from utils.auth_utils import check_token_middleware

# Router
router = APIRouter()


@router.post(
    "/embedding_institutes", dependencies=[Depends(check_token_middleware)]
)
async def save_college_data(item: InstituteIds):
    logging.info(f"Starting data extraction for IDs: {item.institute_ids}")
    response = await make_function_async(extract_and_save_college_data, item)
    logging.info(f"Data extraction completed for IDs: {item.institute_ids}")

    return response
