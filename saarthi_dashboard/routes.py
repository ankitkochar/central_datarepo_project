# Library
from fastapi import APIRouter, File, UploadFile, Depends

# Modules
from .utils import make_function_async
from .controller import (
    fetch_all_institutes,
    prompt_crud_operations,
    populate_institutes_to_scrape,
    institute_crud,
    institute_reset,
    get_institute_latest_news,
    populate_institutes_for_recommended_url,
    run_institutes_for_recommended_url,
    get_institute_refund_policies,
    add_institute_to_master_courses,
    add_institute_specific_courses,
)
from .Interfaces import PromptCRUD, InstituteCrud, LatestNews
from utils.auth_utils import check_token_middleware, check_authorization

# Router
router = APIRouter()


# Routes
@router.get("/get-institutes", dependencies=[Depends(check_token_middleware), Depends(check_authorization)])
async def get_institutes():
    result = await make_function_async(fetch_all_institutes, "")
    return result


@router.post('/populate-institutes-to-scrape', dependencies=[Depends(check_token_middleware), Depends(check_authorization)])
async def populate_institutes(file: UploadFile = File(...)):    
    result = await make_function_async(populate_institutes_to_scrape, file)
    return result


@router.post("/populate-inst-master-courses", dependencies=[Depends(check_token_middleware)])
async def populate_master_courses(file: UploadFile = File(...)):
    result = await make_function_async(add_institute_to_master_courses, file)
    return result


@router.post("/populate-inst-specific-courses", dependencies=[Depends(check_token_middleware)])
async def populate_inst_specific_courses(file: UploadFile = File(...)):
    result = await make_function_async(add_institute_specific_courses, file)
    return result


@router.post("/prompt-crud", dependencies=[Depends(check_token_middleware), Depends(check_authorization)])
async def prompt_crud(item: PromptCRUD):
    result = await make_function_async(prompt_crud_operations, item)
    return result

@router.post('/update-institute-status', dependencies=[Depends(check_token_middleware), Depends(check_authorization)])
async def update_institute_status(item: InstituteCrud):
    result = await make_function_async(institute_crud, item)
    return result


@router.post('/reset-institutes',dependencies=[Depends(check_token_middleware), Depends(check_authorization)])
async def reset_institutes(file: UploadFile = File(...)):
    result = await make_function_async(institute_reset, file)
    return result

@router.post('/fetch-latest-news')
async def fetch_latest_news(item: LatestNews):
    result = await make_function_async(get_institute_latest_news, item)
    return result


@router.post('/fetch-refund-policy')
async def fetch_refund_policy(item: LatestNews):
    result = await make_function_async(get_institute_refund_policies, item)
    return result


@router.post('/institutes_for_recommended_url', dependencies=[Depends(check_token_middleware), Depends(check_authorization)])
async def populate_institutes(file: UploadFile = File(...)):    
    result = await make_function_async(populate_institutes_for_recommended_url, file)
    return result

@router.post('/run_for_recommended_url', dependencies=[Depends(check_token_middleware), Depends(check_authorization)])
async def update_institute_status():
    result = await make_function_async(run_institutes_for_recommended_url)
    return result
