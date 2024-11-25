# Library
import asyncio
import logging
import os
import signal
from typing import Dict
from multiprocessing import Process
from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends

# Modules
from .Interfaces import InstituteIds
from .utils import make_function_async
from .controller import download_and_save_scrape_data, auto_run_scrapper
from utils.auth_utils import check_token_middleware

# Router
router = APIRouter()

# Inilialization
auto_run_process_scrapper: Dict[str, Process] = {}


# Auto Run Scraper
@router.post("/start-auto-run", dependencies=[Depends(check_token_middleware)])
async def save_college_data_auto_run(background_tasks: BackgroundTasks):
    global auto_run_process_scrapper
    if (
        auto_run_process_scrapper
        and auto_run_process_scrapper.get("process")
        and auto_run_process_scrapper["process"].is_alive()
    ):
        raise HTTPException(
            status_code=400, detail="Auto-run process is already running"
        )

    process = Process(target=auto_run_scrapper)
    process.start()
    auto_run_process_scrapper["process"] = process
    return {"message": "Auto-run process started"}


@router.get("/auto-run-status", dependencies=[Depends(check_token_middleware)])
async def get_auto_run_status():
    if (
        auto_run_process_scrapper
        and auto_run_process_scrapper.get("process")
        and auto_run_process_scrapper["process"].is_alive()
    ):
        return {"status": "running"}
    return {"status": "stopped"}


@router.post("/stop-auto-run", dependencies=[Depends(check_token_middleware)])
async def stop_auto_run():
    global auto_run_process_scrapper
    if (
        not auto_run_process_scrapper
        or not auto_run_process_scrapper.get("process")
        or not auto_run_process_scrapper["process"].is_alive()
    ):
        raise HTTPException(
            status_code=400, detail="No auto-run process is currently running"
        )

    process = auto_run_process_scrapper["process"]
    process.terminate()

    for _ in range(50):
        if not process.is_alive():
            break
        asyncio.sleep(0.1)

    if process.is_alive():
        os.kill(process.pid, signal.SIGKILL)

    process.join()
    auto_run_process_scrapper.clear()
    return {"message": "Auto-run process stopped"}


# Manual Scraper
@router.post("/scrape_institutes", dependencies=[Depends(check_token_middleware)])
async def extract_scrape_data(item: InstituteIds):
    process = Process(target=download_and_save_scrape_data, args=(item,))
    process.start()
    return {"message": "Scraping process started"}


async def global_exception_handler(request, exc):
    logging.error(f"Unhandled exception: {str(exc)}")
    return {"detail": "An unexpected error occurred"}
