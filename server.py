import os
import logging
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import asyncio
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os
import logging
from typing import Dict
from multiprocessing import Process
from utils.auto_generate_embeddings import auto_run
from utils.auto_generate_validation import auto_run_validation
from utils.auto_generate_transformation import auto_run_transformation
import signal
from utils.auth_utils import check_token_middleware

auto_run_process_embedding: Dict[str, Process] = {}
auto_run_process_validation: Dict[str, Process] = {}
auto_run_process_transformation: Dict[str, Process] = {}

# Routes
from crawling.routes import router as crawling_router
from embedding.routes import router as embedding_router
from output_generation.routes import router as output_generation_router
from saarthi_dashboard.routes import router as saarthi_dashboard_router

load_dotenv()
log_files_folder = os.environ.get("LOG_FILES_FOLDER")

try:
    log_file_path = os.path.join(log_files_folder, "server_logs.log")
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format="%(asctime)s - %(module)s - %(funcName)s - %(levelname)s - %(message)s",
    )
    logging.info("Logging started successfully.")
except Exception as e:
    print(f"Failed to set up logging: {e}")


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(crawling_router)
app.include_router(embedding_router)
app.include_router(output_generation_router)
app.include_router(saarthi_dashboard_router)


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logging.error(f"Unhandled exception: {str(exc)}")
    return {"detail": "An unexpected error occurred"}


@app.post("/start_auto_run_embedding", dependencies=[Depends(check_token_middleware)])
async def start_auto_run():
    global auto_run_process_embedding
    if (
        auto_run_process_embedding
        and auto_run_process_embedding.get("process")
        and auto_run_process_embedding["process"].is_alive()
    ):
        raise HTTPException(
            status_code=400, detail="Auto-run process is already running"
        )

    process = Process(target=auto_run)
    process.start()
    auto_run_process_embedding["process"] = process
    return {"message": "Auto-run process started"}


@app.post("/stop_auto_run_embedding", dependencies=[Depends(check_token_middleware)])
async def stop_auto_run():
    global auto_run_process_embedding
    if (
        not auto_run_process_embedding
        or not auto_run_process_embedding.get("process")
        or not auto_run_process_embedding["process"].is_alive()
    ):
        raise HTTPException(
            status_code=400, detail="No auto-run process is currently running"
        )

    process = auto_run_process_embedding["process"]
    process.terminate()

    for _ in range(50):
        if not process.is_alive():
            break
        await asyncio.sleep(0.1)

    if process.is_alive():
        os.kill(process.pid, signal.SIGKILL)

    process.join()
    auto_run_process_embedding.clear()
    return {"message": "Auto-run process stopped"}


@app.get("/status_auto_run_embedding", dependencies=[Depends(check_token_middleware)])
async def get_status():
    if (
        auto_run_process_embedding
        and auto_run_process_embedding.get("process")
        and auto_run_process_embedding["process"].is_alive()
    ):
        return {"status": "running"}
    return {"status": "stopped"}


@app.post("/start_auto_run_validation", dependencies=[Depends(check_token_middleware)])
async def start_auto_run():
    global auto_run_process_validation
    if (
        auto_run_process_validation
        and auto_run_process_validation.get("process")
        and auto_run_process_validation["process"].is_alive()
    ):
        raise HTTPException(
            status_code=400, detail="Auto-run process is already running for Validation"
        )

    process = Process(target=auto_run_validation)
    process.start()
    auto_run_process_validation["process"] = process
    return {"message": "Auto-run process started for Validation"}


@app.post("/stop_auto_run_validation", dependencies=[Depends(check_token_middleware)])
async def stop_auto_run_validation():
    global auto_run_process_validation
    if (
        not auto_run_process_validation
        or not auto_run_process_validation.get("process")
        or not auto_run_process_validation["process"].is_alive()
    ):
        raise HTTPException(
            status_code=400, detail="No auto-run process is currently running"
        )

    process = auto_run_process_validation["process"]
    process.terminate()

    for _ in range(50):
        if not process.is_alive():
            break
        await asyncio.sleep(0.1)

    if process.is_alive():
        os.kill(process.pid, signal.SIGKILL)

    process.join()
    auto_run_process_validation.clear()
    return {"message": "Auto-run process stopped"}


@app.post(
    "/start_auto_run_transformation", dependencies=[Depends(check_token_middleware)]
)
async def start_auto_run():
    global auto_run_process_transformation
    if (
        auto_run_process_transformation
        and auto_run_process_transformation.get("process")
        and auto_run_process_transformation["process"].is_alive()
    ):
        raise HTTPException(
            status_code=400,
            detail="Auto-run process is already running for Transformation",
        )

    process = Process(target=auto_run_transformation)
    process.start()
    auto_run_process_transformation["process"] = process
    return {"message": "Auto-run process started for Transformation"}


@app.post(
    "/stop_auto_run_transformation", dependencies=[Depends(check_token_middleware)]
)
async def stop_auto_run_validation():
    global auto_run_process_transformation
    if (
        not auto_run_process_transformation
        or not auto_run_process_transformation.get("process")
        or not auto_run_process_transformation["process"].is_alive()
    ):
        raise HTTPException(
            status_code=400, detail="No auto-run process is currently running"
        )

    process = auto_run_process_transformation["process"]
    process.terminate()

    for _ in range(50):
        if not process.is_alive():
            break
        await asyncio.sleep(0.1)

    if process.is_alive():
        os.kill(process.pid, signal.SIGKILL)

    process.join()
    auto_run_process_transformation.clear()
    return {"message": "Auto-run process stopped"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("server:app", host="0.0.0.0", port=3002)
