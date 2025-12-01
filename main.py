from fastapi import (
    FastAPI, File, UploadFile, Form, HTTPException, BackgroundTasks
)
from fastapi.responses import FileResponse
from pydantic import BaseModel
import subprocess
import hashlib
import uuid
import os
from pathlib import Path
from typing import Optional, List

app = FastAPI(
    title="Video Processing API",
    description="""
This API allows you to upload, convert, cache, and manage video files using **FFmpeg**.

### Main Features:
- Upload and hash videos
- Convert videos using arbitrary FFmpeg parameters
- Reuse cached uploads by hash for faster conversions
- Health check for FFmpeg availability  
- Automatic file cleanup after operations
""",
    version="1.0.0"
)

# Directories
TEMP_DIR = Path("temp")
CACHE_DIR = Path("cache")


# -----------------------------
# Models for Swagger
# -----------------------------
class UploadResponse(BaseModel):
    hash: str
    ext: str
    path: str


class DeleteCacheResponse(BaseModel):
    message: str


class HealthResponse(BaseModel):
    status: str
    ffmpeg: str


class ErrorResponse(BaseModel):
    error: str
    stderr: Optional[str] = None


# -----------------------------
# Initialization
# -----------------------------
@app.on_event("startup")
async def startup_event():
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


# -----------------------------
# Utility functions
# -----------------------------
def cleanup_files_sync(*paths):
    """Synchronously remove given file paths."""
    for path in paths:
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except:
                pass


def cleanup_files(*paths):
    """Return background cleanup task."""
    tasks = BackgroundTasks()
    tasks.add_task(cleanup_files_sync, *paths)
    return tasks


# -----------------------------
# Upload Endpoint
# -----------------------------
@app.post(
    "/upload",
    tags=["Upload & Cache"],
    summary="Upload and cache a video",
    response_model=UploadResponse,
    responses={
        200: {"description": "Upload successful"},
        400: {"description": "Invalid upload"},
    },
)
async def upload_video(file: UploadFile = File(..., description="Video file to upload")):
    """
    Upload a video file and store it in the **cache** directory.

    - The file is hashed using SHA-256.
    - Files with identical content will share the same hash.
    - The API returns the computed hash and stored file path.
    """
    content = await file.read()
    video_hash = hashlib.sha256(content).hexdigest()

    filename = file.filename or "video.dat"
    ext = Path(filename).suffix or ".mp4"

    file_path = CACHE_DIR / f"{video_hash}{ext}"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(file_path, "wb") as f:
        f.write(content)

    return UploadResponse(
        hash=video_hash,
        ext=ext,
        path=str(file_path.relative_to(CACHE_DIR))
    )


# -----------------------------
# Convert Uploaded File
# -----------------------------
@app.post(
    "/convert",
    tags=["Conversion"],
    summary="Convert an uploaded video using FFmpeg",
    responses={
        200: {"description": "Conversion successful"},
        500: {"model": ErrorResponse, "description": "FFmpeg conversion failed"},
    },
)
async def convert_video(
    file: UploadFile = File(..., description="Video file to convert"),
    output_format: str = Form(default="mp4", description="Output file format (e.g., mp4, avi, mkv)"),
    params: Optional[str] = Form(
        default="",
        description="Additional FFmpeg parameters (e.g. `-vf scale=1280:720 -b:v 2M`)"
    )
):
    """
    Convert a provided video file to a target format using **FFmpeg**.

    - The uploaded file is written to a temporary directory.
    - Any FFmpeg parameters can be passed directly.
    - The processed output is returned as a downloadable file.
    """
    input_path = None
    output_path = None

    try:
        unique_id = str(uuid.uuid4())

        filename = file.filename or f"uploaded.{output_format}"
        input_extension = filename.rsplit(".", 1)[-1].lower() if "." in filename else "tmp"

        input_path = str(TEMP_DIR / f"{unique_id}_input.{input_extension}")
        output_path = str(TEMP_DIR / f"{unique_id}_output.{output_format}")

        original_name = Path(filename).stem
        download_name = f"{original_name}_converted.{output_format}"

        # Save input
        with open(input_path, "wb") as buffer:
            buffer.write(await file.read())

        # FFmpeg command
        cmd = ["ffmpeg", "-i", input_path]
        if params:
            cmd.extend(params.split())
        cmd.append(output_path)

        # Execute
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise HTTPException(
                status_code=500,
                detail={"error": "FFmpeg conversion failed", "stderr": result.stderr}
            )

        # Return response
        return FileResponse(
            path=output_path,
            filename=download_name,
            media_type="application/octet-stream",
            background=cleanup_files(input_path, output_path)
        )

    except HTTPException:
        cleanup_files_sync(input_path, output_path)
        raise
    except Exception as e:
        cleanup_files_sync(input_path, output_path)
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# Convert Cached File
# -----------------------------
@app.post(
    "/convert-hash",
    tags=["Conversion"],
    summary="Convert a cached video using its hash",
    responses={
        200: {"description": "Conversion successful"},
        404: {"description": "Cached video not found"},
        500: {"model": ErrorResponse, "description": "FFmpeg conversion failed"},
    }
)
async def convert_from_hash(
    video_hash: str = Form(..., description="SHA-256 hash of the uploaded video"),
    output_format: str = Form(default="mp4", description="Desired output format"),
    params: Optional[str] = Form(default="", description="Extra FFmpeg parameters"),
):
    """
    Convert a **previously uploaded and cached** video using its hash.

    This avoids uploading the same file repeatedly and speeds up conversion workflows.
    """
    cached_files = list(CACHE_DIR.glob(f"{video_hash}.*"))
    if not cached_files:
        raise HTTPException(status_code=404, detail="Cached video not found")

    source_path = cached_files[0]

    unique_id = str(uuid.uuid4())
    input_path = str(TEMP_DIR / f"{unique_id}_input{source_path.suffix}")
    output_path = str(TEMP_DIR / f"{unique_id}_output.{output_format}")
    download_name = f"{video_hash}_converted.{output_format}"

    # Copy to temp
    with open(input_path, "wb") as out_f, open(source_path, "rb") as in_f:
        out_f.write(in_f.read())

    # ffmpeg command
    cmd = ["ffmpeg", "-i", input_path]
    if params:
        cmd.extend(params.split())
    cmd.append(output_path)

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        cleanup_files_sync(input_path, output_path)
        raise HTTPException(
            status_code=500,
            detail={"error": "FFmpeg conversion failed", "stderr": result.stderr}
        )

    return FileResponse(
        path=output_path,
        filename=download_name,
        media_type="application/octet-stream",
        background=cleanup_files(input_path, output_path)
    )


# -----------------------------
# Delete From Cache
# -----------------------------
@app.delete(
    "/cache/{video_hash}",
    tags=["Upload & Cache"],
    summary="Delete cached files by hash",
    response_model=DeleteCacheResponse
)
async def delete_video(video_hash: str):
    """
    Remove all cached files that match a particular SHA-256 hash.
    """
    files = list(CACHE_DIR.glob(f"{video_hash}.*"))
    if not files:
        raise HTTPException(status_code=404, detail="Video cache not found")

    deleted = 0
    for f in files:
        try:
            f.unlink()
            deleted += 1
        except:
            pass

    return DeleteCacheResponse(
        message=f"Deleted {deleted} file(s) for hash {video_hash}"
    )


# -----------------------------
# Health Check
# -----------------------------
@app.get(
    "/health",
    tags=["System"],
    summary="Check server and FFmpeg health",
    response_model=HealthResponse
)
async def health_check():
    """
    Confirms that the API server is running and verifies **FFmpeg** availability.
    """
    try:
        result = subprocess.run(
            ["ffmpeg", "-version"],
            capture_output=True,
            timeout=5,
            text=True
        )
        return HealthResponse(status="healthy", ffmpeg=result.stdout)

    except:
        raise HTTPException(
            status_code=503,
            detail={"status": "unhealthy", "ffmpeg": "not available"}
        )