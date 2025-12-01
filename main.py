from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import FileResponse
import subprocess
import os
import uuid
from pathlib import Path
import tempfile
from typing import Optional

app = FastAPI()

# Configure upload and output directories
UPLOAD_FOLDER = tempfile.gettempdir()

@app.post("/convert")
async def convert_video(
    file: UploadFile = File(...),
    output_format: str = Form(default="mp4"),
    params: Optional[str] = Form(default="")
):
    input_path = None
    output_path = None
    
    try:
        # Generate unique filenames
        unique_id = str(uuid.uuid4())
        # Protect against missing filename (UploadFile.filename can be None)
        filename = file.filename or f"uploaded.{output_format}"
        if '.' in filename:
            input_extension = filename.rsplit('.', 1)[1].lower()
        else:
            input_extension = 'tmp'
        input_path = os.path.join(UPLOAD_FOLDER, f"{unique_id}_input.{input_extension}")
        output_path = os.path.join(UPLOAD_FOLDER, f"{unique_id}_output.{output_format}")
        
        # Create download name from original filename
        original_name = Path(filename).stem
        download_name = f"{original_name}_converted.{output_format}"
        
        # Save uploaded file
        with open(input_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Build FFmpeg command
        cmd = ['ffmpeg', '-i', input_path]
        
        # Add custom parameters if provided
        if params:
            cmd.extend(params.split())
        
        # Add output file
        cmd.append(output_path)
        
        # Execute FFmpeg (no timeout)
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            raise HTTPException(
                status_code=500,
                detail={
                    'error': 'FFmpeg conversion failed',
                    'stderr': result.stderr
                }
            )
        
        # Return the converted file
        return FileResponse(
            path=output_path,
            filename=download_name,
            media_type='application/octet-stream',
            background=cleanup_files(input_path, output_path)
        )
    
    except HTTPException:
        # Cleanup on HTTP error
        cleanup_files_sync(input_path, output_path)
        raise
    
    except Exception as e:
        # Cleanup on any other error
        cleanup_files_sync(input_path, output_path)
        raise HTTPException(status_code=500, detail=str(e))

def cleanup_files_sync(input_path, output_path):
    """Synchronously cleanup files"""
    for path in [input_path, output_path]:
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except:
                pass

def cleanup_files(input_path, output_path):
    """Return a background task for cleanup"""
    from fastapi import BackgroundTasks
    tasks = BackgroundTasks()
    tasks.add_task(cleanup_files_sync, input_path, output_path)
    return tasks

@app.get("/health")
async def health_check():
    """Check if FFmpeg is available"""
    try:
        result = subprocess.run(['ffmpeg', '-version'], capture_output=True, timeout=5, text=True)
        return {'status': 'healthy', 'ffmpeg': result.stdout}
    except:
        raise HTTPException(
            status_code=503,
            detail={'status': 'unhealthy', 'ffmpeg': 'not available'}
        )

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)