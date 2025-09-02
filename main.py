from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, HttpUrl
import os
import tempfile
import uuid
import asyncio
import aiofiles
import aiohttp
from moviepy.editor import ImageClip, AudioFileClip, vfx
from PIL import Image
from typing import Optional
import logging
from datetime import datetime
import time
import gc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Image + Audio to Video API",
    description="Combine an image and audio file into a video",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

OUTPUT_DIR = "generated_videos"
os.makedirs(OUTPUT_DIR, exist_ok=True)


job_status = {}


class VideoRequest(BaseModel):
    image_url: HttpUrl
    audio_url: HttpUrl
    fps: Optional[int] = 24
    output_filename: Optional[str] = None


class JobStatus(BaseModel):
    job_id: str
    status: str  # "processing", "completed", "failed"
    message: str
    download_url: Optional[str] = None
    created_at: str


async def download_file_async(
    session: aiohttp.ClientSession, url: str, filepath: str
) -> bool:
    """Download a file asynchronously."""
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            async with aiofiles.open(filepath, "wb") as f:
                async for chunk in response.content.iter_chunked(8192):
                    await f.write(chunk)

        await asyncio.sleep(0.1)
        return True
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        return False


def get_image_dimensions(image_path: str) -> tuple:
    """Get image dimensions."""
    try:
        with Image.open(image_path) as img:
            return img.size
    except Exception as e:
        logger.error(f"Error getting image dimensions: {e}")
        return (1920, 1080)


async def create_video_async(
    job_id: str, image_url: str, audio_url: str, output_filename: str, fps: int = 24
):
    """Create video asynchronously."""
    try:
        job_status[job_id]["status"] = "processing"
        job_status[job_id]["message"] = "Downloading files..."

        with tempfile.TemporaryDirectory() as temp_dir:
            image_path = os.path.join(temp_dir, "temp_image.jpg")
            audio_path = os.path.join(temp_dir, "temp_audio.mp3")

            async with aiohttp.ClientSession() as session:
                image_success = await download_file_async(
                    session, image_url, image_path
                )
                if not image_success:
                    raise Exception("Failed to download image")

                audio_success = await download_file_async(
                    session, audio_url, audio_path
                )
                if not audio_success:
                    raise Exception("Failed to download audio")

            await asyncio.sleep(1.0)

            job_status[job_id]["message"] = "Processing video..."

            def create_video():
                audio_clip = None
                image_clip = None
                final_video = None

                try:
                    time.sleep(1.0)

                    # file validation
                    if not os.path.exists(audio_path):
                        raise Exception("Audio file not found after download")
                    if not os.path.exists(image_path):
                        raise Exception("Image file not found after download")

                    # audio clip
                    audio_clip = AudioFileClip(audio_path)
                    audio_duration = audio_clip.duration

                    # dimensions
                    img_width, img_height = get_image_dimensions(image_path)

                    # image clip
                    image_clip = ImageClip(image_path, duration=audio_duration)

                    # resive but keep aspect ratio
                    if img_width > 1920 or img_height > 1080:
                        image_clip = image_clip.fx(vfx.resize, height=1080)

                    # combine image and audio
                    final_video = image_clip.set_audio(audio_clip)

                    output_path = os.path.join(OUTPUT_DIR, output_filename)

                    final_video.write_videofile(
                        output_path,
                        fps=fps,
                        codec="libx264",
                        audio_codec="aac",
                        remove_temp=True,
                        verbose=False,
                        logger=None,
                    )

                    return output_path

                except Exception as e:
                    logger.error(f"MoviePy error: {e}")
                    raise e
                finally:
                    # cleanup order
                    if final_video:
                        try:
                            final_video.close()
                        except:
                            pass
                    if image_clip:
                        try:
                            image_clip.close()
                        except:
                            pass
                    if audio_clip:
                        try:
                            audio_clip.close()
                        except:
                            pass

                    gc.collect()
                    time.sleep(0.5)

            # thread pool executor
            try:
                output_path = await asyncio.get_event_loop().run_in_executor(
                    None, create_video
                )

                job_status[job_id]["status"] = "completed"
                job_status[job_id]["message"] = "Video created successfully"
                job_status[job_id]["download_url"] = f"/download/{output_filename}"

            except Exception as e:
                raise e

    except Exception as e:
        logger.error(f"Error creating video for job {job_id}: {e}")
        job_status[job_id]["status"] = "failed"
        job_status[job_id]["message"] = f"Error: {str(e)}"


def cleanup_old_files():
    """Clean up files older than 1 hour."""
    try:
        current_time = time.time()
        for filename in os.listdir(OUTPUT_DIR):
            filepath = os.path.join(OUTPUT_DIR, filename)
            if os.path.isfile(filepath):
                file_age = current_time - os.path.getctime(filepath)
                if file_age > 3600:  # 1 hour
                    os.remove(filepath)
                    logger.info(f"Cleaned up old file: {filename}")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")


@app.on_event("startup")
async def startup_event():
    """Run cleanup on startup and schedule periodic cleanup."""
    cleanup_old_files()


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Image + Audio to Video API",
        "version": "1.0.0",
        "endpoints": {
            "POST /create-video": "Create video from image and audio URLs",
            "GET /status/{job_id}": "Check job status",
            "GET /download/{filename}": "Download completed video",
        },
    }


@app.post("/create-video")
async def create_video(request: VideoRequest, background_tasks: BackgroundTasks):
    """
    Create a video from image and audio URLs.

    Returns a job_id to track the progress.
    """
    try:
        # unique job ID
        job_id = str(uuid.uuid4())

        # output filename
        if request.output_filename:
            output_filename = request.output_filename
            if not output_filename.endswith(".mp4"):
                output_filename += ".mp4"
        else:
            output_filename = f"video_{job_id}.mp4"

        job_status[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "message": "Job queued for processing",
            "download_url": None,
            "created_at": datetime.now().isoformat(),
        }

        background_tasks.add_task(
            create_video_async,
            job_id,
            str(request.image_url),
            str(request.audio_url),
            output_filename,
            request.fps,
        )

        background_tasks.add_task(cleanup_old_files)

        return {
            "job_id": job_id,
            "status": "queued",
            "message": "Video creation job started",
            "status_url": f"/status/{job_id}",
        }

    except Exception as e:
        logger.error(f"Error starting video creation: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error starting video creation: {str(e)}"
        )


@app.get("/status/{job_id}")
async def get_job_status(job_id: str):
    """Get the status of a video creation job."""
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail="Job not found")

    return job_status[job_id]


@app.get("/download/{filename}")
async def download_video(filename: str):
    """Download a generated video file."""
    filepath = os.path.join(OUTPUT_DIR, filename)

    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(filepath, media_type="video/mp4", filename=filename)


@app.delete("/cleanup")
async def manual_cleanup():
    """Manually trigger cleanup of old files."""
    try:
        cleanup_old_files()
        return {"message": "Cleanup completed successfully"}
    except Exception as e:
        logger.error(f"Manual cleanup failed: {e}")
        raise HTTPException(status_code=500, detail="Cleanup failed")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_jobs": len(
            [j for j in job_status.values() if j["status"] == "processing"]
        ),
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info")
