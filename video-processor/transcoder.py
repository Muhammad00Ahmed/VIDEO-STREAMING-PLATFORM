import subprocess
import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class VideoTranscoder:
    """
    Video transcoding service using FFmpeg for multi-resolution encoding
    and adaptive bitrate streaming (HLS/DASH).
    """
    
    def __init__(self, input_dir='./uploads', output_dir='./transcoded'):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.input_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        
        # Video quality presets
        self.quality_presets = {
            '2160p': {'width': 3840, 'height': 2160, 'bitrate': '15000k', 'audio_bitrate': '192k'},
            '1080p': {'width': 1920, 'height': 1080, 'bitrate': '5000k', 'audio_bitrate': '128k'},
            '720p': {'width': 1280, 'height': 720, 'bitrate': '2500k', 'audio_bitrate': '128k'},
            '480p': {'width': 854, 'height': 480, 'bitrate': '1000k', 'audio_bitrate': '96k'},
            '360p': {'width': 640, 'height': 360, 'bitrate': '600k', 'audio_bitrate': '96k'}
        }
    
    def transcode_video(self, input_file: str, video_id: str, 
                       resolutions: List[str] = None) -> Dict:
        """
        Transcode video to multiple resolutions
        
        Args:
            input_file: Path to input video file
            video_id: Unique identifier for the video
            resolutions: List of resolutions to generate (default: all)
            
        Returns:
            Dictionary with transcoding results
        """
        if resolutions is None:
            resolutions = list(self.quality_presets.keys())
        
        input_path = self.input_dir / input_file
        
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")
        
        logger.info(f"Starting transcoding for video {video_id}")
        
        # Get video metadata
        metadata = self._get_video_metadata(str(input_path))
        
        # Create output directory for this video
        video_output_dir = self.output_dir / video_id
        video_output_dir.mkdir(exist_ok=True)
        
        results = {
            'video_id': video_id,
            'metadata': metadata,
            'resolutions': {},
            'thumbnails': [],
            'preview': None
        }
        
        # Transcode to each resolution
        for resolution in resolutions:
            if resolution not in self.quality_presets:
                logger.warning(f"Unknown resolution: {resolution}, skipping")
                continue
            
            logger.info(f"Transcoding to {resolution}")
            
            output_file = self._transcode_resolution(
                str(input_path),
                str(video_output_dir),
                resolution,
                self.quality_presets[resolution]
            )
            
            if output_file:
                results['resolutions'][resolution] = output_file
        
        # Generate HLS playlist
        hls_playlist = self._generate_hls_playlist(
            str(video_output_dir),
            results['resolutions']
        )
        results['hls_playlist'] = hls_playlist
        
        # Generate thumbnails
        thumbnails = self._generate_thumbnails(
            str(input_path),
            str(video_output_dir),
            count=10
        )
        results['thumbnails'] = thumbnails
        
        # Generate preview clip
        preview = self._generate_preview(
            str(input_path),
            str(video_output_dir),
            duration=30
        )
        results['preview'] = preview
        
        logger.info(f"Transcoding completed for video {video_id}")
        
        return results
    
    def _get_video_metadata(self, input_file: str) -> Dict:
        """Extract video metadata using FFprobe"""
        cmd = [
            'ffprobe',
            '-v', 'quiet',
            '-print_format', 'json',
            '-show_format',
            '-show_streams',
            input_file
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            data = json.loads(result.stdout)
            
            # Extract relevant metadata
            video_stream = next(
                (s for s in data['streams'] if s['codec_type'] == 'video'),
                None
            )
            audio_stream = next(
                (s for s in data['streams'] if s['codec_type'] == 'audio'),
                None
            )
            
            metadata = {
                'duration': float(data['format'].get('duration', 0)),
                'size': int(data['format'].get('size', 0)),
                'bitrate': int(data['format'].get('bit_rate', 0)),
                'format': data['format'].get('format_name'),
            }
            
            if video_stream:
                metadata.update({
                    'width': int(video_stream.get('width', 0)),
                    'height': int(video_stream.get('height', 0)),
                    'codec': video_stream.get('codec_name'),
                    'fps': eval(video_stream.get('r_frame_rate', '0/1'))
                })
            
            if audio_stream:
                metadata['audio_codec'] = audio_stream.get('codec_name')
                metadata['audio_channels'] = audio_stream.get('channels')
            
            return metadata
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error getting video metadata: {e}")
            return {}
    
    def _transcode_resolution(self, input_file: str, output_dir: str,
                            resolution: str, preset: Dict) -> Optional[str]:
        """Transcode video to specific resolution"""
        output_file = os.path.join(output_dir, f"{resolution}.mp4")
        
        cmd = [
            'ffmpeg',
            '-i', input_file,
            '-vf', f"scale={preset['width']}:{preset['height']}",
            '-c:v', 'libx264',
            '-preset', 'medium',
            '-b:v', preset['bitrate'],
            '-maxrate', preset['bitrate'],
            '-bufsize', str(int(preset['bitrate'].replace('k', '')) * 2) + 'k',
            '-c:a', 'aac',
            '-b:a', preset['audio_bitrate'],
            '-movflags', '+faststart',
            '-y',
            output_file
        ]
        
        try:
            subprocess.run(cmd, check=True, capture_output=True)
            logger.info(f"Successfully transcoded to {resolution}")
            return output_file
        except subprocess.CalledProcessError as e:
            logger.error(f"Error transcoding to {resolution}: {e}")
            return None
    
    def _generate_hls_playlist(self, output_dir: str, 
                              resolutions: Dict[str, str]) -> Optional[str]:
        """Generate HLS master playlist and variant playlists"""
        hls_dir = os.path.join(output_dir, 'hls')
        os.makedirs(hls_dir, exist_ok=True)
        
        master_playlist = os.path.join(hls_dir, 'master.m3u8')
        
        # Generate variant playlists for each resolution
        variant_playlists = []
        
        for resolution, video_file in resolutions.items():
            preset = self.quality_presets[resolution]
            variant_name = f"{resolution}.m3u8"
            variant_path = os.path.join(hls_dir, variant_name)
            
            cmd = [
                'ffmpeg',
                '-i', video_file,
                '-c', 'copy',
                '-start_number', '0',
                '-hls_time', '6',
                '-hls_list_size', '0',
                '-f', 'hls',
                '-hls_segment_filename', os.path.join(hls_dir, f"{resolution}_%03d.ts"),
                '-y',
                variant_path
            ]
            
            try:
                subprocess.run(cmd, check=True, capture_output=True)
                variant_playlists.append({
                    'resolution': resolution,
                    'bandwidth': int(preset['bitrate'].replace('k', '')) * 1000,
                    'playlist': variant_name
                })
            except subprocess.CalledProcessError as e:
                logger.error(f"Error generating HLS for {resolution}: {e}")
        
        # Create master playlist
        with open(master_playlist, 'w') as f:
            f.write('#EXTM3U\n')
            f.write('#EXT-X-VERSION:3\n\n')
            
            for variant in sorted(variant_playlists, 
                                key=lambda x: x['bandwidth'], 
                                reverse=True):
                preset = self.quality_presets[variant['resolution']]
                f.write(f"#EXT-X-STREAM-INF:BANDWIDTH={variant['bandwidth']},"
                       f"RESOLUTION={preset['width']}x{preset['height']}\n")
                f.write(f"{variant['playlist']}\n\n")
        
        logger.info("HLS playlist generated successfully")
        return master_playlist
    
    def _generate_thumbnails(self, input_file: str, output_dir: str,
                           count: int = 10) -> List[str]:
        """Generate video thumbnails at regular intervals"""
        thumbnails_dir = os.path.join(output_dir, 'thumbnails')
        os.makedirs(thumbnails_dir, exist_ok=True)
        
        # Get video duration
        metadata = self._get_video_metadata(input_file)
        duration = metadata.get('duration', 0)
        
        if duration == 0:
            return []
        
        interval = duration / (count + 1)
        thumbnails = []
        
        for i in range(1, count + 1):
            timestamp = interval * i
            output_file = os.path.join(thumbnails_dir, f"thumb_{i:02d}.jpg")
            
            cmd = [
                'ffmpeg',
                '-ss', str(timestamp),
                '-i', input_file,
                '-vframes', '1',
                '-vf', 'scale=320:-1',
                '-y',
                output_file
            ]
            
            try:
                subprocess.run(cmd, check=True, capture_output=True)
                thumbnails.append(output_file)
            except subprocess.CalledProcessError as e:
                logger.error(f"Error generating thumbnail {i}: {e}")
        
        logger.info(f"Generated {len(thumbnails)} thumbnails")
        return thumbnails
    
    def _generate_preview(self, input_file: str, output_dir: str,
                         duration: int = 30) -> Optional[str]:
        """Generate preview clip from the beginning of the video"""
        output_file = os.path.join(output_dir, 'preview.mp4')
        
        cmd = [
            'ffmpeg',
            '-i', input_file,
            '-t', str(duration),
            '-vf', 'scale=640:-1',
            '-c:v', 'libx264',
            '-preset', 'fast',
            '-b:v', '500k',
            '-c:a', 'aac',
            '-b:a', '96k',
            '-movflags', '+faststart',
            '-y',
            output_file
        ]
        
        try:
            subprocess.run(cmd, check=True, capture_output=True)
            logger.info("Preview clip generated successfully")
            return output_file
        except subprocess.CalledProcessError as e:
            logger.error(f"Error generating preview: {e}")
            return None


class S3VideoUploader:
    """Upload transcoded videos to AWS S3"""
    
    def __init__(self, bucket_name: str, region: str = 'us-east-1'):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name=region)
    
    def upload_video_files(self, video_id: str, local_dir: str) -> Dict[str, str]:
        """Upload all video files to S3"""
        uploaded_files = {}
        
        local_path = Path(local_dir)
        
        for file_path in local_path.rglob('*'):
            if file_path.is_file():
                relative_path = file_path.relative_to(local_path)
                s3_key = f"videos/{video_id}/{relative_path}"
                
                try:
                    self.s3_client.upload_file(
                        str(file_path),
                        self.bucket_name,
                        s3_key,
                        ExtraArgs={'ContentType': self._get_content_type(file_path)}
                    )
                    
                    url = f"https://{self.bucket_name}.s3.amazonaws.com/{s3_key}"
                    uploaded_files[str(relative_path)] = url
                    
                    logger.info(f"Uploaded {relative_path} to S3")
                    
                except ClientError as e:
                    logger.error(f"Error uploading {relative_path}: {e}")
        
        return uploaded_files
    
    def _get_content_type(self, file_path: Path) -> str:
        """Get content type based on file extension"""
        extension = file_path.suffix.lower()
        content_types = {
            '.mp4': 'video/mp4',
            '.m3u8': 'application/vnd.apple.mpegurl',
            '.ts': 'video/mp2t',
            '.jpg': 'image/jpeg',
            '.png': 'image/png'
        }
        return content_types.get(extension, 'application/octet-stream')


# Example usage
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize transcoder
    transcoder = VideoTranscoder()
    
    # Transcode video
    results = transcoder.transcode_video(
        input_file='sample_video.mp4',
        video_id='video_123',
        resolutions=['1080p', '720p', '480p', '360p']
    )
    
    print("\nTranscoding Results:")
    print(f"Video ID: {results['video_id']}")
    print(f"Duration: {results['metadata']['duration']} seconds")
    print(f"Resolutions: {list(results['resolutions'].keys())}")
    print(f"Thumbnails: {len(results['thumbnails'])}")
    print(f"HLS Playlist: {results['hls_playlist']}")
    
    # Upload to S3 (optional)
    # uploader = S3VideoUploader(bucket_name='my-video-bucket')
    # uploaded = uploader.upload_video_files('video_123', './transcoded/video_123')
    # print(f"\nUploaded {len(uploaded)} files to S3")