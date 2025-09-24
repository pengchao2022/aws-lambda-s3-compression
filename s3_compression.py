import boto3
import zipfile
import io
import os
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration from environment variables
SOURCE_BUCKET = os.getenv('SOURCE_BUCKET')
TARGET_BUCKET = os.getenv('TARGET_BUCKET')
SOURCE_PREFIX = os.getenv('SOURCE_PREFIX', '')
DELETE_ORIGINAL = os.getenv('DELETE_ORIGINAL', 'false').lower() == 'true'
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
MAX_FILES = int(os.getenv('MAX_FILES', '1000'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Time configuration: MINUTES_BACK has priority over HOURS_BACK
MINUTES_BACK = os.getenv('MINUTES_BACK')
HOURS_BACK = os.getenv('HOURS_BACK')

if MINUTES_BACK:
    TIME_BACK_MINUTES = int(MINUTES_BACK)
    TIME_BACK_HOURS = None
elif HOURS_BACK:
    TIME_BACK_MINUTES = int(HOURS_BACK) * 60  # Convert hours to minutes
    TIME_BACK_HOURS = int(HOURS_BACK)
else:
    # Default to 24 hours if neither is specified
    TIME_BACK_MINUTES = 24 * 60
    TIME_BACK_HOURS = 24

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

# Initialize S3 client
s3_client = boto3.client('s3', region_name=AWS_REGION)

def validate_configuration():
    """Validate that all required configuration is present"""
    if not SOURCE_BUCKET:
        raise ValueError("SOURCE_BUCKET environment variable is required")
    
    if not SOURCE_BUCKET.strip():
        raise ValueError("SOURCE_BUCKET cannot be empty")
    
    if TIME_BACK_MINUTES <= 0:
        raise ValueError("Time back must be greater than 0")
    
    if MAX_FILES <= 0:
        raise ValueError("MAX_FILES must be greater than 0")
    
    return True

def test_s3_connection():
    """Test S3 connection and bucket access"""
    try:
        logger.info("Testing S3 connection...")
        
        # Test AWS credentials by listing buckets
        response = s3_client.list_buckets()
        bucket_names = [bucket['Name'] for bucket in response['Buckets']]
        
        logger.info(f"Available buckets: {len(bucket_names)}")
        logger.info(f"Source bucket '{SOURCE_BUCKET}' accessible: {SOURCE_BUCKET in bucket_names}")
        
        if SOURCE_BUCKET not in bucket_names:
            raise ValueError(f"Source bucket '{SOURCE_BUCKET}' not found or not accessible")
        
        # Test target bucket if different from source
        target_bucket = TARGET_BUCKET or SOURCE_BUCKET
        if target_bucket != SOURCE_BUCKET and target_bucket not in bucket_names:
            raise ValueError(f"Target bucket '{target_bucket}' not found or not accessible")
        
        logger.info("S3 connection test passed successfully")
        return True
        
    except Exception as e:
        logger.error(f"S3 connection test failed: {str(e)}")
        return False

def get_files_to_compress(cutoff_time):
    """
    Retrieve list of files from S3 that match the compression criteria
    """
    files = []
    
    try:
        logger.info(f"Searching for files in bucket: {SOURCE_BUCKET}, prefix: '{SOURCE_PREFIX}'")
        logger.info(f"Looking for files older than: {cutoff_time}")
        
        paginator = s3_client.get_paginator('list_objects_v2')
        operation_parameters = {
            'Bucket': SOURCE_BUCKET,
            'MaxKeys': 1000
        }
        
        if SOURCE_PREFIX:
            operation_parameters['Prefix'] = SOURCE_PREFIX
        
        total_files_scanned = 0
        for page_num, page in enumerate(paginator.paginate(**operation_parameters), 1):
            if 'Contents' in page:
                for obj in page['Contents']:
                    total_files_scanned += 1
                    
                    # Filter criteria
                    last_modified = obj['LastModified'].replace(tzinfo=None)
                    is_zip_file = obj['Key'].lower().endswith('.zip')
                    is_directory = obj['Key'].endswith('/')
                    file_size = obj['Size']
                    
                    # Skip if file doesn't meet criteria
                    if (last_modified >= cutoff_time or 
                        is_zip_file or 
                        is_directory or
                        file_size == 0 or
                        len(files) >= MAX_FILES):
                        continue
                    
                    files.append({
                        'key': obj['Key'],
                        'size': file_size,
                        'last_modified': last_modified,
                        'etag': obj['ETag']
                    })
                    
                    # Stop if we've reached the maximum
                    if len(files) >= MAX_FILES:
                        logger.info(f"Reached maximum file limit: {MAX_FILES}")
                        break
            
            if len(files) >= MAX_FILES:
                break
        
        logger.info(f"Scanning completed: {total_files_scanned} files scanned, {len(files)} files eligible for compression")
        
        # Sort files by modification time (oldest first)
        files.sort(key=lambda x: x['last_modified'])
        
    except Exception as e:
        logger.error(f"Error listing files from S3: {str(e)}")
        raise e
    
    return files

def create_zip_archive(files):
    """
    Create a ZIP archive containing the specified files
    """
    zip_buffer = io.BytesIO()
    
    try:
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            successful_files = 0
            total_size_compressed = 0
            
            for file_info in files:
                try:
                    # Download file from S3
                    logger.debug(f"Downloading: {file_info['key']}")
                    response = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=file_info['key'])
                    file_content = response['Body'].read()
                    
                    # Create filename for zip (use basename to avoid directory structure)
                    zip_filename = os.path.basename(file_info['key'])
                    if not zip_filename:
                        zip_filename = file_info['key'].replace('/', '_').strip('_')
                    
                    # Ensure unique filename in zip
                    if zip_filename in zip_file.namelist():
                        name, ext = os.path.splitext(zip_filename)
                        zip_filename = f"{name}_{successful_files}{ext}"
                    
                    # Add file to zip archive
                    zip_file.writestr(zip_filename, file_content)
                    total_size_compressed += len(file_content)
                    successful_files += 1
                    
                    logger.info(f"Added to zip: {file_info['key']} -> {zip_filename} ({len(file_content)} bytes)")
                    
                except Exception as e:
                    logger.error(f"Failed to process file {file_info['key']}: {str(e)}")
                    continue
        
        zip_buffer.seek(0)
        zip_size = len(zip_buffer.getvalue())
        
        logger.info(f"Zip archive created: {successful_files}/{len(files)} files processed")
        logger.info(f"Total compressed size: {zip_size} bytes")
        
        return zip_buffer
        
    except Exception as e:
        logger.error(f"Error creating zip archive: {str(e)}")
        raise e

def upload_zip_to_s3(zip_buffer):
    """
    Upload the created ZIP file to S3
    """
    try:
        target_bucket = TARGET_BUCKET or SOURCE_BUCKET
        current_time = datetime.now()
        
        # Generate target path with timestamp
        timestamp = current_time.strftime('%Y%m%d_%H%M%S')
        zip_filename = f"compressed_files_{timestamp}.zip"
        target_key = f"compressed/{current_time.strftime('%Y/%m/%d')}/{zip_filename}"
        
        logger.info(f"Uploading zip file to: s3://{target_bucket}/{target_key}")
        
        s3_client.put_object(
            Bucket=target_bucket,
            Key=target_key,
            Body=zip_buffer.getvalue(),
            ContentType='application/zip',
            ServerSideEncryption='AES256'
        )
        
        logger.info("Zip file uploaded successfully")
        return target_bucket, target_key
        
    except Exception as e:
        logger.error(f"Failed to upload zip file to S3: {str(e)}")
        raise e

def delete_original_files(files):
    """
    Delete original files after successful compression (optional)
    """
    try:
        if not files:
            return
        
        logger.info(f"Deleting {len(files)} original files...")
        
        for file_info in files:
            s3_client.delete_object(Bucket=SOURCE_BUCKET, Key=file_info['key'])
            logger.info(f"Deleted: {file_info['key']}")
            
        logger.info("Original files deletion completed")
            
    except Exception as e:
        logger.error(f"Error deleting original files: {str(e)}")
        raise e

def display_configuration():
    """Display current configuration"""
    logger.info("=== S3 Compression Configuration ===")
    logger.info(f"Source Bucket: {SOURCE_BUCKET}")
    logger.info(f"Target Bucket: {TARGET_BUCKET or SOURCE_BUCKET}")
    logger.info(f"Source Prefix: '{SOURCE_PREFIX}'")
    
    if MINUTES_BACK:
        logger.info(f"Minutes Back: {TIME_BACK_MINUTES} minutes")
    elif HOURS_BACK:
        logger.info(f"Hours Back: {TIME_BACK_HOURS} hours ({TIME_BACK_MINUTES} minutes)")
    else:
        logger.info(f"Default Time Back: {TIME_BACK_MINUTES} minutes")
    
    logger.info(f"Delete Original: {DELETE_ORIGINAL}")
    logger.info(f"AWS Region: {AWS_REGION}")
    logger.info(f"Max Files: {MAX_FILES}")
    logger.info(f"Log Level: {LOG_LEVEL}")

def get_cutoff_time():
    """Calculate cutoff time based on configuration"""
    current_time = datetime.now()
    
    if MINUTES_BACK:
        cutoff_time = current_time - timedelta(minutes=TIME_BACK_MINUTES)
        time_description = f"{TIME_BACK_MINUTES} minutes"
    elif HOURS_BACK:
        cutoff_time = current_time - timedelta(hours=TIME_BACK_HOURS)
        time_description = f"{TIME_BACK_HOURS} hours"
    else:
        cutoff_time = current_time - timedelta(hours=24)
        time_description = "24 hours (default)"
    
    return cutoff_time, time_description

def main():
    """
    Main function to execute S3 compression
    """
    try:
        logger.info("Starting S3 File Compression Tool")
        
        # Display configuration
        display_configuration()
        print()  # Empty line for readability
        
        # Validate configuration
        validate_configuration()
        
        # Test S3 connection
        if not test_s3_connection():
            logger.error("S3 connection test failed. Please check your configuration.")
            return
        
        # Calculate cutoff time
        current_time = datetime.now()
        cutoff_time, time_description = get_cutoff_time()
        
        logger.info(f"Current time: {current_time}")
        logger.info(f"Cutoff time ({time_description} ago): {cutoff_time}")
        
        # Get list of files to compress
        files_to_compress = get_files_to_compress(cutoff_time)
        
        if not files_to_compress:
            logger.info("No files found matching the compression criteria")
            return
        
        # Display files to be compressed
        logger.info(f"Found {len(files_to_compress)} files to compress:")
        for i, file_info in enumerate(files_to_compress, 1):
            file_age = current_time - file_info['last_modified']
            age_minutes = int(file_age.total_seconds() / 60)
            logger.info(f"  {i:3d}. {file_info['key']} ({file_info['size']} bytes, {age_minutes} minutes old)")
        
        # Ask for confirmation
        print()  # Empty line
        response = input("Proceed with compression? (y/N): ").strip().lower()
        if response not in ['y', 'yes']:
            logger.info("Compression cancelled by user")
            return
        
        # Create zip archive
        zip_buffer = create_zip_archive(files_to_compress)
        
        # Upload to S3
        target_bucket, target_key = upload_zip_to_s3(zip_buffer)
        
        # Delete original files if enabled
        if DELETE_ORIGINAL:
            delete_original_files(files_to_compress)
        
        # Calculate and display statistics
        original_size = sum(f['size'] for f in files_to_compress)
        compressed_size = len(zip_buffer.getvalue())
        compression_ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0
        
        logger.info("=== Compression Completed Successfully ===")
        logger.info(f"Compressed file: s3://{target_bucket}/{target_key}")
        logger.info(f"Files compressed: {len(files_to_compress)}")
        logger.info(f"Original size: {original_size:,} bytes")
        logger.info(f"Compressed size: {compressed_size:,} bytes")
        logger.info(f"Compression ratio: {compression_ratio:.2f}%")
        logger.info(f"Space saved: {original_size - compressed_size:,} bytes")
        
    except Exception as e:
        logger.error(f"Compression process failed: {str(e)}")
        logger.error("Please check your configuration and try again.")

if __name__ == "__main__":
    # Install required packages if not already installed
    try:
        from dotenv import load_dotenv
    except ImportError:
        print("Error: python-dotenv package is required but not installed.")
        print("Please install it using: pip install python-dotenv")
        exit(1)
    
    main()