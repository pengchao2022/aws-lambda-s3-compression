import boto3
import zipfile
import io
import os
from datetime import datetime, timedelta
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    AWS Lambda handler function for S3 file compression
    """
    try:
        # Configuration from Lambda environment variables
        source_bucket = os.environ.get('SOURCE_BUCKET')
        target_bucket = os.environ.get('TARGET_BUCKET', source_bucket)
        source_prefix = os.environ.get('SOURCE_PREFIX', '')
        
        # Validate required parameters
        if not source_bucket:
            raise ValueError("SOURCE_BUCKET environment variable is required")
        
        # Time configuration - support both minutes and hours
        minutes_back = os.environ.get('MINUTES_BACK')
        hours_back = os.environ.get('HOURS_BACK', '24')
        
        if minutes_back:
            time_back_minutes = int(minutes_back)
            time_description = f"{time_back_minutes} minutes"
        else:
            time_back_minutes = int(hours_back) * 60
            time_description = f"{hours_back} hours"
        
        delete_original = os.environ.get('DELETE_ORIGINAL', 'false').lower() == 'true'
        max_files = int(os.environ.get('MAX_FILES', '1000'))
        
        # Calculate cutoff time
        current_time = datetime.now()
        if minutes_back:
            cutoff_time = current_time - timedelta(minutes=time_back_minutes)
        else:
            cutoff_time = current_time - timedelta(hours=int(hours_back))
        
        # Generate compressed filename with timestamp
        timestamp = current_time.strftime('%Y%m%d_%H%M%S')
        zip_filename = f"compressed_files_{timestamp}.zip"
        target_key = f"compressed/{current_time.strftime('%Y/%m/%d')}/{zip_filename}"
        
        logger.info("=== Starting S3 Compression in Lambda ===")
        logger.info(f"Source Bucket: {source_bucket}")
        logger.info(f"Target Bucket: {target_bucket}")
        logger.info(f"Source Prefix: {source_prefix}")
        logger.info(f"Time Back: {time_description}")
        logger.info(f"Cutoff Time: {cutoff_time}")
        
        # Get list of files to compress
        files_to_compress = get_files_to_compress(source_bucket, source_prefix, cutoff_time, max_files)
        
        if not files_to_compress:
            logger.info("No files found matching the compression criteria")
            return {
                'statusCode': 200,
                'body': 'No files found for compression'
            }
        
        logger.info(f"Found {len(files_to_compress)} files to compress")
        
        # Create zip file in memory
        zip_buffer = create_zip_archive(source_bucket, files_to_compress)
        zip_size = len(zip_buffer.getvalue())
        logger.info(f"Zip archive created: {zip_size} bytes")
        
        # Upload zip file to S3
        upload_to_s3(target_bucket, target_key, zip_buffer)
        
        # Delete original files if enabled
        if delete_original:
            delete_original_files(source_bucket, files_to_compress)
            logger.info("Original files deleted successfully")
        
        # Calculate statistics
        original_size = sum(f['size'] for f in files_to_compress)
        compression_ratio = (1 - zip_size / original_size) * 100 if original_size > 0 else 0
        
        logger.info("=== Compression Completed Successfully ===")
        logger.info(f"Files compressed: {len(files_to_compress)}")
        logger.info(f"Original size: {original_size} bytes")
        logger.info(f"Compressed size: {zip_size} bytes")
        logger.info(f"Compression ratio: {compression_ratio:.2f}%")
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'File compression completed successfully',
                'compressed_file': f"s3://{target_bucket}/{target_key}",
                'files_compressed': len(files_to_compress),
                'original_size': original_size,
                'compressed_size': zip_size,
                'compression_ratio': f"{compression_ratio:.2f}%",
                'timestamp': timestamp
            }
        }
        
    except Exception as e:
        logger.error(f"Compression process failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Compression failed: {str(e)}'
        }

def get_files_to_compress(bucket_name, prefix, cutoff_time, max_files=1000):
    """
    Retrieve list of files from S3 that match the compression criteria
    """
    files = []
    
    try:
        logger.info(f"Listing objects in bucket: {bucket_name}, prefix: {prefix}")
        
        paginator = s3_client.get_paginator('list_objects_v2')
        operation_parameters = {
            'Bucket': bucket_name,
            'MaxKeys': 1000
        }
        
        if prefix:
            operation_parameters['Prefix'] = prefix
        
        total_files_scanned = 0
        for page in paginator.paginate(**operation_parameters):
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
                        len(files) >= max_files):
                        continue
                    
                    files.append({
                        'key': obj['Key'],
                        'size': file_size,
                        'last_modified': last_modified,
                        'etag': obj['ETag']
                    })
                    
                    # Stop if we've reached the maximum
                    if len(files) >= max_files:
                        logger.info(f"Reached maximum file limit: {max_files}")
                        break
            
            if len(files) >= max_files:
                break
        
        logger.info(f"Scanning completed: {total_files_scanned} files scanned, {len(files)} files eligible for compression")
        
        # Sort files by modification time (oldest first)
        files.sort(key=lambda x: x['last_modified'])
        
    except Exception as e:
        logger.error(f"Error listing files from S3: {str(e)}")
        raise e
    
    return files

def create_zip_archive(bucket_name, files):
    """
    Create a ZIP archive containing the specified files
    """
    zip_buffer = io.BytesIO()
    
    try:
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            successful_files = 0
            
            for file_info in files:
                try:
                    # Download file from S3
                    response = s3_client.get_object(Bucket=bucket_name, Key=file_info['key'])
                    file_content = response['Body'].read()
                    
                    # Create filename for zip
                    zip_filename = os.path.basename(file_info['key'])
                    if not zip_filename:
                        zip_filename = file_info['key'].replace('/', '_').strip('_')
                    
                    # Add file to zip archive
                    zip_file.writestr(zip_filename, file_content)
                    successful_files += 1
                    
                    logger.info(f"Added to zip: {file_info['key']}")
                    
                except Exception as e:
                    logger.error(f"Failed to process file {file_info['key']}: {str(e)}")
                    continue
        
        zip_buffer.seek(0)
        logger.info(f"Zip archive created: {successful_files}/{len(files)} files processed")
        
        return zip_buffer
        
    except Exception as e:
        logger.error(f"Error creating zip archive: {str(e)}")
        raise e

def upload_to_s3(bucket_name, key, zip_buffer):
    """
    Upload the created ZIP file to S3
    """
    try:
        logger.info(f"Uploading zip file to: s3://{bucket_name}/{key}")
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=zip_buffer.getvalue(),
            ContentType='application/zip',
            ServerSideEncryption='AES256'
        )
        
        logger.info("Zip file uploaded successfully")
        
    except Exception as e:
        logger.error(f"Failed to upload zip file to S3: {str(e)}")
        raise e

def delete_original_files(bucket_name, files):
    """
    Delete original files after successful compression (optional)
    """
    try:
        if not files:
            return
        
        logger.info(f"Deleting {len(files)} original files")
        
        for file_info in files:
            s3_client.delete_object(Bucket=bucket_name, Key=file_info['key'])
            logger.info(f"Deleted: {file_info['key']}")
            
        logger.info("Original files deletion completed")
            
    except Exception as e:
        logger.error(f"Error deleting original files: {str(e)}")
        raise e