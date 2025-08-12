#!/bin/bash

# Activate virtual environment
source /datasets/fwang60/_TEXT2VIDEO/Panda-70M/dataset_dataloading/video2dataset/envs/panda-70m/bin/activate

# Set base paths
BASE_DIR="."
INPUT_DIR="${BASE_DIR}/Panda-70M/metafiles/panda70m_training_2m_split"
OUTPUT_BASE="${BASE_DIR}/Output_panda70m_training_2m/Output_panda70m_training_2m_split"

# Process each split CSV file
for i in {0..20}; do
    echo "Processing part ${i}..."
    
    INPUT_FILE="${INPUT_DIR}/panda70m_training_2m_part${i}.csv"
    OUTPUT_DIR="${OUTPUT_BASE}_part${i}"
    
    # # # Download videos
    python3 advanced_downloader_new.py \
        --input "${INPUT_FILE}" \
        --output "${OUTPUT_DIR}" \
        --workers 32 \
        --delay-min 1 \
        --delay-max 2 \
        --quality 360 \
        --proxy "socks5://yangxiu49368627393:t9BdXRJEAD@216.180.245.3:50101"
    
    if [ $? -eq 0 ]; then
        echo "Part ${i} download complete!"
        
        # Cut videos for this part
        python3 cut_videos_new.py \
            --workdir "${OUTPUT_DIR}" \
            --workers 32 \
            --metafile "${INPUT_FILE}" \
            --resultfile "cut_part${i}.jsonl" \
            --log "log_part${i}.log"

        
        if [ $? -eq 0 ]; then
            echo "Part ${i} cutting complete!"
            
            # Delete download folder
            echo "Cleaning up download folder..."
            rm -rf "${OUTPUT_DIR}/download"
            echo "Download folder deleted."
        else
            echo "Part ${i} cutting failed!" >> process_failures.log
        fi
    else
        echo "Part ${i} download failed!" >> process_failures.log
    fi
    
    sleep 30
done

echo "All parts completed!"