#!/bin/bash

# 激活虚拟环境
source /datasets/fwang60/_TEXT2VIDEO/Panda-70M/dataset_dataloading/video2dataset/envs/panda-70m/bin/activate

# 设置基础路径
BASE_DIR="/datasets/fwang60/_TEXT2VIDEO/YOUTUBE_DOWNLOADER_Panda-70M"
INPUT_DIR="${BASE_DIR}/Panda-70M/metafiles/panda70m_training_2m_split"
OUTPUT_BASE="${BASE_DIR}/Output_panda70m_training_2m/Output_panda70m_training_2m_split"

# 循环处理每个分割的CSV文件
for i in {1..10}; do
    echo "开始处理第 ${i} 部分..."
    
    # 设置输入输出路径
    INPUT_FILE="${INPUT_DIR}/panda70m_training_2m_part${i}.csv"
    OUTPUT_DIR="${OUTPUT_BASE}_part${i}"
    
    # 执行下载命令
    python3 advanced_downloader.py \
        --input "${INPUT_FILE}" \
        --output "${OUTPUT_DIR}" \
        --workers 32 \
        --delay-min 1 \
        --delay-max 2 \
        --quality 360 \
        --use-ip-pool
    
    # 检查命令执行状态
    if [ $? -eq 0 ]; then
        echo "第 ${i} 部分处理完成！"
    else
        echo "第 ${i} 部分处理失败！"
        # 可选：记录失败的部分
        echo "Part ${i} failed at $(date)" >> download_failures.log
    fi
    
    # 可选：在每个部分之间添加延迟，避免过于频繁的请求
    echo "等待30秒后继续下一部分..."
    sleep 30
done

echo "所有部分处理完成！"
