#!/bin/bash -l
#
#SBATCH --job-name=dino
#SBATCH --gpus=1
#SBATCH --nodes=1
#SBATCH --partition=main
#SBATCH --cpus-per-task=32
#SBATCH --exclude=ccvl[14]
module purge
module load conda
conda activate base
source /datasets/fwang60/_TEXT2VIDEO/Panda-70M/dataset_dataloading/video2dataset/envs/panda-70m/bin/activate

python3 advanced_downloader.py \
    --input ./Panda-70M/metafiles/panda70m_testing_with_additional_annotation.csv \
    --output ./Output \
    --workers 32 \
    --delay-min 2 \
    --delay-max 5 \
    --quality 360

# python3 advanced_downloader.py \
#     --input ./Panda-70M/metafiles/panda70m_testing_with_additional_annotation.csv \
#     --output ./Output \
#     --workers 32 \
#     --delay-min 2 \
#     --delay-max 5 \
#     --quality 360
