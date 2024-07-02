export CUDA_VISIBLE_DEVICES=0,1,2,3

torchrun --nproc_per_node=4 main_dino.py --arch mambar_small_patch16_224 --data_path /cis/home/gwei10/data/ImageNet/train --output_dir ./output/