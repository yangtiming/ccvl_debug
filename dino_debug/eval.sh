export CUDA_VISIBLE_DEVICES=0,1,2,3

torchrun --nproc_per_node=4 eval_linear.py --arch mambar_small_patch16_224 --pretrained_weights /cis/home/gwei10/dino/output/checkpoint0078.pth --data_path /cis/home/gwei10/data/ImageNet