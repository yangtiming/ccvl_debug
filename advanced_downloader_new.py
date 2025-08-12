#!/usr/bin/env python3
"""

"""
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

import csv
import os
import subprocess
import time
import random
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import argparse
import hashlib
from datetime import datetime

def get_random_user_agent():
    """获取随机User-Agent"""
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0',
    ]
    return random.choice(user_agents)

def get_random_headers():
    """获取随机请求头"""
    return {
        'Accept-Language': random.choice([
            'en-US,en;q=0.9',
            'en-GB,en;q=0.9',
            'en-US,en;q=0.8,zh-CN;q=0.7',
            'en;q=0.9',
        ]),
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

def download_video_robust(video_info):
    """
    超鲁棒的视频下载函数
    """
    video_id, url, output_dir, proxy, delay_min, delay_max, quality, cookies = video_info
    # 创建download子文件夹
    download_dir = os.path.join(output_dir, "download")
    os.makedirs(download_dir, exist_ok=True)
    output_path = os.path.join(download_dir, f"{video_id}.mp4")
    temp_path = os.path.join(download_dir, f"{video_id}.temp.mp4")
    
    # 检查文件是否已存在且有效
    if os.path.exists(output_path):
        file_size = os.path.getsize(output_path)
        if file_size > 10240:  # 大于10KB
            return (True, video_id, url, None)
        else:
            os.remove(output_path)
    
    # 构建yt-dlp基础命令
    cmd_base = [
        'yt-dlp',
        '--no-playlist',
        '--no-warnings',
        '--user-agent', get_random_user_agent(),
        '--referer', 'https://www.youtube.com/',
        '--socket-timeout', '30',
        '--retries', '5',
        '--fragment-retries', '5',
        '--retry-sleep', 'linear=1::2',
        '--concurrent-fragments', '4',
        '--buffer-size', '16K',
        '--http-chunk-size', '10485760',  # 10MB chunks
        '--no-check-certificate',
        '--prefer-insecure',
        '--geo-bypass',
        '--no-call-home',
        '--ignore-errors',
        '--no-abort-on-error',
        '--throttled-rate', '100K',  # 限速避免检测
    ]
    
    # 添加请求头
    headers = get_random_headers()
    for key, value in headers.items():
        cmd_base.extend(['--add-header', f'{key}:{value}'])
    
    # 质量设置
    quality_formats = {
        '360': 'best[height<=360]',
        '480': 'best[height<=480]',
        '720': 'best[height<=720]',
        '1080': 'best[height<=1080]',
        'best': 'best',
    }
    format_str = quality_formats.get(str(quality), 'best[height<=720]')
    cmd_base.extend(['-f', f'{format_str}/best'])
    
    # 添加代理
    if proxy:
        cmd_base.extend(['--proxy', proxy])
    
    # 添加cookies
    if cookies and os.path.exists(cookies):
        cmd_base.extend(['--cookies', cookies])
    
    # 尝试不同的下载策略
    strategies = [
        {
            'name': '标准下载',
            'cmd': cmd_base + ['-o', output_path, url],
            'delay': 0
        }
    ]
    
    last_error = None
    for i, strategy in enumerate(strategies):
        if i > 0:  # 如果不是第一次尝试，等待
            time.sleep(strategy['delay'])
        
        try:
            # 清理临时文件
            for temp_file in [temp_path, f"{output_path}.part", f"{output_path}.ytdl"]:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            
            # 执行下载
            result = subprocess.run(
                strategy['cmd'],
                capture_output=True,
                text=True,
                timeout=600  # 10分钟超时
            )
            
            # 检查下载结果
            if result.returncode == 0 and os.path.exists(output_path):
                file_size = os.path.getsize(output_path)
                if file_size > 10240:  # 大于10KB
                    # 成功，添加随机延迟
                    delay = random.uniform(delay_min, delay_max)
                    time.sleep(delay)
                    return (True, video_id, url, None)
            
            last_error = f"{strategy['name']}失败: {result.stderr[:200]}"
            
            # 检查特定错误
            if any(err in result.stderr.lower() for err in ['429', 'rate limit', 'too many requests']):
                # 速率限制，等待更长时间
                time.sleep(random.uniform(30, 60))
            elif '403' in result.stderr or 'forbidden' in result.stderr.lower():
                # 访问被拒绝，尝试下一个策略
                continue
                
        except subprocess.TimeoutExpired:
            last_error = f"{strategy['name']}超时"
        except Exception as e:
            last_error = f"{strategy['name']}异常: {str(e)}"
    
    # 所有策略都失败
    return (False, video_id, url, last_error)

def parse_args():
    parser = argparse.ArgumentParser(description='超鲁棒YouTube视频下载器 (CSV版本)')
    parser.add_argument('--input', '-i', required=True, help='输入CSV文件路径')
    parser.add_argument('--output', '-o', default='./download_videos', help='输出目录')
    parser.add_argument('--workers', '-w', type=int, default=2, help='并发进程数(建议1-3)')
    parser.add_argument('--proxy', '-p', default=None, help='代理地址')
    parser.add_argument('--delay-min', type=int, default=2, help='最小延迟(秒)')
    parser.add_argument('--delay-max', type=int, default=5, help='最大延迟(秒)')
    parser.add_argument('--retry', '-r', type=int, default=2, help='失败重试次数')
    parser.add_argument('--cookies', '-c', default=None, help='Cookies文件路径')
    parser.add_argument('--quality', '-q', default='720', help='视频质量(360/480/720/1080/best)')
    parser.add_argument('--shuffle', action='store_true', help='随机打乱下载顺序')
    parser.add_argument('--filter-desirable', action='store_true', help='仅下载desirable_filtering为desirable的视频')
    return parser.parse_args()

def main():
    args = parse_args()
    
    # 创建输出目录
    os.makedirs(args.output, exist_ok=True)
    
    # 读取CSV文件
    videos = []
    with open(args.input, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # 如果启用了过滤，只保留desirable的视频
            # if args.filter_desirable:
            #     # desirable_filtering字段可能包含多个值，检查是否包含'desirable'
            #     if 'desirable' not in row.get('desirable_filtering', ''):
            #         continue
            
            videos.append({
                'video_id': row['videoID'],
                'url': row['url']
            })
    
    # 随机打乱顺序（可选）
    if args.shuffle:
        random.shuffle(videos)
    
    print(f"总共需要下载 {len(videos)} 个视频")
    print(f"使用 {args.workers} 个进程并发下载")
    print(f"延迟范围: {args.delay_min}-{args.delay_max} 秒")
    if args.proxy:
        print(f"使用代理: {args.proxy}")
    if args.filter_desirable:
        print("已启用过滤：仅下载desirable_filtering为desirable的视频")
    print()
    
    # 准备下载任务
    tasks = []
    for video in videos:
        video_id = video['video_id']
        url = video['url']
        task = (video_id, url, args.output, args.proxy, args.delay_min, 
                args.delay_max, args.quality, args.cookies)
        tasks.append(task)
    
    # 执行下载
    failed_downloads = []
    success_count = 0
    skip_count = 0
    
    # 记录开始时间
    start_time = time.time()
    
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        future_to_task = {executor.submit(download_video_robust, task): task for task in tasks}
        
        with tqdm(total=len(tasks), desc="下载进度") as pbar:
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                video_id = task[0]
                
                try:
                    success, vid, vurl, error = future.result()
                    if success:
                        if error is None:
                            success_count += 1
                            pbar.set_description(f"成功: {vid[:20]}")
                        else:
                            skip_count += 1
                            pbar.set_description(f"跳过: {vid[:20]}")
                    else:
                        failed_downloads.append({
                            'video_id': vid,
                            'url': vurl,
                            'error': error,
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
                        pbar.set_description(f"失败: {vid[:20]}")
                except Exception as e:
                    failed_downloads.append({
                        'video_id': video_id,
                        'url': task[1],
                        'error': str(e),
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                
                pbar.update(1)
    
    # 保存失败记录为CSV格式
    if failed_downloads:
        # 保存到output根目录
        failed_file = os.path.join(args.output, f'failed_downloads_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        with open(failed_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['video_id', 'url', 'error', 'timestamp']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(failed_downloads)
        print(f"\n失败记录已保存到: {failed_file}")
    
    # 统计信息
    elapsed = time.time() - start_time
    print(f"\n{'='*50}")
    print(f"下载完成！耗时: {elapsed/60:.1f} 分钟")
    print(f"成功: {success_count}")
    print(f"跳过: {skip_count}")
    print(f"失败: {len(failed_downloads)}")
    print(f"总计: {len(videos)}")
    print(f"{'='*50}")

if __name__ == '__main__':
    main()
