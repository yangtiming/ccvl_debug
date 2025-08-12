import csv
import json
import os
from tqdm import tqdm
import logging
import argparse
import re
import subprocess
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed


def parse_args():
    parser = argparse.ArgumentParser(description='youtube video processing')
    parser.add_argument('--workdir', default='./hdvila_100m',type=str, help='Working Directory')
    parser.add_argument('--metafile', default='meta_part0.csv', type=str, help='youtube video meta CSV file')
    parser.add_argument('--resultfile', default='cut_part0.jsonl', type=str, help='processed videos')
    parser.add_argument('--log', default='log_part0.log', type=str, help='log')
    parser.add_argument('--workers', default=4, type=int, help='number of parallel workers')
    args = parser.parse_args()
    return args


def check_dirs(dirs):
    if not os.path.exists(dirs):
        os.makedirs(dirs, exist_ok=True)


class Cutvideos():
    def __init__(self, metafile, workdir, resultfile,logger=None):
        self.workdir = workdir
        self.metafile = metafile
        self.resultfile = resultfile
        self.metas = self.loadmetas()
        self.logger = logger or logging.getLogger(__name__)

    def loadmetas(self):
        metas = []
        with open(self.metafile, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # 转换CSV格式到所需格式
                meta = {
                    'video_id': row['videoID'],
                    'url': row['url'],
                    'clip': []
                }
                
                # 解析timestamp字段
                try:
                    # 处理timestamp字段 - 可能是双引号包裹的字符串
                    timestamp_str = row['timestamp']
                    # 移除可能的外层引号
                    if timestamp_str.startswith('"') and timestamp_str.endswith('"'):
                        timestamp_str = timestamp_str[1:-1]
                    # 替换单引号为双引号以符合JSON格式
                    timestamp_str = timestamp_str.replace("'", '"')
                    timestamps = json.loads(timestamp_str)
                    
                    # 为每个时间段创建一个clip
                    for idx, span in enumerate(timestamps):
                        clip = {
                            'clip_id': f"{row['videoID']}_{idx:03d}",
                            'span': span
                        }
                        meta['clip'].append(clip)
                    
                    metas.append(meta)
                except Exception as e:
                    self.logger.warning(f"Failed to parse timestamp for video {row['videoID']}: {e}")
                    self.logger.warning(f"Timestamp string: {row['timestamp']}")
                    continue
                
        return metas

    def hhmmss(self, timestamp1, timestamp2):
        hh,mm,s = timestamp1.split(':')
        ss,ms = s.split('.')
        timems1 = 3600*1000*int((hh)) +  60*1000*int(mm) + 1000*int(ss) + int(ms)
        hh,mm,s = timestamp2.split(':')
        ss,ms = s.split('.')
        timems2 = 3600*1000*int((hh)) +  60*1000*int(mm) + 1000*int(ss) + int(ms)
        dur = (timems2 - timems1)/1000
        return str(dur)

    def run(self, cmd):
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = proc.communicate()
        if proc.returncode != 0:
            # 返回错误信息而不是标准输出
            return err.decode('utf-8')
        return out.decode('utf-8')

    def extract_single_clip(self, sb, in_filepath, out_filepath):
        cmd = ['ffmpeg', '-ss', sb[0], '-t', self.hhmmss(sb[0], sb[1]),'-accurate_seek', '-i', in_filepath, '-c', 'copy',
            '-avoid_negative_ts', '1', '-reset_timestamps', '1',
            '-y', '-hide_banner', '-loglevel', 'error', '-map', '0', out_filepath]  # 改为 error 级别
        
        # 获取ffmpeg的输出
        output = self.run(cmd)
        
        if not os.path.isfile(out_filepath):
            # 输出更详细的错误信息
            raise Exception(f"{out_filepath}: ffmpeg clip extraction failed. FFmpeg output: {output}")

    def extract_clips(self, meta):
        clips = meta['clip']
        vid = meta['video_id']
        
        # 修改路径：添加 download 子文件夹
        video_path = os.path.join(self.workdir, 'download', vid + '.mp4')
       
        if not os.path.exists(video_path):
            self.logger.warning(f"Video file not found: {video_path}")
            return []
            
        outfolder = os.path.join(self.workdir,'video_clips', vid)
        check_dirs(outfolder)
        result = []
        
        # 取消注释 try-except 并改进
        for c in clips:
            try:
                self.extract_single_clip(c['span'], video_path, os.path.join(outfolder, c['clip_id'] + '.mp4'))
                result.append(c['clip_id'])
            except Exception as e:
                self.logger.error(f"Failed to extract clip {c['clip_id']} from {vid}: {str(e)}")
                # 继续处理下一个片段
                pass

        return result

    def extract_all_clip(self, max_workers=4):
        results = []
        failed_videos = []
        
        # 使用ProcessPoolExecutor进行并行处理
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_video = {executor.submit(self.extract_clips, v): v for v in self.metas}
            
            # 使用tqdm显示进度
            for future in tqdm(as_completed(future_to_video), total=len(self.metas)):
                v = future_to_video[future]
                try:
                    result = future.result()
                    if result:  # 只有成功提取了片段才添加
                        results.extend(result)
                    else:
                        video_id = v.get('video_id', 'Unknown')
                        failed_videos.append(video_id)
                except Exception as e:
                    video_id = v.get('video_id', 'Unknown')
                    self.logger.error(f"Failed to process video {video_id}: {str(e)}")
                    failed_videos.append(video_id)

        self.logger.info(f"Number of clips processed: {len(results)}")
        self.logger.info(f"Number of failed videos: {len(failed_videos)}")
        
        # 保存成功的结果（作为JSON Lines格式）
        with open(os.path.join(self.workdir, 'cut_video_results', self.resultfile), 'w') as f:
            for l in results:
                f.write(json.dumps(l) + '\n')
        
        # 保存失败的视频ID列表
        if failed_videos:
            failed_file = os.path.join(self.workdir, 'cut_video_results', f'failed_{self.resultfile}')
            with open(failed_file, 'w') as f:
                for vid in failed_videos:
                    f.write(json.dumps({'video_id': vid, 'status': 'failed'}) + '\n')
            self.logger.info(f"Failed videos saved to: {failed_file}")
        

if __name__ == '__main__':
    args = parse_args()
    
    # 检查metafile是否是绝对路径或相对路径
    metafile = args.metafile
    logdir = os.path.join(args.workdir,'cut_video_log')

    check_dirs(os.path.join(args.workdir, 'video_clips'))
    check_dirs(os.path.join(args.workdir, 'cut_video_results'))
    check_dirs(logdir)

    logging.basicConfig(level=logging.INFO,
                    filename=os.path.join(logdir, args.log),
                    datefmt='%Y/%m/%d %H:%M:%S',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(module)s - %(message)s')

    logger = logging.getLogger(__name__)
    logger.info(args)

    cvd = Cutvideos(metafile, args.workdir, args.resultfile, logger)
    cvd.extract_all_clip(max_workers=args.workers)