#!/usr/bin/python

import os
import subprocess
import sys

suffix = ''
disk_path = ''

def filefrag(sst_dir):
    with open('filefrag-%s.txt' % suffix, 'w') as out:
        for filename in os.listdir(sst_dir):
            f = os.path.join(sst_dir, filename)
            p = subprocess.run(["filefrag", "-b512", "-v", f], stdout=subprocess.PIPE)
            out.write('[file]:' + f + '\n')
            out.write(p.stdout.decode('utf-8') + '\n')


def hdparm(sst_dir):
    with open('hdparm-%s.txt' % suffix, 'w') as out:
        for filename in os.listdir(sst_dir):
            f = os.path.join(sst_dir, filename)
            p = subprocess.run(["hdparm", "--fibmap", f], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            out.write('[file]:' + f + '\n')
            out.write(p.stdout.decode('utf-8') + '\n')


def debugfs(sst_dir):
    with open('debugfs-%s.txt' % suffix, 'w') as out:
        for filename in os.listdir(sst_dir):
            f = os.path.join(sst_dir, filename)
            p = subprocess.run(['ls', '-i', f], stdout=subprocess.PIPE)
            inode_num = int(p.stdout.decode('utf-8').strip().split(' ')[0])
            p = subprocess.run(['debugfs', '-R', 'stat \"<%d>\"' % inode_num, disk_path], 
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            # print(p.stdout.decode('utf-8'))
            out.write('[file]:' + f + '\n')
            out.write(p.stdout.decode('utf-8') + '\n')


def dumpe2fs():
    with open('dumpe2fs-%s.txt' % suffix, 'w') as out:
        p = subprocess.run(["dumpe2fs", disk_path], stdout=subprocess.PIPE)
        out.write(p.stdout.decode('utf-8'))



if __name__ == '__main__':
    sst_dir = '/mnt/data/db/shard-0/sst_dir/'
    if len(sys.argv) == 1:
        print('usage: dump-fs.py <disk_path> <sst_dir> <suffix>')
        exit(-1)
    disk_path = sys.argv[1]
    if len(sys.argv) > 2:
        sst_dir = sys.argv[2]
    if len(sys.argv) > 3:
        suffix = sys.argv[3]
    debugfs(sst_dir)
    filefrag(sst_dir)
    hdparm(sst_dir)
    dumpe2fs()