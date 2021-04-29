#---------------------------------------------------------------------------------------
NUM_KV="3000000"
#---------------------------------------------------------------------------------------
ROCKSDB_DIR="/tmp/rocksdb_vanila_test"
SST_DIR="/mnt/sdb/archive_dbs/vanilla/sst_dir"
#---------------------------------------------------------------------------------------
COMMAND="rocksdb_load_test"
#---------------------------------------------------------------------------------------
LOAD_OUT_FILE="log/load_out.txt"
TOP_OUT_FILE="log/cpu_usage.txt"
#---------------------------------------------------------------------------------------
#chain setting
TARGET_ADDR=$1

function remove_or_touch {
    if [ -f $1 ]; then
        rm $1
    fi
    touch $1
}

[ ! -d "${SST_DIR}" ] && mkdir -p ${SST_DIR}

rm -rf $ROCKSDB_DIR
rm ${SST_DIR}/*

echo "remove or touch output file"
remove_or_touch $LOAD_OUT_FILE
remove_or_touch $TOP_OUT_FILE

#trim ssd
fstrim -v /

# Writes data buffered in memory out to disk, then clear memory cache(page cache).
sudo -S sync; echo 1 | sudo tee /proc/sys/vm/drop_caches

{ ./${COMMAND} \
--rocksdb_dir=${ROCKSDB_DIR} \
--sst_dir=${SST_DIR} \
--num_kv=${NUM_KV} \
--thread_num=8 \
--target_addr=${TARGET_ADDR} \
| tee ${LOAD_OUT_FILE}; } &

# | awk '{printf "%6s %-4s %-4s %-s\n",$1,$2,$9,$NF}' \
{ top -u root -b -d 0.2 -o +%CPU -w 512 \
| grep "rocksdb" --line-buffered >> $TOP_OUT_FILE; } &
wait -n

echo -n "CPU Usage : "
awk '{ total += $9 } END { print total/NR }' $TOP_OUT_FILE | tee -a ${LOAD_OUT_FILE}
kill 0
