DB_DIR="/mnt/sdb/archive_dbs"
PRIMARY_DIR=${DB_DIR}/primary
TAIL_DIR=${DB_DIR}/tail

PRIMARY_DB_DIR=${PRIMARY_DIR}/db
PRIMARY_SST_DIR=${PRIMARY_DIR}/sst_dir

TAIL_DB_DIR=${TAIL_DIR}/db
TAIL_SST_DIR=${TAIL_DIR}/sst_dir

rm -rf ${PRIMARY_DB_DIR}
rm ${PRIMARY_SST_DIR}/*.sst
echo "remove primary db"

rm -rf ${TAIL_DB_DIR}
rm ${TAIL_SST_DIR}/*.sst
echo "remove tail db"

LOG_DIR="/mnt/sdb/my_rocksdb/rubble/log"
rm ${LOG_DIR}/primary
rm ${LOG_DIR}/tail