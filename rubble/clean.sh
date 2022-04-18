DB_DIR="/mnt/db"
PRIMARY_DIR=${DB_DIR}/primary
TAIL_DIR=${DB_DIR}/tail
SECONDARY_DIR=${DB_DIR}/secondary

PRIMARY_DB_DIR=${PRIMARY_DIR}/db
PRIMARY_SST_DIR=${PRIMARY_DIR}/sst_dir

TAIL_DB_DIR=${TAIL_DIR}/db
TAIL_SST_DIR=${TAIL_DIR}/sst_dir

SECONDARY_DB_DIR=${SECONDARY_DIR}/db
SECONDARY_SST_DIR=${SECONDARY_DIR}/sst_dir

rm -rf ${PRIMARY_DB_DIR}
rm ${PRIMARY_SST_DIR}/*
echo "remove primary db"

rm -rf ${TAIL_DB_DIR}
rm ${TAIL_SST_DIR}/*
echo "remove tail db"

rm -rf ${SECONDARY_DB_DIR}
rm ${SECONDARY_SST_DIR}/*
echo "remove secondary db"

LOG_DIR="/mnt/code/my_rocksdb/rubble/log"
rm ${LOG_DIR}/primary
rm ${LOG_DIR}/tail
rm ${LOG_DIR}/secondary
echo "remove log"
