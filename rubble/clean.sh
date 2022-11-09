if [ $# != 2 ]; then
  echo "Usage: bash clean.sh shard_num rf"
fi

rm -rf log/* core*
for (( i=0; i<$1; i++ )); do
  for (( j=0; j<$2; j++ )); do
    rm -rf /mnt/data/db/shard-$i/db/* \
           /mnt/data/db/shard-$i/sst_dir/* \
           /mnt/data/db/shard-$i-replica-$j.out
  done
done