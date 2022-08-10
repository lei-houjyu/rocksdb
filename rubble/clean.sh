if [ $# != 1 ]; then
  echo "Usage: bash clean.sh shard_num"
fi

rm -rf log/* core*
for i in $(seq $1); do
  rm -rf /mnt/db/$i/primary/*
  rm -rf /mnt/db/$i/tail/*
  rm -rf primary-$i.out tail-$i.out
done