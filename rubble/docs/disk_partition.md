# Overview

In this document, we introduce how to use partition the disk with `fdisk` and `LVM`. We will partition m510's disk into:

- /dev/mapper/node--1--vg-code: where the code sits
- /dev/mapper/node--1--vg-db: where the primary's data (including both DB and SST files) and secondary's DB and SST symlinks locate.
- /dev/mapper/node--1--vg-sst: where secondary's pre-allocated SST slots are

# Instructions

Firstly, we use `fdisk` to change the type of `/dev/nvme0n1p4` to `Linux LVM`.

```
$ sudo fdisk /dev/nvme0n1

Welcome to fdisk (util-linux 2.31.1).
Changes will remain in memory only, until you decide to write them.
Be careful before using the write command.


Command (m for help): t
Partition number (1-4, default 4): 4
Hex code (type L to list all codes): 8e

Changed type of partition 'Linux' to 'Linux LVM'.

Command (m for help): w
The partition table has been altered.
Syncing disks.
```

Next, we initialize the LVM partition as a Physical Volume.

```
$ sudo pvcreate /dev/nvme0n1p4
  Physical volume "/dev/nvme0n1p4" successfully created.
```

Then create a Volume Group `node-1-vg` in the above Physical Volume (suppose that we are on node-1). Differentiate the group name with machine ID so that devices mounted by NVMeoF will not be confused.

```
$ sudo vgcreate node-1-vg /dev/nvme0n1p4
  Volume group "node-1-vg" successfully created
```

Now we can create Logical Volumes (code, db, and sst) inside the group.

```
$ sudo lvcreate -n code -L 20g node-1-vg
  Logical volume "code" created.
$ sudo lvcreate -n db -L 50g node-1-vg
  Logical volume "db" created.
$ sudo lvcreate -n sst -L 140g node-1-vg
  Logical volume "sst" created.
```

Finally, we make filesystems on these logical volumes and mount them.

```
$ sudo mkfs.ext4 /dev/mapper/node--1--vg-code; sudo mkdir /mnt/code; sudo mount /dev/mapper/node--1--vg-code /mnt/code
$ sudo mkfs.ext4 /dev/mapper/node--1--vg-db; sudo mkdir /mnt/db; sudo mount /dev/mapper/node--1--vg-db /mnt/db
$ sudo mkfs.ext4 /dev/mapper/node--1--vg-sst; sudo mkdir /mnt/sst; sudo mount /dev/mapper/node--1--vg-sst /mnt/sst
```

After creating the SST pool under /mnt/sst, make sure to re-mount `/dev/mapper/node--1--vg-sst` as read-only and no-load, so secondary will not modify the filesystem. But the corresponding primary still mount this partition with both read and write permissions by NVMeoF, because it need to ship SST files to this partition.

```
$ sudo umount /dev/mapper/node--1--vg-sst
$ sudo mount -oro,noload /dev/mapper/node--1--vg-sst /mnt/sst
```
