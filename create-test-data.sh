#!/bin/bash

FOLDER=./test-pcap
NOFOLDERS=100
NOFILES=1000

rm -rf $FOLDER

mkdir -p $FOLDER
cd $FOLDER


create() {
  echo "Create folder $1"
  mkdir -p "${1}"
  cd "${i}"
  for ((j = 0; j < $NOFILES; ++j)); do
    randpkt -t tcp "${j}".pcap
  done
  cd ..
}

for ((i = 0; i < $NOFOLDERS; ++i)); do
  create "${i}" &
done

wait

cd ..
tree $FOLDER
