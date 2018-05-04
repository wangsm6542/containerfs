#!/bin/bash
echo "------------- Start to build ContainerFS -------------"

if [ ! -d "./build" ]; then
  mkdir ./build
else
  rm -rf ./build/*
fi

for dir in ./proto/mp ./proto/dp ./proto/kvp ./proto/vp
do
  pushd $dir
  make
  popd
done

cd ./cmd
for dir in `ls` 
do
  pushd $dir
  go build -ldflags "-X github.com/tiglabs/containerfs/utils.Release=`git tag|head -n 1` -X github.com/tiglabs/containerfs/utils.Build=`git rev-parse HEAD`" -o cfs-$dir
  cp cfs-$dir  ../../build
  rm -rf cfs-$dir
  popd
done

echo "------------- build end -------------"
