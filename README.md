# Introduction
This is a POC code to evaluate ETCD performance with persistent memory (PMEM).

# Authors (alphabetical order)
- Abhishek Dasgupta (abdasgupta@in.ibm.com)
- Basheer Khadarsabgari (bkhadars@in.ibm.com)
- Pradipta Kumar (bpradipt@in.ibm.com)
- Vaibhav Jain (vajain21@in.ibm.com)

# Build Instructions 
These instructions have been tried on Ubuntu 18.04

## Pre-req
- Ensure `git` and `golang` is installed.
- Ensure `GOPATH` is set

## PMDK
```
sudo apt-get update && sudo apt install -y make pkg-config libndctl-dev libdaxctl-dev autoconf
git clone https://github.com/bpradipt/pmdk.git
cd pmdk
git checkout -b etcd-pmem remotes/origin/etcd-pmem
make -j8 && sudo make install
```
## ETCD
```
git clone https://github.com/IBM/etcd-pmem
cd etcd-pmem
git checkout -b etcd-pmem remotes/origin/etcd-pmem
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
make build-pmem
```
