#!/bin/bash

DEBIAN_FRONTEND=noninteractive sudo apt-get -y install debhelper curl autoconf zlib1g-dev \
  libedit-dev libxml2-dev libxslt1-dev libkrb5-dev libssl-dev libpam0g-dev systemtap-sdt-dev \
  libselinux1-dev build-essential bison apt-utils lsb-release devscripts \
  software-properties-common git shellcheck flex

sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update

sudo apt-get -y install libpq-dev postgresql-13 postgresql-server-dev-13

./configure

export PG_CONFIG=/usr/bin/pg_config

/home/buildfarm/sonar/depends/build-wrapper-linux-x86/build-wrapper-linux-x86-64 --out-dir build_wrapper_output_directory make 
