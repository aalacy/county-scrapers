#!/bin/bash
echo('stop TOR container')
docker stop tor45_pinellas tor45_duval tor35_broward tor150_miami
echo('delete TOR container')
docker rm tor45_pinellas
docker rm tor45_duval
docker rm tor35_broward
docker rm tor150_miami
echo('create TOR container')
docker run --name tor45_pinellas -d -p 8121:8118 -p 2093:2090 -e tors=45 -e privoxy=1 zeta0/alpine-tor
docker run --name tor45_duval -d -p 8120:8118 -p 2092:2090 -e tors=45 -e privoxy=1 zeta0/alpine-tor
docker run --name tor35_broward -d -p 8119:8118 -p 2091:2090 -e tors=35 -e privoxy=1 zeta0/alpine-tor
docker run --name tor150_miami -d -p 8118:8118 -p 2090:2090 -e tors=150 -e privoxy=1 zeta0/alpine-tor
