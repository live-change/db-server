FROM node:latest
USER root

RUN npm -g config set user root

RUN npm install -g encoding-down
RUN npm install -g level-rocksdb
RUN npm install -g leveldown
RUN npm install -g levelup
RUN npm install -g memdown
RUN npm install -g node-lmdb
RUN npm install -g rocksdb
RUN npm install -g subleveldown

RUN npm install -g @live-change/db-server@0.3.5

EXPOSE 9417

RUN mkdir /data

CMD livechangedb --verbose serve --dbRoot /data
