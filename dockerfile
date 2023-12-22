FROM ubuntu:22.04
LABEL org.opencontainers.image.authors="WRL"
ADD ./app /app
ADD ./config.json /app
WORKDIR /app
CMD ./DBMS
