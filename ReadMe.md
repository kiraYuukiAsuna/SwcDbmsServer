# Deploy

## Steps

- Run chmod +x build.sh and chmod +x deploy.sh

- Run ./build.sh to build server binary file

- Run ./deploy.sh to deploy server using docker-compose

## Configuration

Modify the compose.yml to fulfill your need:

- By default using nginx.conf located in current directory
- By default using current directory to store mongodb volume data
