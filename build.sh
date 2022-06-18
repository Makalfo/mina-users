
# build image
docker build -t mina-names .

# tag image
docker tag mina-names makalfe/mina-names

# push image
docker push makalfe/mina-names:latest
