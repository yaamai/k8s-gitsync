FROM alpine
COPY . /work/
RUN apk add --update python3 curl &&\
    curl -Lo helm.tar.gz https://get.helm.sh/helm-v3.0.0-linux-amd64.tar.gz &&\
    curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/v1.16.0/bin/linux/amd64/kubectl &&\
    tar xf helm.tar.gz &&\
    chmod +x linux-amd64/helm &&\
    chmod +x kubectl &&\
    mv linux-amd64/helm kubectl /usr/local/bin &&\
    rm -rf helm.tar.gz linux-amd64/ &&\
    cd /work && python3 setup.py install
