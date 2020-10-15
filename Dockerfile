FROM alpine
ARG ARCH=amd64

RUN apk add --update python3 py3-setuptools curl &&\
    curl -Lo helm.tar.gz https://get.helm.sh/helm-v3.3.4-linux-$ARCH.tar.gz &&\
    curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/v1.16.0/bin/linux/$ARCH/kubectl &&\
    tar xf helm.tar.gz &&\
    chmod +x linux-$ARCH/helm &&\
    chmod +x kubectl &&\
    mv linux-$ARCH/helm kubectl /usr/local/bin &&\
    rm -rf helm.tar.gz linux-$ARCH/
COPY . /work/
RUN cd /work && python3 setup.py install
