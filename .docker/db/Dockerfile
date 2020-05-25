FROM postgres:12.3

RUN apt-get update && apt-get install -y git make gcc mono-mcs postgresql-server-dev-12
RUN postgres --version
RUN cd /tmp && \
    git clone https://github.com/tvondra/quantile.git && \
    cd quantile && make install