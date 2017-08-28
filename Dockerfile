FROM scratch
LABEL maintainer "Maxim Bogranov <muravlion@gmail.com>"

COPY release/server /bin/iqdb

VOLUME     [ "/iqdb" ]
WORKDIR    /iqdb

ENTRYPOINT [ "/bin/iqdb" ]