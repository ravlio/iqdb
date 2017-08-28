FROM scratch
LABEL maintainer "Maxim Bogranov <muravlion@gmail.com>"

COPY iqdb /bin/iqdb

VOLUME     [ "/iqdb" ]
WORKDIR    /iqdb

ENTRYPOINT [ "/bin/iqdb" ]