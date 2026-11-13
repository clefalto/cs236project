FROM postgres:16
ENV POSTGRES_USER user
ENV POSTGRES_PASSWORD password
ENV POSTGRES_PORT 5432
ENV POSTGRES_DB threeoneone

EXPOSE 5432
USER postgres

COPY ./init.sql /docker-entrypoint-initdb.d/01-init.sql

VOLUME /var/lib/postgresql/data