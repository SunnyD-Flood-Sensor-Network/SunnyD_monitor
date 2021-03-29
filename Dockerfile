FROM rocker/r-ver:latest

# system libraries of general use
## install debian packages
RUN apt-get update -qq && apt-get -y --no-install-recommends install \
    libxml2-dev \
    libpq-dev \
    libssh2-1-dev \
    unixodbc-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    odbc-postgresql 

## update system libraries
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get clean

# RUN cron start

# install renv & restore packages
RUN install2.r lubridate dplyr DBI RPostgres dbx pool dbplyr foreach readr

RUN groupadd -r monitor && useradd --no-log-init -r -g monitor monitor

EXPOSE 5432

# copy folder which contains cronfile, RScript and Big Query auth JSON
ADD monitor.R /home/monitor.R

# ADD cron_start.R /cron_start.R
# ADD postgres_keys.R /postgres_keys.R
ADD atm_pressure.csv /home/atm_pressure.csv


WORKDIR /home
USER monitor

CMD Rscript monitor.R