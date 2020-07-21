FROM golang:latest

ENV SQLANY17="/db-runtime"

#install sqlanywhere server
ARG DB_SETUP_DIR="/db-setup"
WORKDIR $DB_SETUP_DIR
ADD sqla17developerlinux.tar.gz $DB_SETUP_DIR
RUN ${DB_SETUP_DIR}/sqlany17/setup -ss -I_accept_the_license_agreement -sqlany-dir "${SQLANY17}" -type CREATE

#add binaries to path
ENV PATH="${SQLANY17}/bin64:${PATH:-}"

#tell ld where libs are (sqlanywhere c libraries, eg, libdbcapi_r.so)
ENV LD_LIBRARY_PATH="${SQLANY17}/lib64"

#tell cgo where libs are
ENV CGO_LDFLAGS="-L ${SQLANY17}/lib64"

#create the /app dir
ARG APP_DIR="/app"
WORKDIR $APP_DIR
