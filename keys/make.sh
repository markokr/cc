#! /bin/sh

# PEM format

# req fields
# C  = Country
# ST = State/Province
# L = Locality
# O = Organization
# OU = Org Unit
# CN = commonName
# ? = emailAddress

rm -f private
ln -s . private
rm -rf testca

mkdir -p testca/private
mkdir -p testca/newcerts
touch testca/index.txt
echo 01 > testca/serial

run() {
  echo '$' "$@"
  "$@" 2>&1 | sed 's/^/  > /' 
}

# key -> csr
run_req() {
  tmp="csr.template"
  args=""
  while test "$1" != '--'; do
    args="$args $1"
    shift
  done
  shift

  (
    echo "[req]"
    echo "prompt=no"
    echo "distinguished_name=req_distinguished_name"
    echo "[req_distinguished_name]"
    for arg; do echo "$arg"; done
  ) > "$tmp"
  run openssl req $args -config "$tmp"
}

run_ca() {
  ser=`cat testca/serial`
  run openssl ca -batch -config ca.ini "$@"
  while test "$1" != '-out'; do
    shift
  done
  if test "$1" = '-out'; then
    cp "testca/newcerts/$ser.pem" "$2"
  fi
}
run openssl genrsa -out user1.key 512

run openssl genrsa -out ca.key 512

run openssl genrsa -out server.key 512

run openssl genrsa -out confdb.key 512

# self-signed cert
run_req -new -x509 -key ca.key -out ca.crt -- C=EE L=Tartu O=Skype OU=TempCA CN="Test CA Server"

# cert reqs
run_req -new -key server.key -out server.csr -- C=EE L=Tartu O=Skype OU=DubColo CN="Host 1"
run_req -new -key confdb.key -out confdb.csr -- C=EE L=Tartu O=Skype OU=Service CN="confdb"
run_req -new -key user1.key  -out user1.csr  -- C=EE L=Tartu O=Skype OU=SiteOps CN="User Name" emailAddress="user1@skype.net"

# accept certs
run_ca -days 1024 -policy pol-user   -in user1.csr  -out user1.crt
run_ca -days 1024 -policy pol-server -in server.csr -out server.crt
run_ca -days 1024 -policy pol-server -in confdb.csr -out confdb.crt

