#! /bin/bash

# This script creates a new database

if [ "$EUID" -ne 0 ]; then
  echo " This script should be run as sudo user, i.e.:"
  echo " sudo bash create_pgDB.sh"
  exit
fi

# apt-get --purge remove --yes postgres*
# rm -rf /var/lib/postgresql
# deluser postgres
# rm -rf /home/postgres/db

# install postgre is not existent
if ! hash psql 2>/dev/null; then
  echo " Installing PostGreSQL (this may take a moment)"
  apt-get install --yes --allow-unauthenticated postgresql-9.5 postgresql-9.5-postgis-2.4-scripts pgadmin3 > /dev/null 2>&1
fi

read -r -p " Please provide the new database's name: " dbname
read -s -p " Please provide the new database's password (db-username is postgres): " pwDb
echo ""
read -r -p " Please provide the path to where the database's data will be written to: " dbLoc

mkdir -p $dbLoc
cd $dbLoc

chown -R postgres:postgres $dbLoc

#cd /home/postgres
echo " Setting new password"
su postgres -c "psql -d template1 -c \"ALTER USER postgres WITH PASSWORD '$pwDb';\""
su postgres -c "psql -d template1 -c \"CREATE TABLESPACE phisar LOCATION '${dbLoc}'\""

echo " Creating $dbname at $dbLoc"
su postgres -c "createdb $dbname -D phisar"

echo " Adding postgis functionalities"
su postgres -c "psql -c \"CREATE EXTENSION postgis; CREATE EXTENSION postgis_topology;\" $dbname"
systemctl restart postgresql

echo " Creating a phiSAR connection file"
mkdir -p $HOME/.phiSAR
echo "$dbname" > ${HOME}/.phiSAR/pgdb
echo "postgres" >> ${HOME}/.phiSAR/pgdb
echo "$pwDb" >> ${HOME}/.phiSAR/pgdb
echo "localhost" >> ${HOME}/.phiSAR/pgdb
echo "5432" >> ${HOME}/.phiSAR/pgdb
chown -R ${SUDO_USER} ${HOME}/.phiSAR
chmod 600 ${HOME}/.phiSAR/pgdb
