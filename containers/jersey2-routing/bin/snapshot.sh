#!/usr/bin/env bash

arg=$1

case $arg in
  snapshot)
    echo "hello"
  ;;
esac

# features: https://www.percona.com/doc/percona-xtrabackup/2.1/intro.html
# https://www.percona.com/doc/percona-xtrabackup/2.1/how-tos.html#recipes-ibk
# incremental:  https://www.percona.com/doc/percona-xtrabackup/2.1/howtos/recipes_ibkx_inc.html
# https://www.percona.com/doc/percona-xtrabackup/2.1/innobackupex/restoring_a_backup_ibk.html
# https://www.percona.com/doc/percona-xtrabackup/2.3/innobackupex/partial_backups_innobackupex.html
# https://www.percona.com/doc/percona-xtrabackup/2.3/innobackupex/restoring_individual_tables_ibk.html
#/usr/local/xtrabackup/bin/innobackupex  --user=root --database "..." ~/backup

/usr/local/xtrabackup/bin/innobackupex  --user=root --no-timestamp ~/backup/gondola
/usr/local/xtrabackup/bin/innobackupex  --apply-log  ~/backup/gondola
mysql.server stop
mv /usr/local/var/mysql /usr/local/var/mysql.`date "+%Y-%m-%d_%H:%M:%S"`
/usr/local/xtrabackup/bin/innobackupex --copy-back backup/gondola
mysql.server start
