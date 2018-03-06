###########################
# Host specific settings? #
###########################

if [ -z "$SM_CUSTOMIZATION" ]; then
    read -r -d '' SM_CUSTOMIZATION <<EOF
if ! hash module 2> /dev/null; then
  . /etc/profile.d/modules.sh
fi
module load java/1.8u51
EOF
    export SM_CUSTOMIZATION
fi

export SPARK_ROOT=${SPARK_ROOT:-$HOME/work/spark-2.3.0-bin-hadoop2.7}

##############################
# Create and use environment #
##############################

create_work_environment() {
    workdir=$1

    mkdir -p $workdir/{conf,eventlog,log,tmp,worker}

    virtualenv $workdir/virtualenv

    cat > $workdir/conf/spark-defaults.conf <<EOF
spark.local.dir=$workdir/tmp
spark.eventLog.enabled=true
spark.eventLog.dir=$workdir/eventlog
spark.history.fs.logDirectory=$workdir/eventlog
EOF

    if [[ "$(uname -r)" == 2.6.32* ]]; then
        cat >> $workdir/conf/spark-defaults.conf <<EOF

# This is a 2.6.32 kernel bug…
spark.file.transferTo=false
EOF
    fi

    if [ -n "$SM_CUSTOMIZATION" ]; then
        echo "$SM_CUSTOMIZATION" > $workdir/env.sh
    fi

    cat >> $workdir/env.sh <<EOF
. $workdir/virtualenv/bin/activate

export SPARK_ROOT=$SPARK_ROOT

export SPARK_CONF_DIR=$workdir/conf
export SPARK_LOG_DIR=$workdir/log
export SPARK_WORKER_DIR=$workdir/worker

export PATH=\$SPARK_ROOT/bin:\$PATH
EOF
}

workdir=$(readlink -f ${SM_WORKDIR:-$HOME/scratch/_default})

if [ ! -d "$workdir" ]; then
    create_work_environment $workdir
fi

. $workdir/env.sh
