#!/usr/bin/env bash

product_name="codis-demo"

case "$1" in

### 清理原来codis遗留数据
cleanup)
    # stop codis-proxy
    ./codis-proxy-admin.sh stop
    if [ $? != 0 ]; then
        echo "stop codis-proxy error, plz check it!!"
    fi

    # stop codis-server redis
    ./codis-server-admin.sh stop
    if [ $? != 0 ]; then
        echo "stop codis-server redis error, plz check it!!"
    fi

    # stop codis-topom
    ./codis-topom-admin.sh stop
    if [ $? != 0 ]; then
        echo "stop codis-topom error, plz check it!!"
    fi

    # stop codis-fe
    ./codis-fe-admin.sh stop
    if [ $? != 0 ]; then
        echo "stop codis-fe error, plz check it!!"
    fi

    # wait all servers stopped
    sleep 10

    rm -r ./default.topom ./dump.rdb
    rm -r ../log
    rm ../bin/*.pid
    echo "cleanup codis cluster done!!"
    ;;

### 创建新的codis集群
buildup)
    # start codis-topom
    ./codis-topom-admin.sh start
    if [ $? != 0 ]; then
        echo "start codis-topom error, plz check it!!"
    fi

    # sleep 15 second, wait codis-topom start
    sleep 15

    # create product in topom
    ../bin/codis-admin -v --codis-topom="127.0.0.1:18080"  --product=$product_name    --create-product
    if [ $? != 0 ]; then
        echo "create product in topom error, plz check it!!"
    fi

    # start codis-server redis
    ./codis-server-admin.sh start
    if [ $? != 0 ]; then
        echo "start codis-server redis error, plz check it!!"
    fi

    # create group for created product
    ../bin/codis-admin -v --codis-topom="127.0.0.1:18080"  --product=$product_name    --create-group    --gid=1
    if [ $? != 0 ]; then
        echo "create group for created product error, plz check it!!"
    fi

    # add redis-server in group
    ../bin/codis-admin -v --codis-topom="127.0.0.1:18080"  --product=$product_name    --group-add      --gid=1 --addr=127.0.0.1:6379
    if [ $? != 0 ]; then
        echo "add redis-server in group error, plz check it!!"
    fi

    # rebalance slots
    ../bin/codis-admin -v --codis-topom="127.0.0.1:18080"  --product=$product_name    --rebalance    --confirm
    if [ $? != 0 ]; then
        echo "rebalance slots error, plz check it!!"
    fi

    # sleep 60 second, wait for slots rebalance
    sleep 60

    # start codis-proxy
    ./codis-proxy-admin.sh start
    if [ $? != 0 ]; then
        echo "start codis-proxy error, plz check it!!"
    fi

    # start codis-fe redis
    ./codis-fe-admin.sh start
    if [ $? != 0 ]; then
        echo "start codis-fe error, plz check it!!"
    fi

    # check cluster is build well
    for i in {1..1500}; do ../bin/redis-cli -h 127.0.0.1 -p 19000 setex test$i 100 $i; echo $i; done;
    if [ $? != 0 ]; then
        echo "buildup codis cluster with problems, plz check it!!"
    fi

    echo "codis cluster build successfully!!"
    ;;

*)
    echo "Usage:  buildup   ## buildup the codis cluster
        cleanup   ## cleanup the codis cluster}"
    ;;

esac
