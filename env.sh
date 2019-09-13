function oslo_setup_cluster () {
    pip install docker-compose
    git clone https://github.com/harbur/docker-rabbitmq-cluster
}

function oslo_check () {
    if [ -z "$(which docker)" ]; then
        echo "Docker is required. Please it install first"
        exit 1
    fi
}

function oslo_start_cluster () {
    oslo_check
    if [ ! -d docker-rabbitmq-cluster ]; then
        oslo_setup_cluster
    fi
    previous="$(pwd)"
    cd docker-rabbitmq-cluster
    docker-compose up &
    cd ${previous}
}

function oslo_blackout_start () {
    sudo iptables -I 1 INPUT -p tcp --sport 5672 --dport 5672 --j DROP
}

function oslo_blackout_stop () {
    sudo iptables -D INPUT 1
}

function oslo_monitor_cluster () {
    watch -n 2 "docker exec -it docker-rabbitmq-cluster_rabbit1_1 rabbitmqctl list_queues"
}

function oslo_monitor_network () {
    sudo tcpdump -i any '(port 5672)' -vvnnS
}

function oslo_monitor_connections () {
    ss -4tnp | grep 5672
}

function oslo_monitor_process () {
    watch -n 1 'ps ax | grep python | grep -E "listener|notifier" | grep -v watch'
}

function oslo_listener () {
    python listener --url rabbit://localhost:5672
}

function oslo_notifier () {
    python notifier --payload "test test"
}

function oslo_notifier_infinite () {
    while :
    do
        echo "send at $(date)"
        python notifier --payload "test test" --count 30 --threads 12; sleep 2
    done
}
