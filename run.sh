sudo tc qdisc del dev eth0 root
sudo tc qdisc del dev eth1 root

sudo tc qdisc add dev eth1 handle 1: root htb default 11
sudo tc class add dev eth1 parent 1: classid 1:1 htb rate 100mbps
sudo tc class add dev eth1 parent 1:1 classid 1:11 htb rate 100mbps

sudo tc qdisc add dev eth0 handle 1: root htb default 11
sudo tc class add dev eth0 parent 1: classid 1:1 htb rate 100kbps
sudo tc class add dev eth0 parent 1:1 classid 1:11 htb rate 100kbps
