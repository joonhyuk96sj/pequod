if [ $# == 2 ]; then
    ./obj/pqserver --round-robin=1024 --evict-periodic --mem-lo=500 --mem-hi=$1 --$2 --dbname=testdb -H=results/hosts.txt -B=1 -P=twitternew-text -kl=7000 | tee server-$1-$2.log
else
    echo "Invalid argument" 
fi
