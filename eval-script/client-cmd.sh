./obj/pqserver --round-robin=1024 --twitternew --verbose --no-binary --initialize --no-populate --no-execute -H=results/hosts.txt -B=1
./obj/pqserver --round-robin=1024 --twitternew --verbose --no-binary --no-initialize --no-execute --popduration=200000000 --nusers=50000 -H=results/hosts.txt -B=1

echo "sleep start"
sleep 15m
echo "sleep done"

./obj/pqserver --round-robin=1024 --twitternew --verbose --no-binary --no-initialize --no-populate --fetch --nusers=50000 --duration=4000000 --pactive=70 --pread=100 --psubscribe=10 --ppost=1 --plogout=5 --outpath=results/test/ -H=results/hosts.txt -B=1
