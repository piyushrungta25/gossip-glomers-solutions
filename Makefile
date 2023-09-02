.ONESHELL:
SHELL := /usr/bin/bash


echo:
	pushd $(CURDIR)/Echo && dotnet publish -c Release && popd && $(CURDIR)/maelstrom test -w echo --bin $(CURDIR)/Echo/bin/Release/net7.0/linux-x64/publish/Echo --node-count 1 --time-limit 10

uid:
	pushd $(CURDIR)/UID && dotnet publish -c Release && popd && $(CURDIR)/maelstrom test -w unique-ids --bin $(CURDIR)/UID/bin/Release/net7.0/linux-x64/publish/UID --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

broadcast:
	pushd $(CURDIR)/Broadcast && dotnet publish -c Release && popd && $(CURDIR)/maelstrom test -w broadcast --bin $(CURDIR)/Broadcast/bin/Release/net7.0/linux-x64/publish/Broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100

gcounter:
	pushd $(CURDIR)/g-counter && dotnet publish -c Release && popd && $(CURDIR)/maelstrom test -w g-counter --bin $(CURDIR)/g-counter/bin/Release/net7.0/linux-x64/publish/g-counter  --node-count 3 --rate 100 --time-limit 20 --nemesis partition

kafka:
	pushd $(CURDIR)/Kafka && dotnet publish -c Release && popd && $(CURDIR)/maelstrom test -w kafka --bin $(CURDIR)/Kafka/bin/Release/net7.0/linux-x64/publish/Kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000  

kvstore:
	pushd $(CURDIR)/kv-store && dotnet publish -c Release && popd && $(CURDIR)/maelstrom test -w txn-rw-register --bin $(CURDIR)/kv-store/bin/Release/net7.0/linux-x64/publish/kv-store --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total –-nemesis partition