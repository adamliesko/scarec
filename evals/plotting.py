from rediser import redis
import csv

a = {}

keysmap20 = redis.keys('eval:als:map20*')
keysmap10 = redis.keys('eval:als:map10*')
keystime = redis.keys('eval:als:time*')

for key in keysmap10:
    items = key.decode('utf-8').split(':')
    rank = items[5]
    iters = items[7]
    if a.get(rank, None) is None:
        a[rank] = {}
    if a[rank].get(iters, None) is None:
        a[rank][iters] = {}
    a[rank][iters]['map10'] = float(redis.get(key).decode('utf-8'))

for key in keysmap20:
    items = key.decode('utf-8').split(':')
    rank = items[5]
    iters = items[7]

    a[rank][iters]['map20'] = float(redis.get(key).decode('utf-8'))

for key in keystime:
    items = key.decode('utf-8').split(':')
    rank = items[5]
    iters = items[7]
    a[rank][iters]['time'] = float(redis.get(key).decode('utf-8'))

x = []
print(a)
for rank in a:
    rank = str(rank)
    for iter in a[rank]:
        iter = str(iter)
        x.append([rank, iter, a[rank][iter]['time'], a[rank][iter]['map10'], a[rank][iter]['map20']])

print(x)
with open("file.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerows(x)
