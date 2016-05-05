from rediser import redis
import csv, re

a = []

x=redis.keys("eval:gbt:*cluster_id:3*mse:")

for key in x:
    items = key.decode('utf-8').split('_')

    tree_count = items[3]
    depth = int(
    re.findall("\d+", items[5])[0])
    val = redis.get(key.decode('utf-8'))
    a.append([key, tree_count, depth, float(val), 'iters:' + str(tree_count)+':d:'+str(depth)])

with open("file_gbt.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerows(a)
