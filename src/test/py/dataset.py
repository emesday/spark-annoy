import random

n = 100
d = 50
means = 3

with open('dataset', 'w') as f:
    f.write('%s\n' % d)
    for _ in range(means):
        mean = list([random.random() * 100 for j in range(d)])
        print(mean)
        for i in range(n):
            f.write(','.join([str(random.gauss(mean[j], 1)) for j in range(d)]))
            f.write('\n')
