import random
from annoy import AnnoyIndex

n = 100
d = 10
means = 3

angular = AnnoyIndex(d, 'angular')
euclidean = AnnoyIndex(d, 'euclidean')
angular_output = 'src/test/resources/annoy-index-angular-py'
euclidean_output = 'src/test/resources/annoy-index-euclidean-py'
num_items = 0
for _ in range(means):
    mean = list([((random.random() * 2) - 1) * 10 for j in range(d)])
    for i in range(n):
        v = [round(random.gauss(mean[j], 1), 2) for j in range(d)]
        angular.add_item(num_items, v)
        euclidean.add_item(num_items, v)
        num_items += 1

angular.build(10)
angular.save(angular_output)

euclidean.build(10)
euclidean.save(euclidean_output)

angular_result_output = 'src/test/scala/annoy4s/PyAngularResult.scala'
euclidean_result_output = 'src/test/scala/annoy4s/PyEuclideanResult.scala'

with open(angular_result_output, 'w') as f:
    nns = []
    for j in range(angular.get_n_items()):
        nn = angular.get_nns_by_item(j, 10)
        nns.append('Array(' + ','.join(['%d' %x for x in nn]) + ')')
    f.write('''// generated code
package annoy4s

object PyAngularResult {

  val result = Seq(
    ''')
    f.write(',\n    '.join(nns))
    f.write(''')

}

''')

with open(euclidean_result_output, 'w') as f:
    nns = []
    for j in range(euclidean.get_n_items()):
        nn = euclidean.get_nns_by_item(j, 10)
        nns.append('Array(' + ','.join(['%d' %x for x in nn]) + ')')
    f.write('''// generated code
package annoy4s

object PyEuclideanResult {

  val result = Seq(
    ''')
    f.write(',\n    '.join(nns))
    f.write(''')

}

''')

