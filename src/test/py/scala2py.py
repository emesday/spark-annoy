import sys

from annoy import AnnoyIndex

from scala_angular_result import result as angular_result
from scala_euclidean_result import result as euclidean_result

f = 10

angular_output = 'src/test/resources/annoy-index-angular-scala'
euclidean_output = 'src/test/resources/annoy-index-euclidean-scala'

angular = AnnoyIndex(f, 'angular')
angular.verbose(True)
angular.load(angular_output)
euclidean = AnnoyIndex(f, 'euclidean')
euclidean.verbose(True)
euclidean.load(euclidean_output)

for j in range(angular.get_n_items()):
    r = angular.get_nns_by_item(j, 10)
    t = angular_result[j]
    if len(set(r).intersection(t)) < 8:
        print(j, r, t)
        sys.exit(1)

for j in range(euclidean.get_n_items()):
    r = euclidean.get_nns_by_item(j, 10)
    t = euclidean_result[j]
    if len(set(r).intersection(t)) < 5:
        print(j, r, t)
        sys.exit(1)

