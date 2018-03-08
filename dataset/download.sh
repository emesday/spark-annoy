name='glove-25-angular.hdf5'
wget -c http://vectors.erikbern.com/${name}
python dump.py ${name}
