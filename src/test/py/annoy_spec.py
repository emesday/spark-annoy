from annoy import AnnoyIndex

f = 10
i = AnnoyIndex(f)
i.verbose(True)
i.load("annoy-index-scala")
for j in range(i.get_n_items()):
    print(i.get_nns_by_item(j, 3))

