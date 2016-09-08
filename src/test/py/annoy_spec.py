from annoy import AnnoyIndex

f = 3
i = AnnoyIndex(f)
i.verbose(True)
i.load("annoy-index-scala")
print(i.get_nns_by_item(0, 10))
# i.add_item(0, [2, 1, 0])
# i.add_item(1, [1, 2, 0])
# i.add_item(2, [0, 0, 1])
# i.build(10)
# i.save("annoy-index-py")
