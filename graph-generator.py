import address_reuse.visualize.graph

graph = address_reuse.visualize.graph.TopReuserAreaChartBuilder(
    num_reusers = 10, min_block_height = 0, max_block_height = 200000, 
    csv_dump_filename = None, csv_load_filename = None,
    sqlite_db_filename = 'addres_reuse_local.db',
    #sqlite_db_filename = None,
    top_reusers_over_span = True,
    block_resolution = 5000
)

text_file = open("generated-graph.html", "w")
text_file.write(str(graph))
text_file.close()
