import argparse
import address_reuse.visualize.graph

DEFAULT_NUM_TICKS_PER_GRAPH = 20
DEFAULT_GRAPH_FILENAME = 'generated-graph.html'

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--reusers',
                        help=('The # of address reusing entites to include in '
                              'the graph'), type=int)
    parser.add_argument('--min_block_height',
                        help=('Minimum block height to include.'), type=int)
    parser.add_argument('--max_block_height',
                        help=('Maximum block height to include.'), type=int)
    parser.add_argument('--block_resolution',
                        help=('Number of blocks to include per graph tick.'),
                        type=int)
    args = parser.parse_args()

    num_reusers = 10
    min_block_height = 0
    max_block_height = None

    if args.reusers:
        num_reusers = args.reusers

    if args.min_block_height:
        min_block_height = args.min_block_height

    if args.max_block_height:
        max_block_height = args.max_block_height

    block_resolution = 10000
    if isinstance(min_block_height, int) and isinstance(max_block_height, int):
        block_span = max_block_height - min_block_height
        assert block_span > 0
        block_resolution  = int(block_span / DEFAULT_NUM_TICKS_PER_GRAPH)
    if args.block_resolution:
        block_resolution = args.block_resolution

    graph = address_reuse.visualize.graph.TopReuserAreaChartBuilder(
        num_reusers=num_reusers, min_block_height=min_block_height,
        max_block_height=max_block_height, csv_dump_filename=None,
        csv_load_filename=None, sqlite_db_filename='addres_reuse_local.db',
        top_reusers_over_span=True, block_resolution=block_resolution)

    text_file = open(DEFAULT_GRAPH_FILENAME, "w")
    text_file.write(str(graph))
    text_file.close()

if __name__ == "__main__":
    main()
