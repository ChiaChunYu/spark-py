from pyspark import SparkContext

def parse_line(line):
    # Parse the input line and return the node and its neighbors
    parts = line.split()
    node = parts[0]
    neighbors = parts[1:]
    return node, neighbors

def compute_contributions(node, neighbors, rank):
    # Calculate contributions to each neighbor
    num_neighbors = len(neighbors)
    for neighbor in neighbors:
        yield (neighbor, rank / num_neighbors)

def pagerank_iter(data, ranks, damping_factor):
    # Perform one iteration of the PageRank algorithm
    contribs = data.join(ranks).flatMap(lambda x: compute_contributions(x[0], x[1][0], x[1][1]))
    new_ranks = contribs.reduceByKey(lambda x, y: x + y).mapValues(lambda rank: (1 - damping_factor) + damping_factor * rank)
    return new_ranks

if __name__ == "__main__":
    sc = SparkContext("local", "PageRank")
    lines = sc.textFile("a.txt")

    # Parse the input data to create an initial set of ranks
    nodes = lines.map(parse_line)
    ranks = nodes.map(lambda node: (node[0], 1.0))

    # Specify the number of iterations and damping factor
    num_iterations = 10
    damping_factor = 0.85

    # Run the PageRank algorithm for the specified number of iterations
    for _ in range(num_iterations):
        ranks = pagerank_iter(nodes, ranks, damping_factor)

    final_ranks = ranks.collect()
    for (node, rank) in final_ranks:
        print(f"Node {node} has rank: {rank}")

    sc.stop()