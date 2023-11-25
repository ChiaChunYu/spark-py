from pyspark import SparkContext, SparkConf

def process_text(doc_id, text):
    words = text.lower().split()
    word_count = {}
    for word in words:
        if word in word_count:
            word_count[word] += 1
        else:
            word_count[word] = 1
    return [(word, (doc_id, count)) for word, count in word_count.items()]

def main():
    conf = SparkConf().setAppName("InvertedIndex")
    sc = SparkContext(conf=conf)

    data = [(1, "This is a sample output."),
            (2, "Another input with some text."),
            (3, "This is another output and input.")]

    # Create an RDD from the input data
    input_rdd = sc.parallelize(data)

    # Process the text to compute the inverted index and frequency counts
    inverted_index_rdd = input_rdd.flatMap(lambda x: process_text(x[0], x[1])) \
                                  .groupByKey() \
                                  .mapValues(list)

    result = inverted_index_rdd.collect()
    for r in result:
        print(r)

    sc.stop()

if __name__ == "__main__":
    main()
