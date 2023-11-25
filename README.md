# spark_py
## a.py
Implement the PageRank algorithm with Spark and provide suitable input to test it.
## b.py
Given a file with store sales records in the format:
StoreID ItemID1 #sold1 ItemID2 #sold2 …and a file of item prices in the format:
ItemID1 price1 ItemID2 price2 …
Write a Spark program to compute the total sales of each
store, the total number sold of each item, the average total sales and the grand total sales of all stores.
## c.py
Write a Spark program to compute the inverted index and
frequency counts of keywords on a set of documents. More
specifically, given a set of (DocumentID, text) pairs, output a list of (word, ((doc1, #1), (doc2, #2) …)) pairs.
## d.py
Given a text file of purchase records and a threshold θ, write a Spark program to find all sets of frequent items that are purchased together. Each line of the input is a
transaction of the format
<tid> item1
item2
. . . where <tid> is the transaction ID and itemi are the
purchased items (all represented by integer IDs). A set of items is considered frequent if it appears in at least
θ transactions. Keep in mind that purchase order is irrelevant. {A, B} is the same as {B, A}. If a set appears in a transaction, it is only counted once no matter how
many times it appears in that transaction.
