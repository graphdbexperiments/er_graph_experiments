# Experiments concerning referential integrity management in E/R graphs 

This folder contains files for the experimental set up enabling users to recreate the experiments. In addition, we have provided the results of these experiments we conducted in our research.

More precisely, in our experiments we measure the time and database hits that are required when updating attributes that are repeated as foreign keys in the TPC-H schema which results in update propagation chains. Here, we analysed the update of $suppkey$ in the chain $SUPPLIER \subset PARTSUPP \subset LINEITEM$, $partkey$ in the chain $PART \subset PARTSUPP \subset LINEITEM$, $custkey$ in $CUSTOMER \subset ORDERS$ and $orderkey$ in $ORDERS \subset LINEITEM$.

