
This folder contains the Python file to recreate the experiments for the update propagation chain $ORDERS \subset LINEITEM$ on TPC-H for different scaling factors and under different semantics as well as the results.

Underneath are the results shown for different scaling factors for TPC-H:

<ins>Results for TPC-H small (sf=0.01) shown for time regularly scaled and logarithmically scaled:</ins><br>
<p align="center" width="100%">
<img src="./images/update_chain_small_order-poly.png" alt="Validation of update chain starting on ORDERS on TPC-H small" width ="40%"/>
<img src="./images/update_chain_small_order.png" alt="Validation of update chain starting on ORDERS on TPC-H small" width ="40%"/>
</p>
<ins>Results for TPC-H medium (sf=0.1) shown for time regularly scaled and logarithmic scaled:</ins><br>
<p align="center" width="100%">
<img src="./images/update_chain_medium_order-poly.png" alt="Validation of update chain starting on ORDERS on TPC-H medium" width ="40%"/>
<img src="./images/update_chain_medium_order.png" alt="Validation of update chain starting on ORDERS on TPC-H medium" width ="40%"/>
</p>
<ins>Results for TPC-H large (sf=1) shown for time regularly scaled and logarithmic scaled:</ins><br>
<p align="center" width="100%">
<img src="./images/update_chain_large_order-poly.png" alt="Validation of update chain starting on ORDERS on TPC-H large" width ="40%"/>
<img src="./images/update_chain_large_order.png" alt="Validation of update chain starting on ORDERS on TPC-H large" width ="40%"/>
</p>

Underneath are the results shown for TPC-H modeled under different semantics shown for time logarithmically scaled:

<ins>Results for TPC-H under relational semantics:</ins><br>
<p align="center" width="100%">
<img src="./images/update_chain_relational_order.png" alt="Validation of update chain starting on ORDERS on TPC-H under relational semantics" width ="40%"/>
</p>

<ins>Results for TPC-H under mixed semantics:</ins><br>
<p align="center" width="100%">
<img src="./images/update_chain_mixed_order.png" alt="Validation of update chain starting on ORDERS on TPC-H under mixed semantics" width ="40%"/>
</p>

<ins>Results for TPC-H under graph semantics:</ins><br>
<p align="center" width="100%">
<img src="./images/update_chain_graph_order.png" alt="Validation of update chain starting on ORDERS on TPC-H under graph semantics" width ="40%"/>
</p>
