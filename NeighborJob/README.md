WaterGridSense4.0 NeighborJob
=============================

Think flink job uses hexagonal "neighborhood" cells as computed with [uber's h3 library](https://eng.uber.com/h3/) to find out by how much does a single sensor reading deviate from the values received from its surrounding sensors. Can be useful to detect outliers in spatially correlated data. Could also be used to detect clogged street inlets during a heavy rain event that concerns an entire area.

