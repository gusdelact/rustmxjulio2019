create external table mexmen(latitudei double,longitudei double,populationi double) stored as csv with header row location 'mexmen.csv';
select avg(populationi) from mexmen;
