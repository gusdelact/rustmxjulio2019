Probando DataFusion con el comando datafusion-cli, con un archivo CSV de 1,000 lineas.

Construir
cargo +nightly build --release

Ejecutar
target/debug/datafusion-cli -p $PWD < query01.sql  
