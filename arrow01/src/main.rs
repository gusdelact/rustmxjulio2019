extern crate datafusion;
extern crate arrow;
extern crate alloc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::context::ExecutionContext;
use datafusion::datasource::csv::CsvFile;
use arrow::array::BinaryArray;
use alloc::rc::Rc;

fn main() {
let field_a = Field::new("a", DataType::Utf8, false);
let field_b = Field::new("b", DataType::Utf8, false);

let schema = Schema::new(vec![field_a, field_b]);

// create local execution context
    let mut ctx = ExecutionContext::new();
    let testdata =
      ::std::env::var("ARROW_TEST_DATA")
                  .expect("ARROW_TEST_DATA not defined");

    let csvfile = CsvFile::new(  
                    &format!("{}/csv/csv01.csv", testdata),
                    &schema,
                    true );
    ctx.register_table("csv01", Rc::new(csvfile) );
    let sql = "SELECT a,b FROM csv01";
    let plan=ctx.create_logical_plan(sql).unwrap();
    println!("{:?}",plan);   
    let relation=ctx.execute(&plan,1024).unwrap();
    let mut results = relation.borrow_mut();

    while let Some(batch) = results.next().unwrap() {
         println!(
            "RecordBatch has {} rows and {} columns",
            batch.num_rows(),
            batch.num_columns()
        ); 
        let a = batch
                .column(0) 
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap(); 
        let b = batch
                .column(1) 
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
        for i in 0..batch.num_rows() {
           let a_value: String = 
             String::from_utf8(a.value(i).to_vec()).unwrap();   
           let b_value: String = 
             String::from_utf8(b.value(i).to_vec()).unwrap();   
             println!("{} , {} ",a_value,b_value);
        }        
    }
}
