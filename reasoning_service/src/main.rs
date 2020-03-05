#![deny(missing_docs)]
//! The purpose of this project is to perform a reasoning service
//! using differential dataflow. This is based on the work of
//! DynamiTE, that we are going to use as a comparison.
 
// TODO: FIX ALL DOCUMENTATION COMMENTS, SO FAR ONLY A SKETCH OF IT

fn main(){
    let c = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Stream-Reasoning-with-Differential-Dataflow\\data\\University0_0.nt", 0, 2);
    println!{"{:?}", c[0]};
}