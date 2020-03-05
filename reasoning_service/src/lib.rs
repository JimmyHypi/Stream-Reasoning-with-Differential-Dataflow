#![deny(missing_docs)]
//! Functions and definitions
mod model;
/// function to load the data, parallelized with respect to the number of workers.
pub fn load_data(filename: &str, index: usize, peers: usize) -> Vec<model::Triple>{
    use std::io::{BufRead, BufReader};
    use std::fs::File;

    let mut returning_data = Vec::new();
    let file = BufReader::new(File::open(filename).expect("Couldn't open file"));
    let mut triples = file.lines();
    
    // We use enumerate to parallelize the loading of the data
    for (count, read_triple) in triples.enumerate() {
        if index == count % peers {
            if let Ok(triple) = read_triple {
                let v: Vec<String> = triple.split(" ")
                      .map(|x| String::from(x))
                      .collect();
                let triple_to_push = 
                    model::Triple{
                        // TODO: THESE CLONES, MEH
                        subject: v[0].clone(),
                        property: v[1].clone(),
                        value: v[2].clone()
                    };
                returning_data.push(triple_to_push);
            } else {
                // TODO
            }
        }
    }

    returning_data
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
