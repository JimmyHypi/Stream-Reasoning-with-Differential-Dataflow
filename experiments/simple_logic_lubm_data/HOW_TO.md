# How to run the experiment
## Requirement
To run the computation you need to have Rust installed ([link](https://www.rust-lang.org/tools/install)). Installing Rust using `rustup` will also install `cargo`.
## Data used
In this short guide I will show how to use the reasoner built with the reasoning service system I developed as my thesis work.
The [`data_for_example`](https://github.com/JimmyHypi/Stream-Reasoning-with-Differential-Dataflow/tree/master/experiments/simple_logic_lubm_data/data_for_example) folder contains the data I use in this example.
1. The [`tbox`](https://github.com/JimmyHypi/Stream-Reasoning-with-Differential-Dataflow/tree/master/experiments/simple_logic_lubm_data/data_for_example/tbox) folder contains a snippet of the [*univ-bench*](http://swat.cse.lehigh.edu/onto/univ-bench.owl) converted to the NTriples format.
2. The [`abox`](https://github.com/JimmyHypi/Stream-Reasoning-with-Differential-Dataflow/tree/master/experiments/simple_logic_lubm_data/data_for_example/abox) folder contains a snippet of data generated with the [LUBM data generator](https://github.com/rvesse/lubm-uba). 
3. The [`updates`](https://github.com/JimmyHypi/Stream-Reasoning-with-Differential-Dataflow/tree/master/experiments/simple_logic_lubm_data/data_for_example/updates) folder contains 4 updates to test the incremental materialization.
4. The [`out`](https://github.com/JimmyHypi/Stream-Reasoning-with-Differential-Dataflow/tree/master/experiments/simple_logic_lubm_data/data_for_example/out) folder will be used to store the output of the computation.

The RDF graph corresponding to the dataset used (with simplifying URI renaming) in this example are the following: 
#### ABox
![abox](https://user-images.githubusercontent.com/26743415/120102175-5fbfaf00-c10f-11eb-9851-5791c5281bc3.png)
#### TBox
![tbox_new](https://user-images.githubusercontent.com/26743415/120105667-8d145900-c11f-11eb-814e-2f3fd5fbddab.png)


## Run the computation
To run the computation the system needs: 
1. Timely Dataflow parameters (we are only going to use the number of workers)
2. Path to the TBox data.
3. Path to the ABox data. 
4. Path to the output folder.
5. Any possible update and the type of the update

The software suppors environment variable based logging. Set `RUST_LOG` environment variable to `INFO` to print information on the computation. In the current version it only offers minimal logging, I plan to enrich it in the future. 
On Linux run `RUST_LOG=INFO`.

**N.B.** As of writing, the update is computed after the full materialization, meaning that the full materialization will always be executed first and then the update is applied on the result.
#### Full Materialization only
Make sure to use `.` as the base folder. Assuming we want to run the computation with 4 workers (4 threads).

The command to run:

`cargo run --release -- -w 4 ./data_for_example/tbox/tbox.nt ./data_for_example/abox/abox.nt ./data_for_example/out/`

The system will generate encoding and output folder. The former is not relevant as of right now: it saves the encoded dataset and some performance data on the encoding. 
In the future, I plan to save the map corresponding to the encoded dataset so that the user might ask the system to avoid performing the encoding and to use the saved encoding.
The [`out`](https://github.com/JimmyHypi/Stream-Reasoning-with-Differential-Dataflow/tree/master/experiments/simple_logic_lubm_data/data_for_example/out) folder will contain 
the result of the computation and a `stats` folder that contains performance data relative to the three main steps: loading the data into the dataflow, materialization computation, decoding and save-to-file time.
In particular: 
1. The `best_results` folder contains which number of threads provides the best loading, materialization and save-to-file time. 
2. The `figures` folder contains automatically generated graphs that show the load, materialization and save-to-file time per number of workers used.
3. The `peersX` folder contains the data per worker used by the system to provide the previous performance information so it can be ignored.

**N.B.** When running only with one number of workers the information provided in the `stats` folder is not that informative. Run the same command with different values of `-w`
to provide more data points to the `figures` and `best_results` folders.

**N.B.** As of writing, the computation fragments the result into a number of files corresponding to the number of workers (this allows the threads to write concurrently the result of the computation instead of sharing memory).

On Linux concatenating the results back to one file can be done with: 

`cat full_materialization_worker* > full_materialization.nt`

**N.B.** As of writing, the computation overwrites the files when running the computation with the same output folder. This can create an issue if we run the computation first with e.g. `-w 4` and then `-w 3`, as only the first three files would refer to the computation and the fourth would refer to the old computation.
I plan to fix this in the near future, for now, simply delete all the `.nt` files before re-running the computation with a less amount of workers. On Linux: 

`rm full_materialization_worker*`

The output of the computation represents correctly the ρDF materialization of the initial dataset:
#### Full Materialization graph
![full_mat](https://user-images.githubusercontent.com/26743415/120106084-4293dc00-c121-11eb-9407-c1b137d63a67.png)

In this image the new triples produced are in red. We only show the relevant components and not the full graph. 

#### Update 1: Insert ABox triples
This update can be found in [`a_box_addition_test.nt`](https://github.com/JimmyHypi/Stream-Reasoning-with-Differential-Dataflow/tree/master/experiments/simple_logic_lubm_data/data_for_example/updates/a_box_addition_test.nt)
In this update we add the following RDF graph:

![update1](https://user-images.githubusercontent.com/26743415/120107171-9bfe0a00-c125-11eb-84d0-158b6fc472b5.png)


To run the computation: 

`cargo run --release -- -w 4 ./data_for_example/tbox/tbox.nt ./data_for_example/abox/abox.nt ./data_for_example/out/ -u ./data_for_example/updates/a_box_addition_test.nt=insert_abox`

As we can see the pattern  `=insert_abox` is the way to notify the system to add the triples as ABox triples. This is important for correctness and performance.
The incremental materialization will insert in the dataset the RDF triples represented by the following RDF graph:

![update_1_after](https://user-images.githubusercontent.com/26743415/120108710-0e71e880-c12c-11eb-93b9-35057b371b5b.png)

#### Update 2: Remove ABox triples
This update can be found in [`a_box_deletion_test.nt`](https://github.com/JimmyHypi/Stream-Reasoning-with-Differential-Dataflow/tree/master/experiments/simple_logic_lubm_data/data_for_example/updates/a_box_deletion_test.nt)
In this update we remove RDF triples corresponding to the following RDF graph (this is only the part relevant to the update and not the entire result):

![update2(1)](https://user-images.githubusercontent.com/26743415/120109424-09626880-c12f-11eb-9b4b-822ffd3758d9.png)



To run the computation: 

`cargo run --release -- -w 4 ./data_for_example/tbox/tbox.nt ./data_for_example/abox/abox.nt ./data_for_example/out/ -u ./data_for_example/updates/a_box_deletion_test.nt=delete_abox`

This time the pattern  `=delete_abox` is used to notify the system to incrementally remove the triples as ABox triples.
The incremental materialization will remove the RDF triples from the materialization, as we can see all triples deriving from the triple we delete are not present in the resulting dataset:

![update_2_after(2)](https://user-images.githubusercontent.com/26743415/120109680-0ae06080-c130-11eb-8941-3720c634dbb6.png)

The triples removed are the red dashed arrows in the graph. It is interesting to notice that even if the triple `<../FullProfessor7> <../#type> <../FullProfessor>` has been deleted, from the domain of the
`teacherOf` property we can still infer that `FullProfessor7>` is of type `Faculty` and then that he/she is also fo type `Employee` by `subClassOf` property.
#### Update 3: Insert TBox triples
This update can be found in [`t_box_addition_test.nt`](https://github.com/JimmyHypi/Stream-Reasoning-with-Differential-Dataflow/tree/master/experiments/simple_logic_lubm_data/data_for_example/updates/t_box_addition_test.nt).
The corresponding graph:

![update3(2)](https://user-images.githubusercontent.com/26743415/120110651-f900bc80-c133-11eb-8fa0-ef7e4afe74c5.png)



To run the computation: 

`cargo run --release -- -w 4 ./data_for_example/tbox/tbox.nt ./data_for_example/abox/abox.nt ./data_for_example/out/ -u ./data_for_example/updates/t_box_addition_test.nt=insert_tbox`

This time the pattern  `=insert_abox` is used to notify the system to incrementally insert the triples as TBox triples.
The incremental materialization will insert the RDF triples from the materialization and all the deriving triples, as shown in the resulting dataset:

![update_3_after](https://user-images.githubusercontent.com/26743415/120110592-b212c700-c133-11eb-9fa7-c9a792863270.png)

#### Update 4: Remove TBox triples
The last update can be found in [`t_box_deletion_test.nt`](https://github.com/JimmyHypi/Stream-Reasoning-with-Differential-Dataflow/tree/master/experiments/simple_logic_lubm_data/data_for_example/updates/t_box_deletion_test.nt).

The corresponding graph:

![update4](https://user-images.githubusercontent.com/26743415/120110820-cb684300-c134-11eb-9594-fd5fbc0fb53c.png)

To run the computation: 

`cargo run --release -- -w 4 ./data_for_example/tbox/tbox.nt ./data_for_example/abox/abox.nt ./data_for_example/out/ -u ./data_for_example/updates/t_box_deletion_test.nt=delete_tbox`

The final combination is `=delete_tbox`, used to notify the system to incrementally remove the triples as TBox triples.
The incremental materialization will remove the RDF triples from the materialization and all the deriving triples, as shown in the resulting dataset:

![update_4_after](https://user-images.githubusercontent.com/26743415/120111170-82b18980-c136-11eb-96f7-1e3f7b726911.png)

The triples removed are the red dashed arrows in the graph. As we can see, removing both the information of the `Professor` being `subClassOf` `Faculty` and `teacherOf` having as `domain` `Faculty`, there is no information that implies that `FullProfessor7` is of `type` `Faculty`, hence neither the fact that he/she is of `type` `Employee` can be proven from the updated dataset.

These examples try to touch possible situations that can happen when updating a RDF dataset. Of course, we don't provinde full test coverage, but we plan in the future in testing the reasoning system against more sophisticated benchmarks and dataset.

The system outputs in the folder `out/update_stats`, for each update, the related performance evaluation similarly to what the system does for the full materialization.
The purpose of these information data is so that the user can seperately analyze them at a more macroscopic level, thing that I do in the thesis, when comparing the load, materialization and save-to-file time with respect to the size of the input dataset.

For any questions, feel free to post an issue or contact me directly.


