very rough prototype of getting data out of cassandra and into a fuse fs. all functions currently stubbed out.
Builds are done using Cargo

Build using cargo:

`cargo build`

Create the keyspace and tables:

`target/mkcrustfs` will create proper tables in Cassandra assuming it's running without authentication. 

Create a directory that will serve as the mount point.

`mkdir blah`

Mount crustfs:

`target/mount-crustfs blah`

Touch a file

`touch blah/hello.txt`

Check cqlsh to see if your file exists

