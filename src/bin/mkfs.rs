extern crate libc;
extern crate time;
extern crate fuse;
extern crate cassandra;
extern crate crustfs;

use cassandra::Cluster;
use cassandra::Statement;

fn main () {

  struct Commands {
    create_ks:String,
    create_inode_table:String,
    create_fs_metadata_table:String,
} 

let cmds = Commands{
  //FIXME The keyspace create command should be able to take topology parameters
    create_ks: "CREATE KEYSPACE IF NOT EXISTS crustfs
      WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };".to_string(),
   create_inode_table: "CREATE TABLE IF NOT EXISTS crustfs.inode
      (part_id bigint, inode bigint, parent_inode bigint, size bigint, blocks bigint, atime bigint, mtime bigint,
      ctime bigint, crtime bigint, kind text, perm int, nlink int, uid int, gid int,
      rdev int, flags int, PRIMARY KEY (part_id,inode)) WITH CLUSTERING ORDER BY (inode DESC);".to_string(),
    create_fs_metadata_table: "CREATE TABLE IF NOT EXISTS crustfs.fs_metadata
      (key text, value text, PRIMARY KEY (key))".to_string(),
  };

  //FIXME contact points should be configurable
  let contact_points = "127.0.0.1".to_string();
  let cluster = Cluster::create(contact_points);

   match cluster.connect() {
    Err(fail) => println!("fail: {}",fail),
    Ok(session) => {
      println!("Session Established. Making fs.");
      assert!(session.execute(&mut Statement::build_from_string(cmds.create_ks.clone(),0)).is_ok());
      assert!(session.execute(&mut Statement::build_from_string(cmds.create_inode_table.clone(),0)).is_ok());
      assert!(session.execute(&mut Statement::build_from_string(cmds.create_fs_metadata_table.clone(),0)).is_ok());
    }
  }
}
