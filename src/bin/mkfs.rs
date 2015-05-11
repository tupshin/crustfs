#![feature(libc)]

extern crate libc;
extern crate time;
extern crate fuse;
extern crate cql_ffi;
extern crate crustfs;

use cql_ffi::CassCluster;
use cql_ffi::CassSession;
use cql_ffi::CassStatement;
use crustfs::CrustFS;

fn main() {

  //FIXME contact points should be configurable
    let contact_points = "127.0.0.1";
    let cluster = CassCluster::new()
          .set_contact_points(contact_points).unwrap();

    let session = CassSession::new();
    match session.connect(&cluster).wait() {
    Err(fail) => println!("fail: {}",fail),
    Ok(session) => {
      let crustfs = CrustFS::build(session);
      println!("Session Established. Making fs.");
      assert!(crustfs.execute(crustfs.cmds.create_ks.to_string()).wait().is_ok());
      assert!(crustfs.execute(crustfs.cmds.drop_inode_table.to_string()).wait().is_ok());
      assert!(crustfs.execute(crustfs.cmds.drop_fs_metadata_table.to_string()).wait().is_ok());
      assert!(crustfs.execute(crustfs.cmds.create_inode_table.to_string()).wait().is_ok());
      assert!(crustfs.execute(crustfs.cmds.create_fs_metadata_table.to_string()).wait().is_ok());
      assert!(crustfs.execute(crustfs.cmds.create_null_inode.to_string()).wait().is_ok());

      let insert_root_inode_statement = CassStatement::new(&crustfs.cmds.create_root_inode,4);
      let seconds = time::get_time().sec as i64;
      insert_root_inode_statement.bind_int64(0, seconds).unwrap();
      insert_root_inode_statement.bind_int64(1, seconds).unwrap(); 
      insert_root_inode_statement.bind_int64(2, seconds).unwrap(); 
      insert_root_inode_statement.bind_int64(3, seconds).unwrap();
      assert!(crustfs.execute_statement(insert_root_inode_statement).wait().is_ok());
       
    }
  }
}
