extern crate libc;
extern crate time;
extern crate fuse;
extern crate cassandra;
extern crate crustfs;

use cassandra::CassCluster;
use cassandra::CassStatement;
use crustfs::CrustFS;

fn main () {
    
  //FIXME contact points should be configurable
  let contact_points = "127.0.0.1";
  let mut cluster = CassCluster::new()
          .set_contact_points(contact_points).unwrap();

   match cluster.connect() {
    Err(fail) => println!("fail: {}",fail),
    Ok(session) => {
      let crustfs = CrustFS::build(session);
      println!("Session Established. Making fs.");
      assert!(session.execute(&mut CassStatement::build_from_string(&crustfs.cmds.create_ks,0)).is_ok());
      assert!(session.execute(&mut CassStatement::build_from_string(&crustfs.cmds.drop_inode_table,0)).is_ok());
      assert!(session.execute(&mut CassStatement::build_from_string(&crustfs.cmds.drop_fs_metadata_table,0)).is_ok());
      assert!(session.execute(&mut CassStatement::build_from_string(&crustfs.cmds.create_inode_table,0)).is_ok());
      assert!(session.execute(&mut CassStatement::build_from_string(&crustfs.cmds.create_fs_metadata_table,0)).is_ok());
      assert!(session.execute(&mut CassStatement::build_from_string(&crustfs.cmds.create_null_inode,0)).is_ok());

      let insert_root_inode_statement = &mut CassStatement::build_from_string(&crustfs.cmds.create_root_inode,4);
      let seconds = time::get_time().sec as i64;
      insert_root_inode_statement.bind_int64(0, seconds).unwrap();
      insert_root_inode_statement.bind_int64(1, seconds).unwrap(); 
      insert_root_inode_statement.bind_int64(2, seconds).unwrap(); 
      insert_root_inode_statement.bind_int64(3, seconds).unwrap();
      assert!(session.execute(insert_root_inode_statement).is_ok());
       
    }
  }
}
