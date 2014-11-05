extern crate libc;
extern crate time;
extern crate fuse;
extern crate cassandra;
extern crate crustfs;

use cassandra::Cluster;
use cassandra::Statement;
use crustfs::CrustFS;

fn main () {
    
  //FIXME contact points should be configurable
  let contact_points = "127.0.0.1".to_string();
  let cluster = Cluster::create(contact_points);

   match cluster.connect() {
    Err(fail) => println!("fail: {}",fail),
    Ok(session) => {
      let crustfs = CrustFS::build(session);
      println!("Session Established. Making fs.");
      assert!(session.execute(&mut Statement::build_from_string(&crustfs.cmds.create_ks,0)).is_ok());
      assert!(session.execute(&mut Statement::build_from_string(&crustfs.cmds.create_inode_table,0)).is_ok());
      assert!(session.execute(&mut Statement::build_from_string(&crustfs.cmds.create_fs_metadata_table,0)).is_ok());
      assert!(session.execute(&mut Statement::build_from_string(&crustfs.cmds.create_null_inode,0)).is_ok());

      let insert_root_inode_statement = &mut Statement::build_from_string(&crustfs.cmds.create_root_inode,4);
      let seconds = time::get_time().sec as i64;
      insert_root_inode_statement.bind_int64(0, seconds);
      insert_root_inode_statement.bind_int64(1, seconds); 
      insert_root_inode_statement.bind_int64(2, seconds); 
      insert_root_inode_statement.bind_int64(3, seconds);
      assert!(session.execute(insert_root_inode_statement).is_ok());
       
    }
  }
}
