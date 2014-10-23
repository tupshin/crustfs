extern crate libc;
extern crate time;
extern crate fuse;
extern crate cassandra;
extern crate crustfs;

use cassandra::Cluster;
use cassandra::Statement;
use crustfs::CrustFS;

use std::os;

fn main () {
  let mountpoint = Path::new(os::args()[1].as_slice());
  let contact_points = "127.0.0.1".to_string();
  let cluster = Cluster::create(contact_points);

   match cluster.connect() {
    Err(fail) => println!("fail: {}",fail),
    Ok(session) => {
      let crustfs = CrustFS::build(session);
      assert!(session.execute(&mut Statement::build_from_string(crustfs.cmds.create_ks.clone(),0)).is_ok());
      assert!(session.execute(&mut Statement::build_from_string(crustfs.cmds.create_inode_table.clone(),0)).is_ok());
      assert!(session.execute(&mut Statement::build_from_string(crustfs.cmds.create_fs_metadata_table.clone(),0)).is_ok());

      println!("Session Established. Mounting fs.");
      fuse::mount(crustfs, &mountpoint, []);
    }
  }
}





