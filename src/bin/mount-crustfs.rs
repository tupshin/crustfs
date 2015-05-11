#![feature(libc,convert)]

extern crate libc;
extern crate time;
extern crate fuse;
extern crate cql_ffi;
extern crate crustfs;

use cql_ffi::{CassSession, CassCluster};
use crustfs::CrustFS;
use std::path::Path;

use std::env;

fn main() {
    let mp = env::args().nth(1).unwrap();
    let mountpoint = Path::new(mp.as_str());
    let contact_points = "127.0.0.1";
    let cluster = CassCluster::new()
          .set_contact_points(contact_points).unwrap();
    let session = CassSession::new();
    match session.connect(&cluster).wait() {
    Err(fail) => println!("fail: {}",fail),
    Ok(session) => {
      let crustfs = CrustFS::build(session);
      assert!(crustfs.execute(crustfs.cmds.create_ks.to_string()).wait().is_ok());
      assert!(crustfs.execute(crustfs.cmds.create_inode_table.to_string()).wait().is_ok());
      assert!(crustfs.execute(crustfs.cmds.create_fs_metadata_table.to_string()).wait().is_ok());
      println!("Session Established. Mounting fs.");
      fuse::mount(crustfs, &mountpoint, &[]);
    }
  }
}
