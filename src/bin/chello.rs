extern crate libc;
extern crate time;
extern crate fuse;
extern crate cassandra;
extern crate crustfs;

use cassandra::Cluster;
use cassandra::Statement;
use crustfs::Commands;
use crustfs::CrustFS;

use std::os;

fn main () {
  let cmds = Commands{
    use_ks:"Use crustfs".to_string(),
    create_ks: "CREATE KEYSPACE IF NOT EXISTS crustfs
      WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };".to_string(),
    create_inode_table: "CREATE TABLE IF NOT EXISTS crustfs.inode
      (part_id int, inode bigint, parent_inode bigint, size text, blocks text, atime text, mtime text,
      ctime text, crtime text, kind text, perm text, nlink text, uid text, gid text,
      rdev text, flags text, PRIMARY KEY (part_id,inode)) WITH CLUSTERING ORDER BY (inode DESC);".to_string(),
    create_fs_metadata_table: "CREATE TABLE IF NOT EXISTS crustfs.fs_metadata
      (key text, value text, PRIMARY KEY (key))".to_string(),
    select_inode: "SELECT * FROM crustfs.inode
      WHERE part_id=1 and inode =1;".to_string(),
    create_inode: "UPDATE crustfs.inode SET parent_inode=?, size=?, blocks=?,
      atime=?, mtime=?, ctime=?, crtime=?, kind=?, perm=?, nlink=?, uid=?, gid=?, rdev=?, flags=?
      where part_id = ? and inode = ? if parent_inode=NULL".to_string(),
    insert_inode_placeholder: "INSERT INTO crustfs.inode(part_id, inode)
      VALUES(?,?) IF NOT EXISTS".to_string(),
    select_max_inode: "SELECT inode FROM crustfs.inode where part_id = ? order by inode desc limit 1".to_string(),
  };

  let mountpoint = Path::new(os::args()[1].as_slice());
  let contact_points = "127.0.0.1".to_string();
  let cluster = Cluster::create(contact_points);

   match cluster.connect() {
    Err(fail) => println!("fail: {}",fail),
    Ok(session) => {
      assert!(session.execute(&mut Statement::build_from_string(cmds.create_ks.clone(),0)).is_ok());
      assert!(session.execute(&mut Statement::build_from_string(cmds.create_inode_table.clone(),0)).is_ok());
      assert!(session.execute(&mut Statement::build_from_string(cmds.create_fs_metadata_table.clone(),0)).is_ok());

      println!("Session Established. Mounting fs.");
      let crustfs = CrustFS{session:session,cmds:cmds};
      fuse::mount(crustfs, &mountpoint, []);
    }
  }
}





