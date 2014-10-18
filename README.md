very rough prototype of getting data out of cassandra and into a fuse fs. all functions currently stubbed out. 


CREATE KEYSPACE crustfs WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

USE crustfs;

CREATE TABLE inode (
  inode int,
  atime int,
  blocks int,
  crtime int,
  ctime int,
  flags int,
  gid int,
  kind text,
  mtime int,
  nlink int,
  perm text,
  rdev int,
  size int,
  uid int,
  PRIMARY KEY ((inode))
)

insert into inode (inode,atime,blocks,crtime,ctime,flags,gid,kind) values(1,1,1,1,1,1,1,'hello cassandra')

compile

run ./target/chello foo (where foo is a directory)
cat foo/hello.txt
