#![crate_name = "crustfs"]
#![feature(plugin,libc,path_ext,convert)]


#[macro_use]
extern crate log;

extern crate libc;
extern crate time;
extern crate fuse;
extern crate cql_ffi;
extern crate rand;

use cql_ffi::result_future::ResultFuture;

use fuse::{FileType, FileAttr, Filesystem, Request, ReplyData, ReplyEntry,
           ReplyAttr, ReplyDirectory, ReplyEmpty, ReplyOpen, ReplyCreate,
           ReplyStatfs, ReplyWrite, ReplyLock, ReplyBmap};


use std::string::ToString;
use std::path::Path;

use rand::Rng;

//use std::fs::{FilePermission, USER_FILE, USER_DIR};
use std::fs::PathExt;
use std::ffi::OsStr;

use libc::consts::os::posix88::EIO;
use libc::c_int;
use libc::ENOENT;
use libc::ENOSYS;

use time::Timespec;

use cql_ffi::CassStatement;
use cql_ffi::CassSession;
use cql_ffi::CassValueType;
use cql_ffi::CassResult;

static INODE_PARTITIONS:u64=5;


static HELLO_DIR_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: Timespec { sec: 1381237736, nsec: 0 },
    mtime: Timespec { sec: 1381237736, nsec: 0 },
    ctime: Timespec { sec: 1381237736, nsec: 0 },
    crtime: Timespec { sec: 1381237736, nsec: 0 },
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
};

static HELLO_TXT_ATTR: FileAttr = FileAttr {
    ino: 2,
    size: 15,
    blocks: 1,
    atime: Timespec { sec: 1381237736, nsec: 0 },
    mtime: Timespec { sec: 1381237736, nsec: 0 },
    ctime: Timespec { sec: 1381237736, nsec: 0 },
    crtime: Timespec { sec: 1381237736, nsec: 0 },
    kind: FileType::RegularFile,
    perm: 0o755,
    nlink: 1,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
};


static TTL: Timespec = Timespec { sec: 1, nsec: 0 };    // 1 second

pub struct Commands {
    pub use_ks:&'static str,
    pub select_inode:&'static str,
    pub create_ks:&'static str,
    pub drop_inode_table:&'static str,
    pub drop_fs_metadata_table:&'static str,
    pub create_inode_table:&'static str,
    pub create_fs_metadata_table:&'static str,
    pub create_inode:&'static str,
    pub select_max_inode:&'static str,
    pub insert_default_inode:&'static str,
    pub add_inode_to_parent:&'static str,
    pub create_root_inode:&'static str,
    pub create_null_inode:&'static str,
    pub select_child_inodes:&'static str,
}

pub struct CrustFS {
    pub session:CassSession,
    pub cmds:Commands,
}

impl CrustFS {
    pub fn build(session: CassSession) -> CrustFS {
        let cmds = Commands{
            use_ks:"Use crustfs",
            create_ks: "CREATE KEYSPACE IF NOT EXISTS crustfs
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };",
            drop_inode_table: "DROP TABLE IF EXISTS crustfs.inode",
            drop_fs_metadata_table: "DROP TABLE IF EXISTS crustfs.fs_metadata",
            create_inode_table: "CREATE TABLE IF NOT EXISTS crustfs.inode
            (part_id bigint, inode bigint, parent_inode bigint, size bigint, blocks bigint,
            atime bigint, mtime bigint,ctime bigint, crtime bigint, kind text, perm int,
            nlink int, uid int, gid int, rdev int, flags int, dir_contents map<text,bigint>,
            PRIMARY KEY (part_id,inode))
            WITH CLUSTERING ORDER BY (inode DESC);",
            create_fs_metadata_table: "CREATE TABLE IF NOT EXISTS crustfs.fs_metadata
            (key text, value text, PRIMARY KEY (key))",
            select_inode: "SELECT part_id,inode,dir_contents,parent_inode,size,blocks,atime,
            mtime,ctime,crtime,kind,perm,nlink,uid,gid,rdev,flags FROM crustfs.inode
            WHERE part_id=? and inode =?;",
            create_inode: "UPDATE crustfs.inode SET parent_inode=?, size=?, blocks=?,
            atime=?, mtime=?, ctime=?, crtime=?, kind=?, perm=?, nlink=?, uid=?, gid=?, rdev=?, flags=?
            where part_id = ? and inode = ? if parent_inode=NULL",
            add_inode_to_parent: "UPDATE crustfs.inode SET dir_contents[?] = ? WHERE part_id=? and inode=?
            IF kind='dir'",
            insert_default_inode: "INSERT INTO crustfs.inode(part_id, inode, dir_contents)
            VALUES(?,?,{}) IF NOT EXISTS",
            select_max_inode: "SELECT inode FROM crustfs.inode where part_id = ? order by inode desc
            limit 1",
            create_root_inode: "INSERT INTO crustfs.inode (part_id, inode, size, blocks, atime,mtime,
            ctime,crtime,kind,perm,nlink,uid,gid,rdev,flags)
            VALUES(1,1,4096,1,?,?,?,?,'dir',0,0,0,0,0,0)",
            create_null_inode: "INSERT INTO crustfs.inode (part_id, inode, size, blocks, atime,mtime,
            ctime,crtime,kind,perm,nlink,uid,gid,rdev,flags)
            VALUES(0,0,0,0,0,0,0,0,'null',0,0,0,0,0,0)",
            select_child_inodes: "SELECT dir_contents FROM crustfs.inode where part_id=? and inode=?",
        };
        CrustFS{session:session,cmds:cmds}
    }

  pub fn execute(&self, statement: String) -> ResultFuture {
        self.session.execute(statement.as_str(),0)
    }

  pub fn execute_statement(&self, statement: CassStatement) -> ResultFuture {
        self.session.execute_statement(&statement)
    }


//This is the number of partitions the inodes will be sharded into.
//In production, this should be quite high. If you want strictly linear
//growth in inode generation for testing, set it to 1, but that will
//cause a hot spot in the cluster.


    /*This function chooses a random inode partition,
    generates the next valid inode for that partition
    and inserts a stub (just the partition_id and inode)
    reserving the inode for the calling function
    */
    fn allocate_inode(&mut self) -> (u64, u64) {
        debug!("allocate_inode");
        //choose a random partition
        let partition:u64 = rand::thread_rng().gen_range(0u64,INODE_PARTITIONS);

        //select the maximum inode value in that partition.
        let select_max_inode_statement = CassStatement::new(self.cmds.select_max_inode, 1);
        debug!("allocate_inode: binding partition: {}",partition);
        select_max_inode_statement.bind_int64(0, partition as i64).unwrap();

        //return the inode that is generated out of the
        let mut future = self.session.execute_statement(&select_max_inode_statement);
        //future.wait();
        //FIXME match not needed or safe api should change. choose.
        match future.wait() {
            Err(err) => panic!("{:?}",err),
            Ok(select_result) => {
                //generate  a new inode by taking max found in #2 + INODE_PARTITIONS
                //which is our offset for inodes within each partition.
                let next_inode = match select_result.first_row() {
                    None => {panic!("no first row")},
                    Some(row) => {
                        match row.get_column(0).unwrap().get_int64() {
                            Ok(res) => {
                                debug!("allocate_inode: got row {} in partition {}. adding new row {}",
                                res,partition,res as u64+INODE_PARTITIONS);
                                res as u64 + INODE_PARTITIONS
                            },
                            Err(e) => {panic!("corrupt fs: {:?}",e)}
                        }
                    }
                };
                //insert into inode if not exists on the new inode.
                let insert_inode_placeholder_stmt = CassStatement::new(self.cmds.insert_default_inode,2);

                //FIXME make these chainable
                insert_inode_placeholder_stmt.bind_int64(0, partition as i64).unwrap();
                insert_inode_placeholder_stmt.bind_int64(1, next_inode as i64).unwrap();
                let mut future = self.session.execute_statement(&insert_inode_placeholder_stmt);
                match future.wait() {
                    Ok(_) => {
                        //FIXME. make sure I don't need to pay more attention to a succsesful result
                        //the insert succeeded, so we can consider the generated inode to be valid
                        (partition,next_inode)
                    }
                    Err(err) => {
                        debug!("allocate_inode: insert race condition encountered: {:?}", err);
                        self.allocate_inode() //can retry on failed inode allocation be tail recursive?
                    }
                }
            },
        }
    }
}

fn inode_exists(result:CassResult, name: &Path) -> bool {
    match result.first_row() {
        None=> true,
        Some(row) => {
            debug!("lookup: got row");
            let mut exists=false;
            match row.get_column(2).unwrap().set_iter() {
                Err(err) => {panic!("bad: {:?}", err)}
                Ok(iter) => {
                    for value in iter {
                        match value.is_null() {
                            false =>  if value.get_string().unwrap() == name.to_str().unwrap() {exists = true} else {exists = false},
                            true => {                        
                                exists = true;
                                debug!("file_exists");
                            }
                        }
                        
                    }
                }
            }
            exists
        }
    }
}

struct Inode {
    inode:u64
}

#[allow(dead_code)]
impl Inode {
    fn to_u64(&self) -> u64 {
        self.inode
    }

    fn to_i64(&self) -> i64 {
        self.inode as i64
    }

    fn get_partition(&self) -> u64 {
        self.inode % INODE_PARTITIONS
    }
}

impl Filesystem for CrustFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &Path, reply: ReplyEntry) {
        //let parent = Inode{inode:parent};
        debug!("lookup: parent: {:?}, name: {:?}", parent, name.to_str().unwrap());
        let statement = CassStatement:: new(self.cmds.select_inode, 2);
        statement.bind_int64(0, parent as i64).unwrap();
        statement.bind_int64(1,  Inode{inode:parent}.get_partition() as i64).unwrap();
        match self.session.execute_statement(&statement).wait() {
            Err(err) => debug!("lookup: {:?}", err),
            Ok(result) => {
                match inode_exists(result, name) {
                    //FIXME build a proper ATTR struct here
                    true => reply.entry(&TTL, &HELLO_TXT_ATTR, 0),
                    false => reply.error(ENOENT)
                }
            }
        }
    }
    

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("getattr");
        match ino {
            1 => reply.attr(&TTL, &HELLO_DIR_ATTR),
            2 => reply.attr(&TTL, &HELLO_TXT_ATTR),
            _ => reply.error(ENOENT),
        }
    }

    /// Read data
    /// Read should send exactly the number of bytes requested except on EOF or error,
    /// otherwise the rest of the data will be substituted with zeroes. An exception to
    /// this is when the file has been opened in 'direct_io' mode, in which case the
    /// return value of the read system call will reflect the return value of this
    /// operation. fh will contain the value set by the open method, or will be undefined
    /// if the open method didn't set any value.
    #[allow(unused_variables)]
    fn read(&mut self,
            _req: &Request,
            ino: u64,
            _fh: u64,
            offset: u64,
            u32: u32,
            reply: ReplyData) {
        let statement = CassStatement::new(self.cmds.select_inode.clone(), 1);

        let mut future=self.session.execute_statement(&statement);
        statement.bind_int64(0, ino as i64).unwrap();
        match future.wait() {
            Err(err) => {
                reply.error(ENOENT)
            },
            Ok(result) => {
                match result.first_row() {
                    None => error!("--"),
                    Some(row) => {
                        match row.get_column(9).unwrap().get_string() {
                            Err(err) => error!("{:?}--",err),
                            Ok(col) => {
                                //let cstr = CString::new(col.cass_string.data,false);
                                println!("str: {:?}",col);
                                reply.data(&col.to_string().as_bytes()[0..offset as usize]);
                            }
                        }
                    }
                }
            }
        }
    }


    fn mknod(&mut self,
             _req: &Request,
             _parent: u64,
             _name: &Path,
             _mode: u32,
             _rdev: u32,
             reply: ReplyEntry) {
        reply.error(ENOENT);
        panic!("mknod not implemented");
    }

  fn mkdir(&mut self,
             _req: &Request,
             _parent: u64,
             _name: &Path,
             _mode: u32,
             reply: ReplyEntry) {
        reply.error(ENOENT);
        panic!("mkdir not implemented");
    }

    /// Read directory
    /// Send a buffer filled using buffer.fill(), with size not exceeding the
    /// requested size. Send an empty buffer on end of stream. fh will contain the
    /// value set by the opendir method, or will be undefined if the opendir method
    /// didn't set any value.
  fn readdir(&mut self,
               _req: &Request,
               ino: u64,
               _fh: u64,
               offset: u64,
               mut reply: ReplyDirectory) {
        match offset {
            0 => {
                debug!("readdir ino:{} offset:{}",ino,offset);
                let inode:Inode=Inode{inode:ino};
                reply.add(ino, 1, FileType::Directory, &Path::new("."));
                reply.add(ino, 1, FileType::Directory, &Path::new(".."));

                let statement = CassStatement::new(self.cmds.select_child_inodes, 2);
                statement.bind_int64(0, inode.to_i64()).unwrap();
                statement.bind_int64(1, (inode.get_partition()) as i64).unwrap();
                debug!("ino: {}, offset: {}", ino, offset);
                let mut future = self.session.execute_statement(&statement);
                match future.wait() {
                    Err(err) => panic!("fail: {:?}", err),
                    Ok(result) => {
                        for row in result.iter() {
//              let row = row_iterator.get_row();
            //if row.length() > 0 {
                            let column = row.get_column(0);
                            let collection_items = column.unwrap().set_iter().unwrap();
                            for item in collection_items {
                                //let item = collection_items.get_column();
                                match item.get_type() {
                                    CassValueType::TEXT => {
                                        let result = item.get_string();
                                        panic!("result: {:?}", result);
                                        //FIXME have to reply
                                        //reply.add(ino, 1, FileType::RegularFile, &Path::new(result));
                                        //debug!("readdir: item2: {:?}", result)
                                    }
                                    _ => panic!("getting an item from a collection should never fail")
                                }
                            }
                        }
                        debug!("readdir: ok reply");
                        reply.ok();
                    }
                }
            },
            _ => {
                reply.error(ENOENT);
                debug!("readdir: enonent reply");
            }
        }
    }

    fn init(&mut self, _req: &Request) -> Result<(), c_int> {
        debug!("init");
        Ok(())
    }

    /// Clean up filesystem
    /// Called on filesystem exit.
    fn destroy(&mut self, _req: &Request) {
        debug!("destroy");
    }

    /// Forget about an inode
    /// The nlookup parameter indicates the number of lookups previously performed on
    /// this inode. If the filesystem implements inode lifetimes, it is recommended that
    /// inodes acquire a single reference on each lookup, and lose nlookup references on
    /// each forget. The filesystem may ignore forget calls, if the inodes don't need to
    /// have a limited lifetime. On unmount it is not guaranteed, that all referenced
    /// inodes will receive a forget message.
    fn forget(&mut self, _req: &Request, _ino: u64, _nlookup: u64) {
        panic!("forget not implemented");
    }

    /// Set file attributes
    //FIXME provide proper setattr implementation
    fn setattr(&mut self,
               _req: &Request,
               _ino: u64,
               _mode: Option<u32>,
               _uid: Option<u32>,
               _gid: Option<u32>,
               _size: Option<u64>,
               _atime: Option<Timespec>,
               _mtime: Option<Timespec>,
               _fh: Option<u64>,
               _crtime: Option<Timespec>,
               _chgtime: Option<Timespec>,
               _bkuptime: Option<Timespec>,
               _flags: Option<u32>,
               reply: ReplyAttr) {
        debug!("setattr");
        reply.attr(&time::get_time(),&HELLO_TXT_ATTR);
    }

    /// Read symbolic link
    fn readlink(&mut self, _req: &Request, _ino: u64, reply: ReplyData) {
        reply.error(ENOSYS);
        panic!("readlink not implemented");
    }

    /// Remove a file
    fn unlink(&mut self, _req: &Request, _parent: u64, _name: &Path, reply: ReplyEmpty) {
        reply.error(ENOSYS);
        panic!("unlink not implemented");
    }

    /// Remove a directory
    fn rmdir(&mut self, _req: &Request, _parent: u64, _name: &Path, reply: ReplyEmpty) {
        reply.error(ENOSYS);
        panic!("rmdir not implemented");
    }

    /// Create a symbolic link
    fn symlink(&mut self,
               _req: &Request,
               _parent: u64,
               _name: &Path,
               _link: &Path,
               reply: ReplyEntry) {
        reply.error(ENOSYS);
        panic!("symlink not implemented: parent={:?}, name={:?}, link={:?}",
            _parent, _name.to_str(), _link.to_str());

    }

    /// Rename a file
    fn rename(&mut self,
              _req: &Request,
              _parent: u64,
              _name: &Path,
              _newparent: u64,
              _newname: &Path,
              reply: ReplyEmpty) {
        reply.error(ENOSYS);
        panic!("rename not implemented");
    }

    /// Create a hard link
    fn link(&mut self,
            _req: &Request,
            _ino: u64,
            _newparent: u64,
            _newname: &Path,
            reply: ReplyEntry) {
        reply.error(ENOSYS);
        panic!("link not implemented: ino={:?}, _newparent={:?}, _newname={:?}",
            _ino, _newparent, _newname.to_str());
    }

    /// Open a file
    /// Open flags (with the exception of O_CREAT, O_EXCL, O_NOCTTY and O_TRUNC) are
    /// available in flags. Filesystem may store an arbitrary file handle (pointer, index,
    /// etc) in fh, and use this in other all other file operations (read, write, flush,
    /// release, fsync). Filesystem may also implement stateless file I/O and not store
    /// anything in fh. There are also some flags (direct_io, keep_cache) which the
    /// filesystem may set, to change the way the file is opened. See fuse_file_info
    /// structure in <fuse_common.h> for more details.
    fn open(&mut self, _req: &Request, _ino: u64, _flags: u32, reply: ReplyOpen) {
        debug!("open");
        reply.opened(0, 0);
    }

    /// Write data
    /// Write should return exactly the number of bytes requested except on error. An
    /// exception to this is when the file has been opened in 'direct_io' mode, in
    /// which case the return value of the write system call will reflect the return
    /// value of this operation. fh will contain the value set by the open method, or
    /// will be undefined if the open method didn't set any value.
    fn write(&mut self,
             _req: &Request,
             _ino: u64,
             _fh: u64,
             _offset: u64,
             _data: &[u8],
             _flags: u32,
             reply: ReplyWrite) {
        reply.error(ENOSYS);
        panic!("write not implemented");
    }

    /// Flush method
    /// This is called on each close() of the opened file. Since file descriptors can
    /// be duplicated (dup, dup2, fork), for one open call there may be many flush
    /// calls. Filesystems shouldn't assume that flush will always be called after some
    /// writes, or that if will be called at all. fh will contain the value set by the
    /// open method, or will be undefined if the open method didn't set any value.
    /// NOTE: the name of the method is misleading, since (unlike fsync) the filesystem
    /// is not forced to flush pending writes. One reason to flush data, is if the
    /// filesystem wants to return write errors. If the filesystem supports file locking
    /// operations (setlk, getlk) it should remove all locks belonging to 'lock_owner'.
    fn flush(&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        debug!("flush");
        reply.error(ENOSYS);
    }

    /// Release an open file
    /// Release is called when there are no more references to an open file: all file
    /// descriptors are closed and all memory mappings are unmapped. For every open
    /// call there will be exactly one release call. The filesystem may reply with an
    /// error, but error values are not returned to close() or munmap() which triggered
    /// the release. fh will contain the value set by the open method, or will be undefined
    /// if the open method didn't set any value. flags will contain the same flags as for
    /// open.
    fn release(&mut self,
               _req: &Request,
               _ino: u64,
               _fh: u64,
               _flags: u32,
               _lock_owner: u64,
               _flush: bool,
               reply: ReplyEmpty) {
        debug!("release");
        reply.ok();
    }

    /// Synchronize file contents
    /// If the datasync parameter is non-zero, then only the user data should be flushed,
    /// not the meta data.
    fn fsync(&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        reply.error(ENOSYS);
        panic!("fsync not implemented");
    }

    /// Open a directory
    /// Filesystem may store an arbitrary file handle (pointer, index, etc) in fh, and
    /// use this in other all other directory stream operations (readdir, releasedir,
    /// fsyncdir). Filesystem may also implement stateless directory I/O and not store
    /// anything in fh, though that makes it impossible to implement standard conforming
    /// directory stream operations in case the contents of the directory can change
    /// between opendir and releasedir.
    fn opendir(&mut self, _req: &Request, _ino: u64, _flags: u32, reply: ReplyOpen) {
        debug!("opendir");
        reply.opened(0, 0);
    }

    /// Release an open directory
    /// For every opendir call there will be exactly one releasedir call. fh will
    /// contain the value set by the opendir method, or will be undefined if the
    /// opendir method didn't set any value.
    fn releasedir(&mut self, _req: &Request, _ino: u64, _fh: u64, _flags: u32, reply: ReplyEmpty) {
        debug!("releasedir");
        reply.ok();
    }

    /// Synchronize directory contents
    /// If the datasync parameter is set, then only the directory contents should
    /// be flushed, not the meta data. fh will contain the value set by the opendir
    /// method, or will be undefined if the opendir method didn't set any value.
    fn fsyncdir(&mut self,
                _req: &Request,
                _ino: u64,
                _fh: u64,
                _datasync: bool,
                reply: ReplyEmpty) {
        reply.error(ENOSYS);
        panic!("fsyncdir not implemented");
    }

    /// Get file system statistics
    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        debug!("statfs");
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
    }

    /// Set an extended attribute
    fn setxattr(&mut self,
                _req: &Request,
                _ino: u64,
                _name: &OsStr,
                _value: &[u8],
                _flags: u32,
                _position: u32,
                reply: ReplyEmpty) {
        reply.error(ENOSYS);
        panic!("setxattr not implemented");
    }

    /// Get an extended attribute
    fn getxattr(&mut self, _req: &Request, _ino: u64, _name: &OsStr, reply: ReplyData) {
        // FIXME: If arg.size is zero, the size of the value should be sent with fuse_getxattr_out
        // FIXME: If arg.size is non-zero, send the value if it fits, or ERANGE otherwise
        reply.error(ENOSYS);
        //   panic!("getxattr not implemented");
    }

    /// List extended attribute names
    fn listxattr(&mut self, _req: &Request, _ino: u64, reply: ReplyEmpty) {
        // FIXME: If arg.size is zero, the size of the attr list should be sent with fuse_getxattr_out
        // FIXME: If arg.size is non-zero, send the attribute list if it fits, or ERANGE otherwise
        reply.error(ENOSYS);
        panic!("listxattr not implemented");
    }

    /// Remove an extended attribute
    fn removexattr(&mut self, _req: &Request, _ino: u64, _name: &OsStr, reply: ReplyEmpty) {
        debug!("removexattr");
        reply.error(ENOSYS);
    }

    /// Check file access permissions
    /// This will be called for the access() system call. If the 'default_permissions'
    /// mount option is given, this method is not called. This method is not called
    /// under Linux kernel versions 2.4.x
    fn access(&mut self, _req: &Request, _ino: u64, _mask: u32, reply: ReplyEmpty) {
    //FIXME implement proper access controls
        reply.ok();
    }

    /// Create and open a file
    /// If the file does not exist, first create it with the specified mode, and then
    /// open it. Open flags (with the exception of O_NOCTTY) are available in flags.
    /// Filesystem may store an arbitrary file handle (pointer, index, etc) in fh,
    /// and use this in other all other file operations (read, write, flush, release,
    /// fsync). There are also some flags (direct_io, keep_cache) which the
    /// filesystem may set, to change the way the file is opened. See fuse_file_info
    /// structure in <fuse_common.h> for more details. If this method is not
    /// implemented or under Linux kernel versions earlier than 2.6.15, the mknod()
    /// and open() methods will be called instead.
    fn create(&mut self,
              _req: &Request,
              _parent: u64,
              _name: &Path,
              _mode: u32,
              _flags: u32,
              reply: ReplyCreate) {
        println!("create");
        match _name.to_str() {
            Some(path) => {
                println!("Path: {}", path);
                println!("_mode: {}",_mode);
                println!("_flags: {}",_flags);
                println!("_name: {}",_name.is_file());
                println!("_parent: {}", _parent);

                let (partition,inode) = self.allocate_inode();

                let now = time::get_time();
                let new_file = FileAttr{
                    ino:inode,
                    size:0,blocks:0,
                    atime:now,mtime:now,ctime:now,crtime:now,
                    kind:FileType::RegularFile,
                    perm:0o755,
                    nlink:0,
                    uid:0,gid:0,
                    rdev:0,
                    flags:0,
                };

                let statement = CassStatement::new(self.cmds.create_inode, 16);
                println!("inserting inode:{}",new_file.ino);
                statement.bind_int64(0, _parent as i64).unwrap();
                statement.bind_int64(1, new_file.size as i64).unwrap();
                statement.bind_int64(2, new_file.blocks as i64).unwrap();
                statement.bind_int64(3, new_file.atime.sec as i64).unwrap();
                statement.bind_int64(4, new_file.mtime.sec as i64).unwrap();
                statement.bind_int64(5, new_file.ctime.sec as i64).unwrap();
                statement.bind_int64(6, new_file.crtime.sec as i64).unwrap();
                statement.bind_string(7, /* FIXME new_file.kind */ "file").unwrap();
                statement.bind_int32(8, new_file.perm as i32).unwrap();
                statement.bind_int32(9, new_file.nlink as i32).unwrap();
                statement.bind_int32(10, new_file.uid as i32).unwrap();
                statement.bind_int32(11, new_file.gid as i32).unwrap();
                statement.bind_int32(12, new_file.rdev as i32).unwrap();
                statement.bind_int32(13, new_file.flags as i32).unwrap();
                statement.bind_int64(14, partition as i64).unwrap();
                statement.bind_int64(15, new_file.ino as i64).unwrap();

                assert!(!self.session.execute_statement(&statement).wait().is_err());

                let statement = CassStatement::new(self.cmds.add_inode_to_parent, 4);
                println!("adding inode to parent:{}",new_file.ino);
                let parent_partition = _parent % INODE_PARTITIONS;
                statement.bind_string(0, path).unwrap();
                statement.bind_int64(1, inode as i64).unwrap();
                statement.bind_int64(2, _parent as i64).unwrap();
                statement.bind_int64(3, parent_partition as i64).unwrap();
                assert!(!self.session.execute_statement(&statement).wait().is_err());

                //self, ttl: &Timespec, attr: &FileAttr, generation: u64, fh: u64, flags: u32

                //FIXME set correct generation,fh,flags
                reply.created(&now,&new_file,0,0,0);
            },
            None    => {
                println!("No path specified!!");
                reply.error(EIO)
            }
        }
    }

    /// Test for a POSIX file lock
    fn getlk(&mut self,
             _req: &Request,
             _ino: u64,
             _fh: u64,
             _lock_owner: u64,
             _start: u64,
             _end: u64,
             _typ: u32,
             _pid: u32,
             reply: ReplyLock) {
        reply.error(ENOSYS);
        panic!("getlk not implemented");
    }

    /// Acquire, modify or release a POSIX file lock
    /// For POSIX threads (NPTL) there's a 1-1 relation between pid and owner, but
    /// otherwise this is not always the case. For checking lock ownership,
    /// 'fi->owner' must be used. The l_pid field in 'struct flock' should only be
    /// used to fill in this field in getlk(). Note: if the locking methods are not
    /// implemented, the kernel will still allow file locking to work locally.
    /// Hence these are only interesting for network filesystems and similar.
    fn setlk(&mut self,
             _req: &Request,
             _ino: u64,
             _fh: u64,
             _lock_owner: u64,
             _start: u64,
             _end: u64,
             _typ: u32,
             _pid: u32,
             _sleep: bool,
             reply: ReplyEmpty) {
        reply.error(ENOSYS);
        panic!("setlk not implemented");
    }

    /// Map block index within file to block index within device
    /// Note: This makes sense only for block device backed filesystems mounted
    /// with the 'blkdev' option
    fn bmap(&mut self, _req: &Request, _ino: u64, _blocksize: u32, _idx: u64, reply: ReplyBmap) {
        reply.error(ENOSYS);
        panic!("getxattr not implemented");
    }
}

#[cfg(test)]
mod tests {

    use cql_ffi::{CassCluster,CassSession};
    use super::CrustFS;
    use fuse::Filesystem;

    #[test]
    /// create a test file inode as a child of the root inode
    fn create_inode() {
        let cluster = CassCluster::new();
        let cluster = cluster.set_contact_points("127.0.0.1").unwrap();
        let session = CassSession::new();
        match session.connect(&cluster).wait() {
            Err(fail) => println!("fail: {}",fail),
            Ok(session) => {
                let crustfs = CrustFS::build(session);
                //fn create (&mut self, _req: &Request, _parent: u64, _name: &PosixPath,
                //_mode: u32, _flags: uint, reply: ReplyCreate)
                //crustfs.create();
            }
        }
    }
}
