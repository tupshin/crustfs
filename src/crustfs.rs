#![crate_name = "crustfs"]

#![feature(phase)]
#[phase(plugin, link)] extern crate log;

extern crate libc;
extern crate time;
extern crate fuse;
extern crate cassandra;


use std::c_str::CString;

use std::rand::{task_rng, Rng};

use std::io::{FilePermission,TypeFile, TypeDirectory, USER_FILE, USER_DIR};
use std::io::fs::PathExtensions;

use fuse::{FileAttr, Filesystem, Request, ReplyData, ReplyEntry, ReplyAttr, ReplyDirectory};
use fuse::{ReplyEmpty, ReplyOpen, ReplyCreate, ReplyStatfs, ReplyWrite, ReplyLock, ReplyBmap};

use libc::consts::os::posix88::EIO;
use libc::c_int;
use libc::ENOENT;
use libc::ENOSYS;

use time::Timespec;

use cassandra::Statement;
use cassandra::Session;

static HELLO_DIR_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: CREATE_TIME,
    mtime: CREATE_TIME,
    ctime: CREATE_TIME,
    crtime: CREATE_TIME,
    kind: TypeDirectory,
    perm: USER_DIR,
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
    atime: CREATE_TIME,
    mtime: CREATE_TIME,
    ctime: CREATE_TIME,
    crtime: CREATE_TIME,
    kind: TypeFile,
    perm: USER_FILE,
    nlink: 1,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
};

static TTL: Timespec = Timespec { sec: 1, nsec: 0 };    // 1 second

static CREATE_TIME: Timespec = Timespec { sec: 1381237736, nsec: 0 };   // 2013-10-08 08:56

pub struct Commands {
  pub use_ks:String,
  pub select_inode:String,
  pub create_ks:String,
  pub create_inode_table:String,
  pub create_fs_metadata_table:String,
  pub create_inode:String,
  pub select_max_inode:String,
  pub insert_inode_placeholder:String,
}

pub struct CrustFS {
  pub session:Session,
  pub cmds:Commands,
}

//This is the number of partitions the inodes will be sharded into.
//In production, this should be quite high. If you want strictly linear
//growth in inode generation for testing, set it to 1, but that will
//cause a hot spot in the cluster.
static INODE_PARTITIONS:u64=5;

impl CrustFS {
  /*This function chooses a random inode partition,
    generates the next valid inode for that partition
    and inserts a stub (just the partition_id and inode)
    reserving the inode for the calling function
  */
  fn allocate_inode(&mut self) -> (u64,u64) {
    //choose a random partition
    let partition:u64 = task_rng().gen_range(0u64,INODE_PARTITIONS);

    //select the maximum inode value in that partition.
    let select_max_inode_statement = &mut Statement::build_from_string(self.cmds.select_max_inode.clone(),1);
    println!("binding partition: {}",partition);
    select_max_inode_statement.bind_int64(0, partition as i64);

    //return the inode that is generated out of the 
    match self.session.execute(select_max_inode_statement) {
      Ok(select_result) => {
        //generate  a new inode by taking max found in #2 + INODE_PARTITIONS which is our offset for inodes within each partition.      
        let next_inode = if select_result.row_count() == 0u64
          {
            debug!("zero rows found in partition. adding first one");
            partition
        } //if this will be the first inode in the partition, then the new inode will be the same as the partition id.
        else {
          match select_result.first_row().get_column(0).get_int64() {
            Ok(res) => {
              debug!("got row {} in partition {}. adding new row {}",res,partition,res as u64+INODE_PARTITIONS);
              res as u64 + INODE_PARTITIONS
              },
            Err(e) => {fail!("corrupt fs: {}",e)}
          }
        };
        //insert into inode if not exists on the new inode.
        let insert_inode_placeholder_statement = &mut Statement::build_from_string(self.cmds.insert_inode_placeholder.clone(),2);

        //FIXME make these chainable
        insert_inode_placeholder_statement.bind_int64(0, partition as i64);
        insert_inode_placeholder_statement.bind_int64(1, next_inode as i64); 
        match self.session.execute(insert_inode_placeholder_statement) {
          Ok(_) => {
            //FIXME. make sure I don't need to pay more attention to a succsesful result
            (partition,next_inode) //the insert succeeded, so we can consider the generated inode to be valid
          }
          Err(e) => {
            debug!("insert race condition encountered: {}",e);
            self.allocate_inode() //can retry on failed inode allocation be tail recursive?
          }
        }
      },
      Err(e) => {
        fail!("server errror: {}", e);
      },
    }
  }
  
}

impl Filesystem for CrustFS {
  fn lookup (&mut self, _req: &Request, parent: u64, name: &PosixPath, reply: ReplyEntry) {
    if parent == 1 && name.as_str() == Some("hello.txt") {
      reply.entry(&TTL, &HELLO_TXT_ATTR, 0);
    } else {
      reply.error(ENOENT);
    }
  }

  fn getattr (&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
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
  fn read (&mut self, _req: &Request, ino: u64, _fh: u64, offset: u64, _size: uint, reply: ReplyData) {unsafe{
    let mut statement = Statement::build_from_string(self.cmds.select_inode.clone(), 1);
    
    let future=self.session.execute(&mut statement);
    statement.bind_string(0, ino.to_string());
    match future {
      Err(_) => {
        reply.error(ENOENT)
      },
      Ok(result) => {
        let mut rows = result.iterator();
        if rows.next() {
          let row = rows.get_row();
          match row.get_column(9).get_string() {
            Err(err) => error!("{}--",err),
            Ok(col) => {
              let cstr = CString::new(col.cass_string.data,false);
               debug!("{}--",cstr);
              reply.data(cstr.as_bytes().slice_from(offset as uint));
            }
          }
        }
      }
    }
  }}

  fn mknod (&mut self, _req: &Request, _parent: u64, _name: &PosixPath, _mode: u32, _rdev: u32, reply: ReplyEntry) {
    reply.error(ENOENT);
    fail!("mknod not implemented");
  }

  fn mkdir (&mut self, _req: &Request, _parent: u64, _name: &PosixPath, _mode: u32, reply: ReplyEntry) {
    reply.error(ENOENT);
    fail!("mkdir not implemented");
  }

  /// Read directory
  /// Send a buffer filled using buffer.fill(), with size not exceeding the
  /// requested size. Send an empty buffer on end of stream. fh will contain the
  /// value set by the opendir method, or will be undefined if the opendir method
  /// didn't set any value.
  fn readdir (&mut self, _req: &Request, ino: u64, _fh: u64, offset: u64, mut reply: ReplyDirectory) {
    if ino == 1 {
      if offset == 0 {
                reply.add(1, 0, TypeDirectory, &PosixPath::new("."));
                reply.add(1, 1, TypeDirectory, &PosixPath::new(".."));
                reply.add(2, 2, TypeFile, &PosixPath::new("hello.txt"));
            }
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

  fn init (&mut self, _req: &Request) -> Result<(), c_int> {
    Ok(())
  }

  /// Clean up filesystem
  /// Called on filesystem exit.
  fn destroy (&mut self, _req: &Request) {
  }

  /// Forget about an inode
  /// The nlookup parameter indicates the number of lookups previously performed on
  /// this inode. If the filesystem implements inode lifetimes, it is recommended that
  /// inodes acquire a single reference on each lookup, and lose nlookup references on
  /// each forget. The filesystem may ignore forget calls, if the inodes don't need to
  /// have a limited lifetime. On unmount it is not guaranteed, that all referenced
  /// inodes will receive a forget message.
  fn forget (&mut self, _req: &Request, _ino: u64, _nlookup: uint) {
        fail!("forget not implemented");
  }

  /// Set file attributes
  fn setattr (&mut self, _req: &Request, _ino: u64, _mode: Option<u32>, _uid: Option<u32>, _gid: Option<u32>, _size: Option<u64>, _atime: Option<Timespec>, _mtime: Option<Timespec>, _fh: Option<u64>, _crtime: Option<Timespec>, _chgtime: Option<Timespec>, _bkuptime: Option<Timespec>, _flags: Option<u32>, reply: ReplyAttr) {
    reply.error(ENOSYS);
    fail!("setattr not implemented");
    }

  /// Read symbolic link
  fn readlink (&mut self, _req: &Request, _ino: u64, reply: ReplyData) {
    reply.error(ENOSYS);
    fail!("readlink not implemented");
  }

  /// Remove a file
  fn unlink (&mut self, _req: &Request, _parent: u64, _name: &PosixPath, reply: ReplyEmpty) {
    reply.error(ENOSYS);
    fail!("unlink not implemented");
  }

  /// Remove a directory
  fn rmdir (&mut self, _req: &Request, _parent: u64, _name: &PosixPath, reply: ReplyEmpty) {
    reply.error(ENOSYS);
    fail!("rmdir not implemented");    
  }

  /// Create a symbolic link
  fn symlink (&mut self, _req: &Request, _parent: u64, _name: &PosixPath, _link: &PosixPath, reply: ReplyEntry) {
    reply.error(ENOSYS);
    fail!("symlink not implemented");
  }

  /// Rename a file
  fn rename (&mut self, _req: &Request, _parent: u64, _name: &PosixPath, _newparent: u64, _newname: &PosixPath, reply: ReplyEmpty) {
    reply.error(ENOSYS);
    fail!("rename not implemented");
  }

  /// Create a hard link
  fn link (&mut self, _req: &Request, _ino: u64, _newparent: u64, _newname: &PosixPath, reply: ReplyEntry) {
    reply.error(ENOSYS);
    fail!("link not implemented");
  }

  /// Open a file
  /// Open flags (with the exception of O_CREAT, O_EXCL, O_NOCTTY and O_TRUNC) are
  /// available in flags. Filesystem may store an arbitrary file handle (pointer, index,
  /// etc) in fh, and use this in other all other file operations (read, write, flush,
  /// release, fsync). Filesystem may also implement stateless file I/O and not store
  /// anything in fh. There are also some flags (direct_io, keep_cache) which the
  /// filesystem may set, to change the way the file is opened. See fuse_file_info
  /// structure in <fuse_common.h> for more details.
  fn open (&mut self, _req: &Request, _ino: u64, _flags: uint, reply: ReplyOpen) {
    reply.opened(0, 0);
  }

  /// Write data
  /// Write should return exactly the number of bytes requested except on error. An
  /// exception to this is when the file has been opened in 'direct_io' mode, in
  /// which case the return value of the write system call will reflect the return
  /// value of this operation. fh will contain the value set by the open method, or
  /// will be undefined if the open method didn't set any value.
  fn write (&mut self, _req: &Request, _ino: u64, _fh: u64, _offset: u64, _data: &[u8], _flags: uint, reply: ReplyWrite) {
    reply.error(ENOSYS);
    fail!("write not implemented");    
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
  fn flush (&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
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
  fn release (&mut self, _req: &Request, _ino: u64, _fh: u64, _flags: uint, _lock_owner: u64, _flush: bool, reply: ReplyEmpty) {    
    reply.ok();
  }

  /// Synchronize file contents
  /// If the datasync parameter is non-zero, then only the user data should be flushed,
  /// not the meta data.
  fn fsync (&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
    reply.error(ENOSYS);
    fail!("fsync not implemented");    
  }

  /// Open a directory
  /// Filesystem may store an arbitrary file handle (pointer, index, etc) in fh, and
  /// use this in other all other directory stream operations (readdir, releasedir,
  /// fsyncdir). Filesystem may also implement stateless directory I/O and not store
  /// anything in fh, though that makes it impossible to implement standard conforming
  /// directory stream operations in case the contents of the directory can change
  /// between opendir and releasedir.
  fn opendir (&mut self, _req: &Request, _ino: u64, _flags: uint, reply: ReplyOpen) {
    reply.opened(0, 0);
  }

  /// Release an open directory
  /// For every opendir call there will be exactly one releasedir call. fh will
  /// contain the value set by the opendir method, or will be undefined if the
  /// opendir method didn't set any value.
  fn releasedir (&mut self, _req: &Request, _ino: u64, _fh: u64, _flags: uint, reply: ReplyEmpty) {
    reply.ok();
  }

  /// Synchronize directory contents
  /// If the datasync parameter is set, then only the directory contents should
  /// be flushed, not the meta data. fh will contain the value set by the opendir
  /// method, or will be undefined if the opendir method didn't set any value.
  fn fsyncdir (&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
    reply.error(ENOSYS);
    fail!("fsyncdir not implemented");
  }

  /// Get file system statistics
  fn statfs (&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
    reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
  }

  /// Set an extended attribute
  fn setxattr (&mut self, _req: &Request, _ino: u64, _name: &[u8], _value: &[u8], _flags: uint, _position: u32, reply: ReplyEmpty) {
    reply.error(ENOSYS);
    fail!("setxattr not implemented");
  }

  /// Get an extended attribute
  fn getxattr (&mut self, _req: &Request, _ino: u64, _name: &[u8], reply: ReplyData) {
  // FIXME: If arg.size is zero, the size of the value should be sent with fuse_getxattr_out
  // FIXME: If arg.size is non-zero, send the value if it fits, or ERANGE otherwise
    reply.error(ENOSYS);
    fail!("getxattr not implemented");
  }

  /// List extended attribute names
  fn listxattr (&mut self, _req: &Request, _ino: u64, reply: ReplyEmpty) {
  // FIXME: If arg.size is zero, the size of the attribute list should be sent with fuse_getxattr_out
  // FIXME: If arg.size is non-zero, send the attribute list if it fits, or ERANGE otherwise
    reply.error(ENOSYS);
    fail!("listxattr not implemented");
  }

  /// Remove an extended attribute
  fn removexattr (&mut self, _req: &Request, _ino: u64, _name: &[u8], reply: ReplyEmpty) {
    reply.error(ENOSYS);
    fail!("removexattr not implemented");
  }

  /// Check file access permissions
  /// This will be called for the access() system call. If the 'default_permissions'
  /// mount option is given, this method is not called. This method is not called
  /// under Linux kernel versions 2.4.x
  fn access (&mut self, _req: &Request, _ino: u64, _mask: uint, reply: ReplyEmpty) {
    reply.error(ENOSYS);
    fail!("access not implemented");
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
  fn create (&mut self, _req: &Request, _parent: u64, _name: &PosixPath, _mode: u32, _flags: uint, reply: ReplyCreate) {
    match _name.to_c_str().as_str() {
      Some(p) => {
        println!("Path: {}", p);
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
          kind:TypeFile,
          perm:FilePermission::empty(),
          nlink:0,
          uid:0,gid:0,
          rdev:0,
          flags:0,
        };

        let mut statement = Statement::build_from_string(self.cmds.create_inode.clone(), 16);
        println!("inserting inode:{}",new_file.ino);
        statement.bind_int64(0, _parent as i64);
        statement.bind_int64(1, new_file.size as i64);
        statement.bind_int64(2, new_file.blocks as i64);
        statement.bind_int64(3, new_file.atime.sec as i64);
        statement.bind_int64(4, new_file.mtime.sec as i64);
        statement.bind_int64(5, new_file.ctime.sec as i64);
        statement.bind_int64(6, new_file.crtime.sec as i64);
        statement.bind_string(7, /* FIXME new_file.kind */ "file".to_string());
        statement.bind_int32(8, new_file.perm.bits() as i32);
        statement.bind_int32(9, new_file.nlink as i32);
        statement.bind_int32(10, new_file.uid as i32);
        statement.bind_int32(11, new_file.gid as i32);
        statement.bind_int32(12, new_file.rdev as i32);
        statement.bind_int32(13, new_file.flags as i32);
        statement.bind_int64(14, partition as i64);
        statement.bind_int64(15, new_file.ino as i64);
        
        assert!(self.session.execute(&mut statement).is_ok());

         reply.error(EIO);
        },
      None    => {
        println!("No path specified!!");
        reply.error(EIO)
      }
    }

   
  }

  /// Test for a POSIX file lock
  fn getlk (&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, _start: u64, _end: u64, _typ: u32, _pid: u32, reply: ReplyLock) {
    reply.error(ENOSYS);
    fail!("getlk not implemented");
  }

  /// Acquire, modify or release a POSIX file lock
  /// For POSIX threads (NPTL) there's a 1-1 relation between pid and owner, but
  /// otherwise this is not always the case. For checking lock ownership,
  /// 'fi->owner' must be used. The l_pid field in 'struct flock' should only be
  /// used to fill in this field in getlk(). Note: if the locking methods are not
  /// implemented, the kernel will still allow file locking to work locally.
  /// Hence these are only interesting for network filesystems and similar.
  fn setlk (&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, _start: u64, _end: u64, _typ: u32, _pid: u32, _sleep: bool, reply: ReplyEmpty) {
    reply.error(ENOSYS);
    fail!("setlk not implemented");
  }

  /// Map block index within file to block index within device
  /// Note: This makes sense only for block device backed filesystems mounted
  /// with the 'blkdev' option
  fn bmap (&mut self, _req: &Request, _ino: u64, _blocksize: uint, _idx: u64, reply: ReplyBmap) {
    reply.error(ENOSYS);
    fail!("getxattr not implemented");
  }
}
