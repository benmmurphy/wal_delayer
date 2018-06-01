extern crate libc;

#[macro_use]
extern crate redhook;

#[macro_use]
extern crate lazy_static;

use libc::{c_char, c_int, mode_t, size_t, ssize_t, off_t, O_WRONLY, O_APPEND, O_RDWR, O_DSYNC};
use std::ffi::CStr;
use std::collections::HashMap;
use std::sync::Mutex;
use std::{thread, time};


enum Buffer {
  DSync,
  NonDSync(Vec<u8>)
}

lazy_static! {
  static ref WAL_BUFFERS : Mutex<HashMap<c_int, Buffer>> = Mutex::new(HashMap::new());
}


fn contains(haystack: &[u8], needle: &[u8]) -> bool {
  haystack.windows(needle.len()).any(|window| window == needle)
}

fn sync_buffers(fd: c_int) {

  
  let sleep = match WAL_BUFFERS.lock().unwrap().get(&fd) {
    Some(&Buffer::NonDSync(_)) => {
      true
    },
    _ => false
  };

  if sleep {
    println!("sleeping before syncing buffers to disk");
    thread::sleep(time::Duration::from_secs(10));
  }

  sync_buffers_without_delay(fd);

}

fn sync_buffers_without_delay(fd: c_int) {
  let maybe_buffer = match WAL_BUFFERS.lock().unwrap().get_mut(&fd) {
    Some(&mut Buffer::NonDSync(ref mut wal_buffer)) => {
      let mut result = Vec::new();
      result.append(wal_buffer);
      Some(result)
    },
    _ => None
  };

  match maybe_buffer {
    None => {},
    Some(buffer) => {
      let original_write = real!(write);
      let result = unsafe { original_write(fd, buffer.as_ptr(), buffer.len()) };
      if result < 0 || result as usize != buffer.len() {
        panic!("write failed {} {}", fd, result);
      } 
    }
  }
}

  
fn my_fsync(fd: c_int) -> c_int {
  sync_buffers(fd);
  let original_fsync = real!(fsync);
  unsafe {
    original_fsync(fd)
  }
}

fn my_fdatasync(fd: c_int) -> c_int {

  sync_buffers(fd); 
  let original_fdatasync = real!(fdatasync);
  unsafe {
    original_fdatasync(fd)
  } 
}

fn my_close(fd: c_int) -> c_int {
  /* this function is kind of racy but it doesn't really matter
     because people wont't typically call close() then write() to
     same fd */

  sync_buffers_without_delay(fd);
  WAL_BUFFERS.lock().unwrap().remove(&fd);

  let original_close = real!(close);
  unsafe {
    original_close(fd)
  }
}

fn my_lseek(fd: c_int, offset: off_t, whence: c_int) -> off_t {

  match WAL_BUFFERS.lock().unwrap().get(&fd) {
    Some(&Buffer::NonDSync(ref wal_buffer)) => {
      println!("lseek on WAL_BUFFER {} {} {}", fd, offset, whence);
      if wal_buffer.len() > 0 {
        panic!("lseek while buffer has not been flushed");
      }
    },
    _ => {}
  }

  let original_lseek = real!(lseek);
  unsafe {
    original_lseek(fd, offset, whence)
  }
}

 
fn my_write(fd: c_int, buffer: &[u8]) -> ssize_t {
   
  let (perform_write, delay_write) = match WAL_BUFFERS.lock().unwrap().get_mut(&fd) {
    Some(&mut Buffer::DSync) => {
      (true, true)
    },
    Some(&mut Buffer::NonDSync(ref mut wal_buffer)) => {
      println!("buffered write against fd {} {}", fd, buffer.len());
      wal_buffer.extend_from_slice(buffer);
      (false, false)
    },
    _ => {
      (true, false)

    }
  };

  if perform_write {
    if (delay_write) {
      println!("sleeping for O_DSYNC write");
      thread::sleep(time::Duration::from_secs(10));

    }
    let original_write = real!(write);
    unsafe {
      original_write(fd, buffer.as_ptr(), buffer.len())
    }

  } else {
    buffer.len() as isize
  }
}

fn my_open(path: &CStr, oflag: c_int, mode: mode_t) -> c_int {
  let original_open = real!(open);

  println!("open: {:?}", path);
  let fd = unsafe {
    original_open(path.as_ptr(), oflag, mode)
  };

  if fd >= 0 {
    if (((oflag & O_WRONLY) == O_WRONLY) || ((oflag & O_RDWR) == O_RDWR)) && contains(path.to_bytes(), b"pg_xlog/") {

      println!("open hooked: {:?} {} {} wr_only:{} o_append:{}", path, oflag, mode, oflag & O_WRONLY, oflag & O_APPEND);
      if (oflag & O_DSYNC) == O_DSYNC {
        WAL_BUFFERS.lock().unwrap().insert(fd, Buffer::DSync);
      } else {
        WAL_BUFFERS.lock().unwrap().insert(fd, Buffer::NonDSync(Vec::new()));
      }
    }
  }

  fd
}

hook! {
  unsafe fn open(path: *const c_char, oflag: c_int, mode: mode_t) -> c_int => hooked_open {
    my_open(CStr::from_ptr(path), oflag, mode)
  }
}

hook! {
  /* this is const void* in definition but i think it doesn't matter if we use u8 */
  unsafe fn write(fd: c_int, buffer: *const u8, count: size_t) -> ssize_t => hooked_write {
    my_write(fd, std::slice::from_raw_parts(buffer, count))
  }
}

hook! {

  unsafe fn lseek(fd: c_int, offset: off_t, whence: c_int) -> off_t => hooked_lseek {
    my_lseek(fd, offset, whence)
  }
}

hook! {
  unsafe fn close(fd: c_int) -> c_int => hooked_close {
    my_close(fd)
  }
}

hook! {
  unsafe fn fdatasync(fd: c_int) -> c_int => hooked_fdatasync {
    my_fdatasync(fd)    
  }
}

hook! {
  unsafe fn fsync(fd: c_int) -> c_int => hooked_fsync {
    my_fsync(fd)
  }
}

