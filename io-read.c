/* 9P translator

   Copyright (C) 2021-2026 Sergey Bugaev <bugaevc@gmail.com>

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>. */

/* Proudly written in GNU nano. */

#define _GNU_SOURCE 1

#include "9pfs.h"
#include "9p-rpc.h"
#include "io_S.h"
#include <fcntl.h>
#include <errno.h>
#include <mach.h>

/* Reply buffer for reads too large for MIG's inline reply buffer.

   MIG sends the reply inline while the data fits the 2048 byte buffer
   inside the reply message.  Anything larger is sent out of line.  The
   generated server code hands the pointer from *data to mach_msg,
   which snapshots the pages while building the reply.  The Makefile
   rewrites the generated descriptor to the copying form, so our pages
   survive the send and can be reused for the next read.  Without that
   rewrite the kernel would move the pages to the reader and unmap them
   here.

   One buffer per server thread.  The thread running this handler is
   the same thread whose MIG loop sends the reply right after we
   return.  No other thread can touch this buffer between the read and
   the send, so the cache needs no lock of its own.

   A single shared buffer would not be safe, not even under np->lock.
   Reads on different nodes hold different node locks and can run in
   parallel.  Even on one node the lock is released before the reply is
   sent, so a second read could overwrite pending reply data.  Keeping
   the buffer thread local closes both windows.

   Allocated on first use.  Sized for the negotiated message limit,
   since a read can never ask for more than that.  The 64 KiB floor
   covers reads that arrive before the limit is known.  Only grows.  */

#define REPLY_CACHE_INITIAL_SIZE (64 * 1024)

static __thread vm_address_t reply_cache;
static __thread vm_size_t reply_cache_size;

/* Point *data at this thread's reply cache, making sure it holds at
   least amount bytes.  Returns an errno on allocation failure.  */
static error_t
acquire_reply_cache (struct node *np, vm_size_t amount, char **data)
{
  error_t err;
  vm_size_t wanted, full;
  vm_address_t addr;

  wanted = round_page (amount);
  if (reply_cache_size >= wanted)
    {
      *data = (char *) reply_cache;
      return 0;
    }

  /* First allocation, or a read larger than any before.  Size it for
     the largest read this node can ask for.  The 9P version
     negotiation runs before any read can arrive.  */
  full = REPLY_CACHE_INITIAL_SIZE;
  if (np->max_message_size >= 24)
    {
      full = round_page (np->max_message_size - 24);
      if (full < REPLY_CACHE_INITIAL_SIZE)
        full = REPLY_CACHE_INITIAL_SIZE;
    }
  if (wanted < full)
    wanted = full;

  err = vm_allocate (mach_task_self (), &addr, wanted, 1);
  if (err)
    return err;

  /* The old contents are never read again.  */
  if (reply_cache)
    vm_deallocate (mach_task_self (), reply_cache, reply_cache_size);

  reply_cache = addr;
  reply_cache_size = wanted;
  *data = (char *) reply_cache;
  return 0;
}

error_t
S_io_read (struct protid *protid,
           char **data,
           mach_msg_type_number_t *datalen,
           loff_t offset,
           vm_size_t amount)
{
  error_t err;
  struct peropen *po;
  struct node *np;
  loff_t start;
  uint32_t nread;

  if (protid == NULL)
    return EOPNOTSUPP;

  po = protid->po;
  np = po->np;

  if (!(po->user_open_flags & O_READ))
    return EBADF;

  /* Cap amount to the size we can fit in one message.  */
  if (amount > np->max_message_size - 24)
    amount = np->max_message_size - 24;

  pthread_mutex_lock (&np->lock);

  /* TODO: If user_open_flags & O_NOLINK and we don't have
     an io_fid yet, ensure we have stat, and either parse symlink
     target from there, or use Treadlink.  */
  err = p9_ensure_open (protid, O_READ);
  if (err)
    goto out;

  if (amount > *datalen)
    {
      /* Too large for the inline reply buffer.  Read into the cached
         reply buffer of this thread instead of allocating and freeing
         a fresh one on every call.  */
      err = acquire_reply_cache (np, amount, data);
      if (err)
        /* Fall back to a short inline reply.  */
        amount = *datalen;
    }

  /* Cap amount again.  */
  nread = amount;
  if (nread > np->max_message_size - 24)
    nread = np->max_message_size - 24;

  start = (offset == -1) ? po->offset : offset;

  err = p9_rpc (P9_READ_REQUEST,
                "484", protid->io_fid, (uint64_t) start, nread,
                "d", &nread, *data);
  if (err)
    goto out;

  *datalen = nread;
  if (offset == -1)
    po->offset += nread;

 out:
  pthread_mutex_unlock (&np->lock);

  return err;
}
