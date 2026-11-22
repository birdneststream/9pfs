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
#include <hurd/pager.h>
#include <errno.h>

error_t
S_io_map (struct protid *protid,
          mach_port_t *rdobj,
          mach_msg_type_name_t *rdtype,
          mach_port_t *wrobj,
          mach_msg_type_name_t *wrtype)
{
  error_t err;
  int flags;
  struct node *np;
  struct pager *pager;
  boolean_t deref_pager = FALSE;

  if (!protid)
    return EOPNOTSUPP;

  flags = protid->po->user_open_flags & O_RDWR;
  if (!flags)
    {
      /* Return null ports, successfully.  The client
         translates this to EACCES from mmap.  */
      *rdobj = *wrobj = MACH_PORT_NULL;
      *rdtype = *wrtype = MACH_MSG_TYPE_COPY_SEND;
      return 0;
    }

  np = protid->po->np;
  pthread_mutex_lock (&np->lock);

  pager = np->pager;
  if (!pager)
    {
      uint16_t n_qids;
      /* Whether to open the pager fid for writing.  Only a mapping
         that carries write access can lead to dirty pages, and the
         server checks the fid user against the file at open time.
         Read-only mappings get a read-only fid, which is the common
         case and skips any write permission round trip.  */
      int writable = !p9_readonly && (flags & O_WRITE);

      assert_backtrace (np->pager_fid == P9_NO_FID);
      if (protid->walk_fid == P9_NO_FID)
        {
          pthread_mutex_unlock (&np->lock);
          return EBADF;
        }
      np->pager_fid = p9_fid_alloc ();
      err = p9_rpc (P9_WALK_REQUEST,
                    "442", protid->walk_fid, np->pager_fid, 0,
                    "2", &n_qids);
      if (err)
        {
          pthread_mutex_unlock (&np->lock);
          return err;
        }
      /* FIXME: Do this more robustly, perhaps let the pager hold its own protid */
      {
        struct p9_qid qid;
        uint32_t max_message_size;
        unsigned char core_mode = writable ? 2 : 0;
        uint32_t l_flags = writable ? 2 : 0;

        if (p9_version >= P9_VERSION_2000_L)
          err = p9_rpc (P9_LOPEN_REQUEST,
                        "44", np->pager_fid, l_flags,
                        "1484", &qid.type, &qid.version, &qid.path,
                        &max_message_size);
        else
          err = p9_rpc (P9_OPEN_REQUEST,
                        "41", np->pager_fid, core_mode,
                        "1484", &qid.type, &qid.version, &qid.path,
                        &max_message_size);
        if (err && writable
            && (err == EACCES || err == EPERM))
          {
            /* The server refuses write access to this user.  Retry
               read-only rather than fail the mapping, and let the
               write half of the map be refused below.  */
            writable = 0;
            if (p9_version >= P9_VERSION_2000_L)
              err = p9_rpc (P9_LOPEN_REQUEST,
                            "44", np->pager_fid, 0,
                            "1484", &qid.type, &qid.version, &qid.path,
                            &max_message_size);
            else
              err = p9_rpc (P9_OPEN_REQUEST,
                            "41", np->pager_fid, 0,
                            "1484", &qid.type, &qid.version, &qid.path,
                            &max_message_size);
          }
        if (err)
          {
            p9_rpc (P9_CLUNK_REQUEST,
                    "4", np->pager_fid, "");
            np->pager_fid = P9_NO_FID;
            pthread_mutex_unlock (&np->lock);
            return err;
          }
      }

      /* XXX: MEMORY_OBJECT_COPY_DELAY is not correct here, since the remote
         server could also be modifying the files and we wouldn't know.  */
      pager = pager_create ((struct user_pager_info *) np, p9_pager_bucket,
                            FALSE, MEMORY_OBJECT_COPY_DELAY, FALSE);
      if (!pager)
        {
          err = errno;
          p9_rpc (P9_CLUNK_REQUEST,
                  "4", np->pager_fid, "");
          np->pager_fid = P9_NO_FID;
          pthread_mutex_unlock (&np->lock);
          return err;
        }
      deref_pager = TRUE;

      /* Pager holds a strong reference to its node.  The node holds
         a weak reference back to the pager.  */
      ports_port_ref_weak (pager);
      p9_nref (np);
      np->pager = pager;
      np->pager_writable = writable;
    }
  else if ((flags & O_WRITE) && !np->pager_writable && !p9_readonly)
    {
      /* The pager exists with a read-only fid, opened by an earlier
         read-only mapping, and this mapping wants write access.
         Walk a fresh fid from this protid, so the server checks the
         writing user, open it read-write, then swap it in.

         The old fid is deliberately left open rather than clunked.
         A pager callback thread may hold the old fid number across
         an in flight T-read, and a clunk that lands first would fail
         that read and error the page.  The leaked fid stays valid
         for the life of the connection, and this upgrade happens at
         most once per pager.  */
      uint32_t new_fid = p9_fid_alloc ();
      uint16_t n_qids;

      err = p9_rpc (P9_WALK_REQUEST,
                    "442", protid->walk_fid, new_fid, 0,
                    "2", &n_qids);
      if (!err)
        {
          struct p9_qid qid;
          uint32_t max_message_size;

          if (p9_version >= P9_VERSION_2000_L)
            err = p9_rpc (P9_LOPEN_REQUEST,
                          "44", new_fid, 2,
                          "1484", &qid.type, &qid.version, &qid.path,
                          &max_message_size);
          else
            err = p9_rpc (P9_OPEN_REQUEST,
                          "41", new_fid, 2,
                          "1484", &qid.type, &qid.version, &qid.path,
                          &max_message_size);
        }
      if (err)
        {
          p9_rpc (P9_CLUNK_REQUEST,
                  "4", new_fid, "");
          pthread_mutex_unlock (&np->lock);
          return err;
        }

      /* Plain aligned word stores.  Callbacks running without the
         node lock see either fid, and both remain valid on the
         server.  No write back can race the swap, since pages were
         supplied read-only until now.  */
      np->pager_fid = new_fid;
      np->pager_writable = 1;
    }

  /* Hand out the write half of the map only when the pager fid can
     carry the writes back.  A null write object makes the client mmap
     fail with EACCES, which is the honest answer when the server
     refused write access to the pager fid.  */
  switch (flags)
    {
    case O_RDWR:
      /* Call pager_get_port twice to account for two make-sends.  */
      *wrobj = np->pager_writable ? pager_get_port (pager) : MACH_PORT_NULL;
      *rdobj = pager_get_port (pager);
      *rdtype = *wrtype = MACH_MSG_TYPE_MAKE_SEND;
      break;
    case O_READ:
      *wrobj = MACH_PORT_NULL;
      *rdobj = pager_create_ro_port (pager);
      *rdtype = *wrtype = MACH_MSG_TYPE_MOVE_SEND;
      break;
    case O_WRITE:
      /* We'll return null in *rdport, but that's mostly informative,
         it doesn't really prevent the client from reading.  */
      *wrobj = np->pager_writable ? pager_get_port (pager) : MACH_PORT_NULL;
      *rdobj = MACH_PORT_NULL;
      *rdtype = *wrtype = MACH_MSG_TYPE_MAKE_SEND;
      break;
    default:
      assert_backtrace (0);
    }

  pthread_mutex_unlock (&np->lock);

  if (deref_pager)
    ports_port_deref (pager);

  return 0;
}
