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

#include "9p-rpc.h"
#include "9p-err.h"
#include "9pfs.h"
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>
#include <pthread.h>
#include <error.h>
#include <assert-backtrace.h>
#include <hurd/ihash.h>

static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static struct hurd_ihash interest_table
  = HURD_IHASH_INITIALIZER (offsetof (struct p9_response_interest, locp));
static int some_thread_is_reading = 0;
/* TODO: Use a more sophisticated free tag tracking mechanism.  */
static uint16_t next_tag = ~0;

static pthread_mutex_t write_mutex = PTHREAD_MUTEX_INITIALIZER;

/* The header at the front of every 9P message: a 4-byte
   little-endian size counting the whole message including the
   header, a 1-byte type, and a 2-byte little-endian tag.  */
struct p9_header
{
  uint32_t size;
  unsigned char type;
  uint16_t tag;
} __attribute__ ((packed));

_Static_assert (sizeof (struct p9_header) == 7,
                "the 9P message header is 7 bytes");

/* A heap buffer that grows on demand.  */
struct p9_buffer
{
  unsigned char *buffer;
  size_t size;
};

/* Heap buffers holding the message being sent and the message body
   being received.  Both start at 64 KiB, which covers version
   negotiation, and grow to fit larger messages.  write_mutex guards
   the send buffer, the reading token guards the receive buffer.  */
static struct p9_buffer p9_send_buffer;
static struct p9_buffer p9_recv_buffer;

/* The parse cursor for the message body in the receive buffer, and
   the position one past its end.  */
static unsigned char *p9_recv_cursor;
static unsigned char *p9_recv_end;

#define P9_MIN_BUFFER_SIZE (64 * 1024)

error_t
p9_register_interest (struct p9_response_interest *interest)
{
  error_t err;

  pthread_mutex_lock (&mutex);
  interest->tag = next_tag++;
  err = hurd_ihash_add (&interest_table, interest->tag, interest);
  pthread_mutex_unlock (&mutex);

  return err;
}

/* Read exactly N bytes from FD into BUF, looping over short reads.
   Return 0 on success, or an error code if the connection fails or
   ends before N bytes have arrived.  */
static error_t
readn (int fd, void *buf, size_t n)
{
  unsigned char *p = buf;

  while (n > 0)
    {
      ssize_t nread = read (fd, p, n);
      if (nread < 0)
        {
          if (errno == EINTR)
            continue;
          return errno;
        }
      if (nread == 0)
        /* The connection ended mid-message.  */
        return EIO;
      p += nread;
      n -= nread;
    }
  return 0;
}

/* Write exactly N bytes from BUF to FD, looping over partial writes.
   Return 0 on success, or an error code on failure.  */
static error_t
writen (int fd, const void *buf, size_t n)
{
  const unsigned char *p = buf;

  while (n > 0)
    {
      ssize_t nwritten = write (fd, p, n);
      if (nwritten < 0)
        {
          if (errno == EINTR)
            continue;
          return errno;
        }
      if (nwritten == 0)
        return EIO;
      p += nwritten;
      n -= nwritten;
    }
  return 0;
}

/* Make sure BUF can hold a message of NEED bytes, growing it to at
   least the minimum buffer size.  The caller holds the lock that
   guards BUF.  */
static error_t
p9_grow_buffer (struct p9_buffer *buf, size_t need)
{
  unsigned char *buffer;

  if (need < P9_MIN_BUFFER_SIZE)
    need = P9_MIN_BUFFER_SIZE;
  if (need <= buf->size)
    return 0;

  buffer = realloc (buf->buffer, need);
  if (!buffer)
    return errno;
  buf->buffer = buffer;
  buf->size = need;
  return 0;
}

static error_t
wait_for_header (struct p9_response_interest *interest)
{
  error_t err;
  struct p9_response_interest *other_interest;
  struct p9_header header;

  uint32_t size;
  unsigned char type;
  uint16_t tag;

  pthread_mutex_lock (&mutex);

 again:
  if (interest->size)
    {
      /* Somebody has read our message header; now it's up to us
         to read the body of the message.  The some_thread_is_reading
         token has been passed to us by whoever has read our header.  */
      assert_backtrace (some_thread_is_reading);
 read_our_header:
      hurd_ihash_locp_remove (&interest_table, interest->locp);
      pthread_mutex_unlock (&mutex);
      return 0;
    }

  if (some_thread_is_reading)
    {
      /* Wait for that thread to wake us up.  */
      err = pthread_hurd_cond_wait_np (&interest->cond, &mutex);
      if (err)
        {
          pthread_mutex_unlock (&mutex);
          // TODO: remove from ihash?
          return err;
        }
      goto again;
    }

  /* Noone else is reading, so we'll do it.  */
  some_thread_is_reading = 1;
  pthread_mutex_unlock (&mutex);

  /* Read a message header, converting it from
     little-endian to native endianness. */
  err = readn (p9_conn_in_fd, &header, sizeof (header));
  if (err)
    goto out;

  size = le32toh (header.size);
  type = header.type;
  tag = le16toh (header.tag);

  pthread_mutex_lock (&mutex);

  /* See if we read the message we wanted.  */
  if (tag == interest->tag)
    {
      interest->size = size;
      interest->type = type;
      goto read_our_header;
    }

  other_interest = hurd_ihash_find (&interest_table, tag);
  if (!other_interest)
    {
      // TODO: unexpected message, but we still have to read it.
      assert_backtrace (0);
    }
  /* Fill in the other interest with the header we've read.  */
  assert_backtrace (other_interest->tag == tag);
  other_interest->size = size;
  other_interest->type = type;
  /* Wake it up, and wait for it to figure everything out.
     We pass it the some_thread_is_reading token.  */
  pthread_cond_signal (&other_interest->cond);
  goto again;

 out:
  pthread_mutex_lock (&mutex);
  some_thread_is_reading = 0;
  pthread_mutex_unlock (&mutex);
  return err;
}


static uint32_t
p9_vserialized_size (const char *format, va_list ap)
{
  uint32_t total_size = 0;
  const char *f, *ptr, * const *arr;
  uint32_t size;

  for (f = format; *f; f++)
    switch (*f)
      {
      case '1':
        va_arg (ap, unsigned);
        total_size += 1;
        break;

      case '2':
        va_arg (ap, unsigned);
        total_size += 2;
        break;

      case '4':
        va_arg (ap, uint32_t);
        total_size += 4;
        break;

      case '8':
        va_arg (ap, uint64_t);
        total_size += 8;
        break;

      case 'd':
        size = va_arg (ap, uint32_t);
        total_size += 4;
        va_arg (ap, const char *);
        total_size += size;
        break;

      case 's':
        ptr = va_arg (ap, const char *);
        total_size += 2;
        total_size += strlen (ptr);
        break;

      case 'S':
        arr = va_arg (ap, const char * const *);
        while (*arr)
          {
            total_size += 2;
            total_size += strchrnul (*arr, '/') - *arr;
            arr++;
          }
        break;

      default:
        assert_backtrace (!"unimplemented format specifier");
      }

  return total_size;
}

uint32_t
p9_serialized_size (const char *format, ...)
{
  va_list ap;
  uint32_t size;

  va_start (ap, format);
  size = p9_vserialized_size (format, ap);
  va_end (ap);

  return size;
}

/* Serialize the message body described by FORMAT and *AP into BUF and
   return the position just past the last serialized byte.  The caller
   has sized BUF to hold the whole body.  */
static unsigned char *
p9_do_vsend (const char *format, va_list *ap, unsigned char *buf)
{
  const char *f, *ptr, * const *arr;
  unsigned char *p = buf;
  unsigned char n1;
  uint16_t n2;
  uint32_t n4;
  uint64_t n8;
  size_t size;

  for (f = format; *f; f++)
    switch (*f)
      {
      case '1':
        n1 = va_arg (*ap, unsigned);
        if (p9_dump_exchanges)
          fprintf (stderr, " %" PRIu8, n1);
        memcpy (p, &n1, 1);
        p += 1;
        break;

      case '2':
        n2 = va_arg (*ap, unsigned);
        if (p9_dump_exchanges)
          fprintf (stderr, " %" PRIu16, n2);
        n2 = htole16 (n2);
        memcpy (p, &n2, 2);
        p += 2;
        break;

      case '4':
        n4 = va_arg (*ap, uint32_t);
        if (p9_dump_exchanges)
          fprintf (stderr, " %" PRIu32, n4);
        n4 = htole32 (n4);
        memcpy (p, &n4, 4);
        p += 4;
        break;

      case '8':
        n8 = va_arg (*ap, uint64_t);
        if (p9_dump_exchanges)
          fprintf (stderr, " %" PRIu64, n8);
        n8 = htole64 (n8);
        memcpy (p, &n8, 8);
        p += 8;
        break;

      case 'd':
        size = va_arg (*ap, uint32_t);
        ptr = va_arg (*ap, const char *);
        if (p9_dump_exchanges)
          fprintf (stderr, " %" PRIu32 " <data>", (uint32_t) size);
        n4 = htole32 (size);
        memcpy (p, &n4, 4);
        p += 4;
        memcpy (p, ptr, size);
        p += size;
        break;

      case 's':
        ptr = va_arg (*ap, const char *);
        size = strlen (ptr);
        assert_backtrace (size <= UINT16_MAX);
        if (p9_dump_exchanges)
          fprintf (stderr, " %" PRIu32 " \"%s\"", (uint32_t) size, ptr);
        n2 = htole16 (size);
        memcpy (p, &n2, 2);
        p += 2;
        memcpy (p, ptr, size);
        p += size;
        break;

      case 'S':
        arr = va_arg (*ap, const char * const *);
        while (*arr)
          {
            size = strchrnul (*arr, '/') - *arr;
            assert_backtrace (size <= UINT16_MAX);
            if (p9_dump_exchanges)
              fprintf (stderr, " %" PRIu32 " \"%.*s\"", (uint32_t) size,
                       (int) size, *arr);
            n2 = htole16 (size);
            memcpy (p, &n2, 2);
            p += 2;
            memcpy (p, *arr, size);
            p += size;
            arr++;
          }
        break;

      default:
        assert_backtrace (!"unimplemented format specifier");
      }

  return p;
}

static error_t
p9_vsend (unsigned char type, uint16_t tag,
          const char *format, va_list *ap)
{
  error_t err;
  uint32_t size;
  struct p9_header *hdr;
  unsigned char *end;
  va_list aq;

  /* Count the number of bytes we will need.  */
  va_copy (aq, *ap);
  size = sizeof (struct p9_header) + p9_vserialized_size (format, aq);
  va_end (aq);

  if (p9_dump_exchanges)
    fprintf (stderr, "-> %" PRIu32 " %" PRIu8 " %" PRIu16,
             size, type, tag);

  pthread_mutex_lock (&write_mutex);

  err = p9_grow_buffer (&p9_send_buffer, size);
  if (err)
    goto out;

  /* Serialize the whole message, header first, into the send buffer,
     then write it out in one go.  */
  hdr = (struct p9_header *) p9_send_buffer.buffer;
  hdr->size = htole32 (size);
  hdr->type = type;
  hdr->tag = htole16 (tag);

  end = p9_do_vsend (format, ap, p9_send_buffer.buffer + sizeof *hdr);
  assert_backtrace (end - p9_send_buffer.buffer == size);

  err = writen (p9_conn_out_fd, p9_send_buffer.buffer, size);

 out:
  pthread_mutex_unlock (&write_mutex);

  if (p9_dump_exchanges)
    fprintf (stderr, "\n");

  return err;
}

error_t
p9_send (enum p9_message_type type,
         uint16_t tag, const char *fmt, ...)
{
  error_t err;
  va_list ap;
  va_start (ap, fmt);

  err = p9_vsend (type, tag, fmt, &ap);

  va_end (ap);

  return err;
}

/* Return a pointer to the next N bytes of the message body being parsed
   and advance the cursor past them, or return NULL if the message ends
   before that many bytes.  Only the thread holding the reading token
   may call this.  */
static unsigned char *
p9_take (size_t n)
{
  unsigned char *field = p9_recv_cursor;

  if ((size_t) (p9_recv_end - p9_recv_cursor) < n)
    return NULL;
  p9_recv_cursor += n;
  return field;
}

/* Wrap up a message whose body has been fully read from the connection.
   Complain when BYTES_READ, which counts the seven header bytes, falls
   short of the message size, then hand the reading token back and wake
   up another waiter.  Pass the message size as BYTES_READ to skip the
   complaint.  */
static error_t
p9_finish_message (struct p9_response_interest *interest,
                   uint32_t bytes_read, error_t err)
{
  if (bytes_read < interest->size)
    {
      /* Hmm, we've read less than the whole message; weird.  */
      error (0, 0, "When reading message of type %" PRIu8 ", read only %"
             PRIu32 " of %" PRIu32 " bytes in the message",
             interest->type, bytes_read, interest->size);
    }

  if (p9_dump_exchanges)
    fprintf (stderr, "\n");

  pthread_mutex_lock (&mutex);
  some_thread_is_reading = 0;
  /* If there are threads waiting, wake somebody up.  */
  HURD_IHASH_ITERATE (&interest_table, value)
    {
      struct p9_response_interest *interest = value;
      pthread_cond_signal (&interest->cond);
      break;
    }
  pthread_mutex_unlock (&mutex);

  return err;
}

/* Parse the fields of the received message out of the receive buffer,
   following FORMAT.  Only the thread holding the reading token may
   call this.  */
static error_t
p9_do_vparse (struct p9_response_interest *interest,
              enum p9_message_type request_type,
              const char *format, va_list ap)
{
  error_t err = 0;
  const char *f;
  char *ptr, **dptr;
  uint32_t bytes_read = sizeof (struct p9_header);
  uint32_t size;
  uint16_t string_size;
  unsigned char *field;

  unsigned char *n1p;
  uint16_t *n2p;
  uint32_t *n4p;
  uint64_t *n8p;
  int *intp;
  struct p9_qid *qptr, **qbuf;

  assert_backtrace (interest->type == request_type + 1);

  if (p9_dump_exchanges)
    fprintf (stderr, "<- %" PRIu32 " %" PRIu8 " %" PRIu16,
             interest->size, interest->type, interest->tag);

  for (f = format; *f; f++)
    {
      if (*f != '?' && bytes_read >= interest->size)
        {
          error (0, 0, "Already read %" PRIu32 " bytes of %" PRIu32
                 " bytes in a message of type %" PRIu8 ", and still"
                 " expecting more", bytes_read, interest->size,
                 interest->type);
          err = EIO;
          goto out;
        }
      switch (*f)
        {
        case '?':
          intp = va_arg (ap, int *);
          /* Do we have more data in the message?  */
          *intp = bytes_read < interest->size;
          if (!*intp)
            goto out;
          break;

        case '1':
          n1p = va_arg (ap, unsigned char *);
          field = p9_take (1);
          if (!field)
            goto truncated;
          memcpy (n1p, field, 1);
          bytes_read++;
          if (p9_dump_exchanges)
            fprintf (stderr, " %" PRIu8, *n1p);
          break;

        case '2':
          n2p = va_arg (ap, uint16_t *);
          field = p9_take (2);
          if (!field)
            goto truncated;
          memcpy (n2p, field, 2);
          bytes_read += 2;
          *n2p = le16toh (*n2p);
          if (p9_dump_exchanges)
            fprintf (stderr, " %" PRIu16, *n2p);
          break;

        case '4':
          n4p = va_arg (ap, uint32_t *);
          field = p9_take (4);
          if (!field)
            goto truncated;
          memcpy (n4p, field, 4);
          bytes_read += 4;
          *n4p = le32toh (*n4p);
          if (p9_dump_exchanges)
            fprintf (stderr, " %" PRIu32, *n4p);
          break;

        case '8':
          n8p = va_arg (ap, uint64_t *);
          field = p9_take (8);
          if (!field)
            goto truncated;
          memcpy (n8p, field, 8);
          bytes_read += 8;
          *n8p = le64toh (*n8p);
          if (p9_dump_exchanges)
            fprintf (stderr, " %" PRIu64, *n8p);
          break;

        case 'd':
          n4p = va_arg (ap, uint32_t *);
          size = *n4p;
          field = p9_take (4);
          if (!field)
            goto truncated;
          memcpy (n4p, field, 4);
          bytes_read += 4;
          *n4p = le32toh (*n4p);
          if (p9_dump_exchanges)
            fprintf (stderr, " %" PRIu32 " <data>", *n4p);
          if (*n4p > size)
            {
              err = EOVERFLOW;
              goto out;
            }
          ptr = va_arg (ap, char *);
          field = p9_take (*n4p);
          if (!field)
            goto truncated;
          memcpy (ptr, field, *n4p);
          bytes_read += *n4p;
          break;

        case 's':
          dptr = va_arg (ap, char **);
          field = p9_take (2);
          if (!field)
            goto truncated;
          memcpy (&string_size, field, 2);
          bytes_read += 2;
          string_size = le16toh (string_size);
          field = p9_take (string_size);
          if (!field)
            goto truncated;
          *dptr = malloc (string_size + 1);
          if (!*dptr)
            {
              err = errno;
              goto out;
            }
          memcpy (*dptr, field, string_size);
          bytes_read += string_size;
          (*dptr)[string_size] = 0;
          if (p9_dump_exchanges)
            fprintf (stderr, " %" PRIu16 " \"%s\"",
                     string_size, *dptr);
          break;

        case 'Q':
          n2p = va_arg (ap, uint16_t *);
          qbuf = va_arg (ap, struct p9_qid **);
          field = p9_take (2);
          if (!field)
            goto truncated;
          memcpy (n2p, field, 2);
          bytes_read += 2;
          *n2p = le16toh (*n2p);
          if (p9_dump_exchanges)
            fprintf (stderr, " %" PRIu16, *n2p);
          *qbuf = calloc (*n2p, sizeof (struct p9_qid));
          if (!*qbuf)
            {
              err = errno;
              goto out;
            }
          for (qptr = *qbuf; qptr < *qbuf + *n2p; qptr++)
            {
              field = p9_take (1);
              if (!field)
                goto truncated;
              memcpy (&qptr->type, field, 1);
              bytes_read++;
              if (p9_dump_exchanges)
                fprintf (stderr, " %" PRIu8, qptr->type);

              field = p9_take (4);
              if (!field)
                goto truncated;
              memcpy (&qptr->version, field, 4);
              bytes_read += 4;
              qptr->version = le32toh (qptr->version);
              if (p9_dump_exchanges)
                fprintf (stderr, " %" PRIu32, qptr->version);

              field = p9_take (8);
              if (!field)
                goto truncated;
              memcpy (&qptr->path, field, 8);
              bytes_read += 8;
              qptr->path = le64toh (qptr->path);
              if (p9_dump_exchanges)
                fprintf (stderr, " %" PRIu64, qptr->path);
            }
          break;

        default:
          assert_backtrace (!"unimplemented format specifier");
        }
    }

 out:
  return p9_finish_message (interest, bytes_read, err);

 truncated:
  /* The message ended in the middle of a field.  */
  err = EIO;
  goto out;
}

/* Parse the fields of the message currently waiting in the receive
   buffer.  Used for recursively decoding error replies.  */
static error_t
p9_do_recv (struct p9_response_interest *interest,
            enum p9_message_type request_type,
            const char *fmt, ...)
{
  error_t err;
  va_list ap;

  va_start (ap, fmt);
  err = p9_do_vparse (interest, request_type, fmt, ap);
  va_end (ap);

  return err;
}

/* Receive the body of the message this interest has been woken up for,
   load it into the receive buffer, and dispatch on its type.  We hold
   the reading token on entry.  */
static error_t
p9_do_vrecv (struct p9_response_interest *interest,
             enum p9_message_type request_type,
             const char *format, va_list ap)
{
  error_t err;
  uint32_t body_size;

  /* Load the whole body of the message into the receive buffer, so
     that parsing the fields never has to touch the connection again.  */
  if (interest->size < sizeof (struct p9_header)
      || interest->size > p9_max_message_size)
    {
      error (0, 0, "Server sent a bogus message size of %" PRIu32
             ", the limit is %" PRIu32,
             interest->size, p9_max_message_size);
      return p9_finish_message (interest, interest->size, EIO);
    }

  body_size = interest->size - sizeof (struct p9_header);
  err = p9_grow_buffer (&p9_recv_buffer, body_size);
  if (err)
    return p9_finish_message (interest, interest->size, err);

  err = readn (p9_conn_in_fd, p9_recv_buffer.buffer, body_size);
  if (err)
    return p9_finish_message (interest, interest->size, err);

  p9_recv_cursor = p9_recv_buffer.buffer;
  p9_recv_end = p9_recv_buffer.buffer + body_size;

  if (interest->type == request_type + 1)
    /* All good: go parse the fields.  */
    return p9_do_vparse (interest, request_type, format, ap);

  if (interest->type == P9_LERROR_RESPONSE)
    {
      uint32_t p9_2000l_error;
      err = p9_do_recv (interest, P9_LERROR_REQUEST,
                        "4", &p9_2000l_error);
      if (err)
        return err;
      return p9_lerror_to_err (p9_2000l_error);
    }

  if (interest->type == P9_ERROR_RESPONSE)
    {
      char *error_string = NULL;
      uint32_t unix_errno;

      if (p9_version >= P9_VERSION_2000_U)
        err = p9_do_recv (interest, P9_ERROR_REQUEST,
                          "s4", &error_string, &unix_errno);
      else
        err = p9_do_recv (interest, P9_ERROR_REQUEST,
                          "s", &error_string);
      if (err)
        return err;
      err = p9_error_to_err (error_string);
      free (error_string);
      return err;
    }

  /* Other than in error replies,
     we only expect matching types.  */
  return p9_finish_message (interest, sizeof (struct p9_header), EIEIO);
}

static error_t
p9_vrecv (struct p9_response_interest *interest,
          enum p9_message_type request_type,
          const char *fmt, va_list ap)
{
  error_t err;

  err = wait_for_header (interest);
  if (err)
    return err;

  /* We own the some_thread_is_reading token now.  */

  return p9_do_vrecv (interest, request_type, fmt, ap);
}

error_t
p9_recv (struct p9_response_interest *interest,
          enum p9_message_type request_type,
          const char *fmt, ...)
{
  error_t err;
  va_list ap;

  va_start (ap, fmt);
  err = p9_vrecv (interest, request_type, fmt, ap);
  va_end (ap);

  return err;
}

error_t
p9_rpc (enum p9_message_type request_type,
        const char *request_format, ...)
{
  error_t err;
  va_list ap;
  const char *response_format;
  struct p9_response_interest interest = { 0 };

  pthread_cond_init (&interest.cond, NULL);

  err = p9_register_interest (&interest);
  if (err)
    return err;

  va_start (ap, request_format);

  err = p9_vsend (request_type, interest.tag,
                  request_format, &ap);
  if (err)
    goto out;

  response_format = va_arg (ap, const char *);

  err = p9_vrecv (&interest, request_type,
                  response_format, ap);
  pthread_cond_destroy (&interest.cond);
 out:
  va_end (ap);
  return err;
}
