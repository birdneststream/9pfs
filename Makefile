all: 9pfs

MIG ?= mig

IO_SRCS = $(patsubst %.c,%.o,$(wildcard io-*.c))
FSYS_SRCS = $(patsubst %.c,%.o,$(wildcard fsys-*.c))
FS_SRCS = $(patsubst %.c,%.o,$(wildcard file-*.c) $(wildcard dir-*.c))

SRCS =  9p.o 9pfs.o 9p-rpc.o 9p-err.o main.o \
	nref.o nrele.o nput.o \
	fid-alloc.o fid-free.o \
	make-node.o make-peropen.o make-protid.o \
	release-protid.o release-peropen.o \
	ioServer.o fsysServer.o fsServer.o \
	pager.o \
	$(IO_SRCS) $(FSYS_SRCS) $(FS_SRCS)

$(FSYS_SRCS) main.o: fsys_S.h
$(IO_SRCS) main.o: io_S.h
$(FS_SRCS) main.o: fs_S.h

LIBS = shouldbeinlibc ihash ports iohelp fshelp pager

9pfs: $(SRCS)
	$(CC) $(CFLAGS) $^ -o $@ -pthread $(foreach LIB,$(LIBS),-l $(LIB))

# The io_read reply data goes out of line once it does not fit the
# inline buffer.  MIG generates the descriptor with deallocate set,
# which makes mach_msg move our pages to the reader instead of copying
# them.  The reply cache in io-read.c needs its pages back, so flip the
# flag to the copying form, for io only.  The kernel rewrites the
# deallocate bit before delivery, so the reader sees the same message
# either way.  If a MIG upgrade changes the generated line, fail the
# build instead of silently losing the cache.
%Server.c %_S.h:
	echo "#include <hurd/"$*".defs>" | $(MIG) \
		-n -DSERVERPREFIX=S_ -imacros mig-mutate.h \
		-server $*Server.c -sheader $*_S.h \
		/dev/stdin
	@if [ "$*" = io ]; then \
	  grep -qF 'OutP->dataType.msgtl_header.msgt_deallocate = TRUE;' $*Server.c \
	    || { echo "io_read reply no longer sets deallocate; review the reply cache" >&2; exit 1; }; \
	  sed -i 's/OutP->dataType.msgtl_header.msgtl_deallocate = TRUE;/OutP->dataType.msgtl_header.msgtl_deallocate = FALSE;/' $*Server.c; \
	fi

clean:
	rm -f *.o *Server.c *_S.h 9pfs

.PHONY: clean
