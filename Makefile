CFLAGS = -Wall -I/usr/local/include -I/usr/include -std=gnu99

GLIB_FLAGS = `pkg-config --libs glib-2.0 --cflags glib-2.0`

all:
	gcc exec.c parser.c main.c -g ${CFLAGS} ${LDFLAGS} ${GLIB_FLAGS} -o implDB_t2

debug:
	gcc exec.c parser.c main.c -g ${CFLAGS} ${LDFLAGS} ${GLIB_FLAGS} -o implDB_t2 -DDEBUG

clean:
	rm -f implDB_t2
