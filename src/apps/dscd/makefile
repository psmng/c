# Makefile for dscd
#
# 사용법:
#   make          - dscd 바이너리 빌드
#   make clean    - 빌드 산출물 삭제
#   make install  - /usr/local/bin 에 설치 (권한 필요시 sudo make install)

CC      = gcc
CFLAGS  = -Wall -Wextra -O2
LDLIBS  = -lcurl

TARGET  = dscd 
SRC     = main.c

PREFIX  = /usr/local
BINDIR  = $(PREFIX)/bin

.PHONY: all clean install uninstall

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LDLIBS)

clean:
	rm -f $(TARGET)

install: $(TARGET)
	install -d $(BINDIR)
	install -m 755 $(TARGET) $(BINDIR)

uninstall:
	rm -f $(BINDIR)/$(TARGET)