# AIX 64bit, Tuxedo Workstation client (-w)
export CC = xlc_r

all:
	buildclient -w -o RUSH_TEST -f "-q64 -O2 -qlanglvl=extc99" -l "-lm" RUSH_TEST.c

clean:
	rm -f RUSH_TEST *.o