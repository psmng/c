/*
 * rush_test.c - Tuxedo rush (load) test driver
 *
 * Replays production logs against one or more Tuxedo domains (원장서버)
 * from a separate load server, using the Workstation client (WSL).
 *
 * Architecture
 *   master  : parses INI, forks workers, start barrier, 1s stats, CSV, summary
 *   worker  : 1 process = 1 (job, target) pair = 1 Tuxedo session (WSNADDR)
 *             reads its own byte-range slice of the job's log (no overlap)
 *   job     : 1 log file, N procs, spread round-robin over its targets,
 *             own TPS / ramp-up / duration / in-flight window
 *   Multiple jobs run at the same time => different logs sent concurrently.
 *
 * Log record format (one record per line, '|' separated):
 *   <anything>|SERVICE|TYPE|PAYLOAD      -> first field is ignored (timestamp etc.)
 *   Simplest form accepted:  SERVICE|TYPE|PAYLOAD  (see PARSE_SKIP in rush.ini)
 *   TYPE:
 *     STRING  : payload sent as STRING buffer
 *     CARRAY  : payload ascii bytes sent as CARRAY (fixed-length telegrams)
 *     CARRAYX : payload is hex string, decoded to binary CARRAY
 *     FML32   : payload = name=value;name=value;...  (FLDTBLDIR32/FIELDTBLS32 required)
 *
 * Build (AIX, 64bit):  see Makefile  (buildclient -w ... -lm)
 * Run:   ./rush_test -c rush.ini
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <time.h>
#include <math.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <atmi.h>
#include <fml32.h>

#ifndef MAP_ANON
#define MAP_ANON MAP_ANONYMOUS
#endif

#define MAX_TGT   16
#define MAX_JOB   32
#define MAX_WRK   512
#define HB        112          /* histogram buckets: 4 per power of 2 (us) */
#define MAXCD     4096
#define LINE_SZ   (1 << 20)
#define RBUF_SZ   65536

/* ------------------------------------------------------------ types */
typedef struct {
    char name[32], addr[160], user[32], cli[32], pw[32];
} target_t;

typedef struct {
    char   name[32], log[256], tgnames[256];
    int    ntg, tg[MAX_TGT];
    int    procs, ramp, dur, inflight, loop, timeout, skip;
    double tps;                /* total TPS of the job (split across procs) */
} job_t;

typedef struct {
    volatile uint64_t sent, ok, svcfail, timeout, err, builderr, badrec, lag, loops;
    volatile uint64_t hist[HB];
    volatile int      state;   /* 0 init, 1 ready, 2 done, 3 failed */
    pid_t             pid;
} wstat_t;

typedef struct { volatile int go, stop; } hdr_t;

typedef struct { int job, tgt, idx; } wk_t;

typedef struct {
    uint64_t sent, ok, sf, to, err;
    uint64_t h[HB];
} agg_t;

typedef struct { int used; uint64_t t0; } pend_t;

/* ------------------------------------------------------------ globals */
static target_t g_tgt[MAX_TGT];  static int g_ntgt;
static job_t    g_job[MAX_JOB];  static int g_njob;
static int      g_interval = 1;
static char     g_csv[256];
static wk_t     g_wk[MAX_WRK];   static int g_nw;
static hdr_t   *H;
static wstat_t *W;

/* worker-process locals */
static wstat_t *g_S;
static int      g_wi, g_consec, g_inflight, g_errshown;
static char    *g_rbuf;
static pend_t   g_pend[MAXCD];
static target_t *g_T;
static job_t    *g_J;

/* ------------------------------------------------------------ utils */
static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}
static void sleep_ns(uint64_t n)
{
    struct timespec ts;
    ts.tv_sec = n / 1000000000ULL; ts.tv_nsec = n % 1000000000ULL;
    nanosleep(&ts, NULL);
}
static int hb(uint64_t us)
{
    int b;
    if (us < 1) us = 1;
    b = (int)(log2((double)us) * 4.0);
    return b >= HB ? HB - 1 : b;
}
static double hb_us(int b) { return pow(2.0, (b + 0.5) / 4.0); }
static double pct(const uint64_t *h, uint64_t tot, double p)
{
    uint64_t need, c = 0; int i;
    if (!tot) return 0;
    need = (uint64_t)ceil((double)tot * p);
    for (i = 0; i < HB; i++) { c += h[i]; if (c >= need) return hb_us(i); }
    return hb_us(HB - 1);
}
static char *trim(char *s)
{
    char *e;
    while (*s == ' ' || *s == '\t') s++;
    e = s + strlen(s);
    while (e > s && (e[-1] == ' ' || e[-1] == '\t' || e[-1] == '\r' || e[-1] == '\n')) *--e = 0;
    return s;
}
static void scpy(char *d, const char *s, size_t n) { strncpy(d, s, n - 1); d[n - 1] = 0; }

/* ------------------------------------------------------------ config */
static int find_tgt(const char *n)
{
    int i;
    for (i = 0; i < g_ntgt; i++) if (!strcmp(g_tgt[i].name, n)) return i;
    return -1;
}

static int load_cfg(const char *fn)
{
    FILE *fp = fopen(fn, "r");
    char line[1024], *p, *k, *v, sec[64] = "";
    target_t *T = NULL; job_t *J = NULL;
    int j;
    if (!fp) { perror(fn); return -1; }
    while (fgets(line, sizeof line, fp)) {
        p = trim(line);
        if (!*p || *p == '#' || *p == ';') continue;
        if (*p == '[') {
            char *e = strchr(p, ']'); if (!e) continue; *e = 0;
            scpy(sec, p + 1, sizeof sec); T = NULL; J = NULL;
            if (!strncmp(sec, "target.", 7) && g_ntgt < MAX_TGT) {
                T = &g_tgt[g_ntgt++]; memset(T, 0, sizeof *T);
                scpy(T->name, sec + 7, sizeof T->name);
                scpy(T->user, "rush", sizeof T->user); scpy(T->cli, "rush", sizeof T->cli);
            } else if (!strncmp(sec, "job.", 4) && g_njob < MAX_JOB) {
                J = &g_job[g_njob++]; memset(J, 0, sizeof *J);
                scpy(J->name, sec + 4, sizeof J->name);
                J->procs = 1; J->timeout = 30; J->loop = 0;
            }
            continue;
        }
        k = p; v = strchr(p, '='); if (!v) continue; *v++ = 0; k = trim(k); v = trim(v);
        if (!strcmp(sec, "global")) {
            if (!strcmp(k, "stats_interval")) g_interval = atoi(v) > 0 ? atoi(v) : 1;
            else if (!strcmp(k, "csv")) scpy(g_csv, v, sizeof g_csv);
        } else if (T) {
            if (!strcmp(k, "addr")) scpy(T->addr, v, sizeof T->addr);
            else if (!strcmp(k, "user")) scpy(T->user, v, sizeof T->user);
            else if (!strcmp(k, "cltname")) scpy(T->cli, v, sizeof T->cli);
            else if (!strcmp(k, "passwd")) scpy(T->pw, v, sizeof T->pw);
        } else if (J) {
            if (!strcmp(k, "log")) scpy(J->log, v, sizeof J->log);
            else if (!strcmp(k, "targets")) scpy(J->tgnames, v, sizeof J->tgnames);
            else if (!strcmp(k, "procs")) J->procs = atoi(v);
            else if (!strcmp(k, "tps")) J->tps = atof(v);
            else if (!strcmp(k, "ramp_sec")) J->ramp = atoi(v);
            else if (!strcmp(k, "duration_sec")) J->dur = atoi(v);
            else if (!strcmp(k, "inflight")) J->inflight = atoi(v);
            else if (!strcmp(k, "loop")) J->loop = atoi(v);
            else if (!strcmp(k, "timeout_sec")) J->timeout = atoi(v);
            else if (!strcmp(k, "parse_skip")) J->skip = atoi(v);
        }
    }
    fclose(fp);
    /* resolve targets */
    for (j = 0; j < g_njob; j++) {
        char tmp[256], *tok, *sv = NULL;
        scpy(tmp, g_job[j].tgnames, sizeof tmp);
        for (tok = strtok_r(tmp, ", ", &sv); tok; tok = strtok_r(NULL, ", ", &sv)) {
            int t = find_tgt(tok);
            if (t < 0) { fprintf(stderr, "job.%s: unknown target '%s'\n", g_job[j].name, tok); return -1; }
            if (g_job[j].ntg < MAX_TGT) g_job[j].tg[g_job[j].ntg++] = t;
        }
        if (!g_job[j].ntg || !g_job[j].log[0] || g_job[j].procs < 1) {
            fprintf(stderr, "job.%s: log/targets/procs invalid\n", g_job[j].name); return -1;
        }
    }
    return g_njob ? 0 : -1;
}

/* ------------------------------------------------------------ worker: buffers */
static int hexv(int c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

static char *build_buf(const char *typ, char *pl, long *len)
{
    char *b = NULL;
    size_t n = strlen(pl);
    if (!strcmp(typ, "STRING")) {
        b = tpalloc("STRING", NULL, (long)n + 1);
        if (b) { memcpy(b, pl, n + 1); *len = (long)n + 1; }
    } else if (!strcmp(typ, "CARRAY")) {
        b = tpalloc("CARRAY", NULL, n ? (long)n : 1);
        if (b) { memcpy(b, pl, n); *len = (long)n; }
    } else if (!strcmp(typ, "CARRAYX")) {
        size_t i, m = n / 2;
        b = tpalloc("CARRAY", NULL, m ? (long)m : 1);
        if (!b) return NULL;
        for (i = 0; i < m; i++) {
            int h = hexv(pl[2 * i]), l = hexv(pl[2 * i + 1]);
            if (h < 0 || l < 0) { tpfree(b); return NULL; }
            b[i] = (char)((h << 4) | l);
        }
        *len = (long)m;
    } else if (!strcmp(typ, "FML32")) {
        char *sv = NULL, *tok, *cp = strdup(pl);
        FBFR32 *fb;
        if (!cp) return NULL;
        fb = (FBFR32 *)tpalloc("FML32", NULL, 4096 + (long)n * 3);
        if (!fb) { free(cp); return NULL; }
        for (tok = strtok_r(cp, ";", &sv); tok; tok = strtok_r(NULL, ";", &sv)) {
            char *eq = strchr(tok, '='); FLDID32 id;
            if (!eq) continue;
            *eq++ = 0;
            id = Fldid32(trim(tok));
            if (id == BADFLDID32) continue;
            if (CFadd32(fb, id, eq, 0, FLD_STRING) == -1) { tpfree((char *)fb); free(cp); return NULL; }
        }
        free(cp);
        b = (char *)fb; *len = 0;
    }
    return b;
}

/* "[skip fields]|SERVICE|TYPE|PAYLOAD" ; returns 0 ok */
static int parse_rec(char *line, int skip, char **svc, char **typ, char **pl)
{
    char *p = line;
    size_t n = strlen(line);
    while (n && (line[n - 1] == '\n' || line[n - 1] == '\r')) line[--n] = 0;
    while (skip-- > 0) { p = strchr(p, '|'); if (!p) return -1; p++; }
    *svc = p; p = strchr(p, '|'); if (!p) return -1; *p++ = 0;
    *typ = p; p = strchr(p, '|'); if (!p) return -1; *p++ = 0;
    *pl = p;
    return *svc[0] ? 0 : -1;
}

/* ------------------------------------------------------------ worker: tuxedo */
static int tux_connect(void)
{
    static char env[256];
    TPINIT *ti; int rc;
    snprintf(env, sizeof env, "WSNADDR=%s", g_T->addr);
    tuxputenv(env);
    ti = (TPINIT *)tpalloc("TPINIT", NULL, TPINITNEED(0));
    if (!ti) return -1;
    scpy(ti->usrname, g_T->user, sizeof ti->usrname);
    scpy(ti->cltname, g_T->cli, sizeof ti->cltname);
    scpy(ti->passwd, g_T->pw, sizeof ti->passwd);
    ti->flags = 0; ti->datalen = 0;
    rc = tpinit(ti);
    tpfree((char *)ti);
    if (rc == -1) return -1;
    tpsblktime(g_J->timeout, TPBLK_ALL);
    return 0;
}

static void account(int rc, int e, uint64_t us, const char *svc)
{
    wstat_t *S = g_S;
    if (rc != -1) { S->ok++; S->hist[hb(us)]++; g_consec = 0; }
    else if (e == TPESVCFAIL) { S->svcfail++; S->hist[hb(us)]++; g_consec = 0; }
    else if (e == TPETIME) { S->timeout++; }
    else {
        S->err++; g_consec++;
        if (g_errshown < 5) {
            g_errshown++;
            fprintf(stderr, "[w%d %s->%s] %s: %s\n", g_wi, g_J->name, g_T->name, svc, tpstrerror(e));
        }
    }
}

/* collect async replies; block=1 waits for exactly one */
static void reap(int block)
{
    while (g_inflight > 0) {
        int cd = 0, rc, e; long rl = 0; uint64_t us = 0;
        rc = tpgetrply(&cd, &g_rbuf, &rl, TPGETANY | (block ? 0 : TPNOBLOCK));
        e = tperrno;
        if (rc == -1 && e == TPEBLOCK) return;
        if (cd > 0 && cd < MAXCD && g_pend[cd].used) {
            us = (now_ns() - g_pend[cd].t0) / 1000;
            g_pend[cd].used = 0; g_inflight--;
        } else if (rc == -1 && e == TPETIME) {
            /* cd unknown on timeout: retire the oldest outstanding call */
            int i, o = -1; uint64_t m = ~0ULL;
            for (i = 1; i < MAXCD; i++) if (g_pend[i].used && g_pend[i].t0 < m) { m = g_pend[i].t0; o = i; }
            if (o > 0) { g_pend[o].used = 0; g_inflight--; }
        } else if (rc == -1) {
            memset(g_pend, 0, sizeof g_pend); g_inflight = 0;
        } else {
            g_inflight--;
        }
        account(rc, e, us, "(async)");
        if (block) return;
    }
}

static void reconnect(void)
{
    fprintf(stderr, "[w%d] reconnecting to %s\n", g_wi, g_T->addr);
    memset(g_pend, 0, sizeof g_pend); g_inflight = 0;
    tpterm(); sleep_ns(1000000000ULL);
    if (tux_connect() == -1)
        fprintf(stderr, "[w%d] reconnect failed: %s\n", g_wi, tpstrerror(tperrno));
    g_consec = 0;
}

static void seek_range(FILE *fp, off_t beg, char *line)
{
    if (beg > 0) { fseeko(fp, beg - 1, SEEK_SET); if (!fgets(line, LINE_SZ, fp)) return; }
    else fseeko(fp, 0, SEEK_SET);
}

static void worker(int wi, job_t *J, target_t *T, int idx, wstat_t *S)
{
    FILE *fp; off_t sz, beg, end; char *line;
    uint64_t t0, tend, next = 0;
    double tps_w; int async = J->inflight > 0, pass_recs = 0;

    signal(SIGINT, SIG_IGN);
    g_S = S; g_wi = wi; g_T = T; g_J = J; S->pid = getpid();

    line = (char *)malloc(LINE_SZ);
    fp = fopen(J->log, "r");
    if (!fp || !line) { fprintf(stderr, "[w%d] cannot open %s\n", wi, J->log); S->state = 3; return; }
    if (tux_connect() == -1) {
        fprintf(stderr, "[w%d] tpinit(%s) failed: %s\n", wi, T->addr, tpstrerror(tperrno));
        S->state = 3; return;
    }
    g_rbuf = tpalloc("CARRAY", NULL, RBUF_SZ);

    fseeko(fp, 0, SEEK_END); sz = ftello(fp);
    beg = (sz * idx) / J->procs;
    end = (idx == J->procs - 1) ? sz : (sz * (idx + 1)) / J->procs;
    seek_range(fp, beg, line);
    tps_w = J->tps / J->procs;

    S->state = 1;
    while (!H->go && !H->stop) sleep_ns(10000000ULL);
    t0 = now_ns(); next = t0;
    tend = J->dur > 0 ? t0 + (uint64_t)J->dur * 1000000000ULL : 0;

    while (!H->stop) {
        char *svc, *typ, *pl, *ib; long ilen = 0; uint64_t now;

        now = now_ns();
        if (tend && now >= tend) break;

        if (ftello(fp) >= end || !fgets(line, LINE_SZ, fp)) {
            if (!J->loop || !pass_recs) break;
            seek_range(fp, beg, line); S->loops++; pass_recs = 0;
            continue;
        }
        pass_recs++;
        if (parse_rec(line, J->skip, &svc, &typ, &pl) == -1) { S->badrec++; continue; }

        /* pacing (token interval with ramp-up) */
        if (tps_w > 0) {
            double f = 1.0;
            if (J->ramp > 0) {
                f = (double)(now - t0) / ((double)J->ramp * 1e9);
                if (f > 1.0) f = 1.0;
                if (f < 0.02) f = 0.02;
            }
            next += (uint64_t)(1e9 / (tps_w * f));
            for (;;) {
                uint64_t d;
                now = now_ns();
                if (now >= next || H->stop) break;
                if (async) reap(0);
                d = next - now; sleep_ns(d > 200000ULL ? 200000ULL : d);
            }
            if (now > next + 1000000000ULL) { next = now; S->lag++; }   /* no catch-up burst */
        }
        if (H->stop) break;

        ib = build_buf(typ, pl, &ilen);
        if (!ib) { S->builderr++; continue; }

        if (!async) {
            uint64_t t1 = now_ns(); int rc; long rl = 0;
            rc = tpcall(svc, ib, ilen, &g_rbuf, &rl, 0);
            S->sent++;
            account(rc, tperrno, (now_ns() - t1) / 1000, svc);
            tpfree(ib);
        } else {
            int cd; uint64_t t1;
            while (g_inflight >= J->inflight) reap(1);
            t1 = now_ns();
            cd = tpacall(svc, ib, ilen, 0);
            tpfree(ib);
            S->sent++;
            if (cd == -1) account(-1, tperrno, 0, svc);
            else if (cd > 0 && cd < MAXCD) { g_pend[cd].used = 1; g_pend[cd].t0 = t1; g_inflight++; }
            reap(0);
        }
        if (g_consec >= 20) reconnect();
    }

    if (async) {                      /* drain */
        uint64_t dl = now_ns() + (uint64_t)(J->timeout + 5) * 1000000000ULL;
        while (g_inflight > 0 && now_ns() < dl) reap(1);
    }
    tpterm();
    S->state = 2;
}

/* ------------------------------------------------------------ master */
static void on_int(int s) { (void)s; if (H) H->stop = 1; }

static void agg_job(int j, int tgt, agg_t *a)
{
    int i, b;
    memset(a, 0, sizeof *a);
    for (i = 0; i < g_nw; i++) {
        wstat_t *s = &W[i];
        if (j >= 0 && g_wk[i].job != j) continue;
        if (tgt >= 0 && g_wk[i].tgt != tgt) continue;
        a->sent += s->sent; a->ok += s->ok; a->sf += s->svcfail; a->to += s->timeout; a->err += s->err;
        for (b = 0; b < HB; b++) a->h[b] += s->hist[b];
    }
}

static uint64_t hsum(const uint64_t *h) { uint64_t t = 0; int i; for (i = 0; i < HB; i++) t += h[i]; return t; }

int main(int argc, char **argv)
{
    const char *cfg = "rush.ini";
    int c, i, j, k;
    size_t shm_sz;
    void *mem;
    agg_t prev[MAX_JOB], cur;
    FILE *csv = NULL;
    uint64_t T0, tick;

    while ((c = getopt(argc, argv, "c:")) != -1) if (c == 'c') cfg = optarg;
    if (load_cfg(cfg) == -1) { fprintf(stderr, "config error\n"); return 1; }

    for (j = 0; j < g_njob; j++)
        for (i = 0; i < g_job[j].procs && g_nw < MAX_WRK; i++) {
            g_wk[g_nw].job = j; g_wk[g_nw].idx = i;
            g_wk[g_nw].tgt = g_job[j].tg[i % g_job[j].ntg];
            g_nw++;
        }

    shm_sz = 4096 + sizeof(wstat_t) * (size_t)g_nw;
    mem = mmap(NULL, shm_sz, PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED, -1, 0);
    if (mem == MAP_FAILED) { perror("mmap"); return 1; }
    memset(mem, 0, shm_sz);
    H = (hdr_t *)mem; W = (wstat_t *)((char *)mem + 4096);
    signal(SIGINT, on_int); signal(SIGTERM, on_int);

    printf("rush_test: %d jobs, %d workers\n", g_njob, g_nw);
    for (j = 0; j < g_njob; j++)
        printf("  job.%s log=%s procs=%d tps=%.0f ramp=%ds dur=%ds inflight=%d loop=%d\n",
               g_job[j].name, g_job[j].log, g_job[j].procs, g_job[j].tps,
               g_job[j].ramp, g_job[j].dur, g_job[j].inflight, g_job[j].loop);

    fflush(stdout);
    for (i = 0; i < g_nw; i++) {
        pid_t p = fork();
        if (p == 0) {
            worker(i, &g_job[g_wk[i].job], &g_tgt[g_wk[i].tgt], g_wk[i].idx, &W[i]);
            _exit(W[i].state == 2 ? 0 : 1);
        }
        if (p < 0) { perror("fork"); H->stop = 1; break; }
    }

    /* start barrier: wait until every worker is connected (or failed) */
    for (k = 0; k < 1200 && !H->stop; k++) {
        int rdy = 0;
        for (i = 0; i < g_nw; i++) if (W[i].state >= 1) rdy++;
        if (rdy == g_nw) break;
        sleep_ns(100000000ULL);
    }
    { int ok = 0; for (i = 0; i < g_nw; i++) if (W[i].state == 1) ok++;
      printf("connected workers: %d/%d -> GO\n", ok, g_nw); fflush(stdout); }

    if (g_csv[0]) {
        csv = fopen(g_csv, "w");
        if (csv) fprintf(csv, "sec,job,sent,ok,svcfail,timeout,err,p50_us,p95_us,p99_us\n");
    }
    memset(prev, 0, sizeof prev);
    H->go = 1; T0 = now_ns(); tick = T0;

    for (;;) {
        int alive = 0;
        sleep_ns(100000000ULL);
        for (i = 0; i < g_nw; i++) if (W[i].state < 2) alive++;
        if (now_ns() - tick >= (uint64_t)g_interval * 1000000000ULL || !alive) {
            double el = (double)(now_ns() - T0) / 1e9;
            double dt = (double)(now_ns() - tick) / 1e9;
            tick = now_ns();
            for (j = 0; j < g_njob; j++) {
                uint64_t dh[HB], tot; int b;
                agg_job(j, -1, &cur);
                for (b = 0; b < HB; b++) dh[b] = cur.h[b] - prev[j].h[b];
                tot = hsum(dh);
                printf("[%6.0fs] %-8s sent/s=%-7.0f ok/s=%-7.0f svcfail=%-5llu timeout=%-5llu err=%-5llu "
                       "p50=%.2fms p95=%.2fms p99=%.2fms\n",
                       el, g_job[j].name, (double)(cur.sent - prev[j].sent) / dt,
                       (double)(cur.ok - prev[j].ok) / dt,
                       (unsigned long long)(cur.sf - prev[j].sf),
                       (unsigned long long)(cur.to - prev[j].to),
                       (unsigned long long)(cur.err - prev[j].err),
                       pct(dh, tot, .50) / 1000, pct(dh, tot, .95) / 1000, pct(dh, tot, .99) / 1000);
                if (csv)
                    fprintf(csv, "%.0f,%s,%llu,%llu,%llu,%llu,%llu,%.0f,%.0f,%.0f\n", el, g_job[j].name,
                            (unsigned long long)(cur.sent - prev[j].sent), (unsigned long long)(cur.ok - prev[j].ok),
                            (unsigned long long)(cur.sf - prev[j].sf), (unsigned long long)(cur.to - prev[j].to),
                            (unsigned long long)(cur.err - prev[j].err),
                            pct(dh, tot, .50), pct(dh, tot, .95), pct(dh, tot, .99));
                prev[j] = cur;
            }
            fflush(stdout); if (csv) fflush(csv);
        }
        if (!alive) break;
    }
    while (waitpid(-1, NULL, 0) > 0) ;

    /* final summary */
    {
        double el = (double)(now_ns() - T0) / 1e9;
        printf("\n==== SUMMARY (%.1fs) ====\n", el);
        for (j = 0; j < g_njob; j++) {
            agg_job(j, -1, &cur);
            printf("job %-8s sent=%llu ok=%llu svcfail=%llu timeout=%llu err=%llu avgTPS=%.1f "
                   "p50=%.2fms p95=%.2fms p99=%.2fms\n", g_job[j].name,
                   (unsigned long long)cur.sent, (unsigned long long)cur.ok, (unsigned long long)cur.sf,
                   (unsigned long long)cur.to, (unsigned long long)cur.err, (double)cur.sent / el,
                   pct(cur.h, hsum(cur.h), .50) / 1000, pct(cur.h, hsum(cur.h), .95) / 1000,
                   pct(cur.h, hsum(cur.h), .99) / 1000);
        }
        for (k = 0; k < g_ntgt; k++) {
            agg_job(-1, k, &cur);
            if (!cur.sent) continue;
            printf("target %-8s sent=%llu ok=%llu svcfail=%llu timeout=%llu err=%llu p99=%.2fms\n",
                   g_tgt[k].name, (unsigned long long)cur.sent, (unsigned long long)cur.ok,
                   (unsigned long long)cur.sf, (unsigned long long)cur.to, (unsigned long long)cur.err,
                   pct(cur.h, hsum(cur.h), .99) / 1000);
        }
        for (i = 0; i < g_nw; i++)
            if (W[i].state == 3) printf("worker %d (job.%s) FAILED to start\n", i, g_job[g_wk[i].job].name);
    }
    if (csv) fclose(csv);
    return 0;
}