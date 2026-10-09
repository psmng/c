/*
 * dscd.c
 *
 * 디스코드 웹훅(Webhook)을 통해 텍스트 메시지를 전송하는 프로그램
 *
 * 빌드:
 *   gcc -o dscd dscd.c -lcurl
 *
 * 실행:
 *   ./dscd "<WEBHOOK_URL>" "보낼 메시지 내용"
 *
 * 사전 준비:
 *   디스코드 채널 설정 > 연동 > 웹훅 에서 웹훅 URL을 미리 생성해두어야 합니다.
 *   (https://discord.com/api/webhooks/xxxxx/yyyyy 형태)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <curl/curl.h>

/* libcurl이 서버 응답 바디를 받을 때 호출하는 콜백 (응답 내용은 버림) */
static size_t discard_response(ptr, size, nmemb, userdata)
void *ptr;
size_t size;
size_t nmemb;
void *userdata;
{
    (void)ptr;
    (void)userdata;
    return size * nmemb;
}

/*
 * JSON 문자열 내에서 이스케이프가 필요한 문자(", \, 개행)를 처리한다.
 * 매우 단순한 구현이며, 일반적인 텍스트 메시지 전송 용도로 충분하다.
 */
static char *json_escape(input)
const char *input;
{
    size_t len = strlen(input);
    /* 최악의 경우 모든 문자가 2바이트로 이스케이프될 수 있으므로 넉넉히 할당 */
    char *out = malloc(len * 2 + 1);
    if (!out)
    {
        fprintf(stderr, "메모리 할당 실패\n");
        exit(EXIT_FAILURE);
    }

    size_t j = 0;
    for (size_t i = 0; i < len; i++)
    {
        unsigned char c = (unsigned char)input[i];
        switch (c)
        {
            case '"':  out[j++] = '\\'; out[j++] = '"';  break;
            case '\\': out[j++] = '\\'; out[j++] = '\\'; break;
            case '\n': out[j++] = '\\'; out[j++] = 'n';  break;
            case '\r': out[j++] = '\\'; out[j++] = 'r';  break;
            case '\t': out[j++] = '\\'; out[j++] = 't';  break;
            default:
                out[j++] = (char)c;
                break;
        }
    }
    out[j] = '\0';

    return (out);
}

int main(argc, argv)
int argc;
char *argv[];
{
    if (argc != 3)
    {
        fprintf(stderr, "사용법: %s <WEBHOOK_URL> <메시지>\n", argv[0]);
        fprintf(stderr, "예시  : %s \"https://discord.com/api/webhooks/xxx/yyy\" \"서버 점검 완료\"\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char *webhook_url = argv[1];
    const char *message = argv[2];
    const int retry_count = 5; /* 전송 실패 시 재시도 횟수 */

    /* 메시지 JSON 이스케이프 처리 */
    char *escaped_message = json_escape(message);

    /* JSON 바디 구성: {"content": "메시지"} */
    size_t body_len = strlen(escaped_message) + 64;
    char *json_body = malloc(body_len);
    
    if (!json_body)
    {
        fprintf(stderr, "메모리 할당 실패\n");
        free(escaped_message);
        return EXIT_FAILURE;
    }
    snprintf(json_body, body_len, "{\"content\": \"%s\"}", escaped_message);

    curl_global_init(CURL_GLOBAL_DEFAULT);
    CURL *curl = curl_easy_init();
    
    if (!curl)
    {
        fprintf(stderr, "curl 초기화 실패\n");
        free(escaped_message);
        free(json_body);
        curl_global_cleanup();
        return EXIT_FAILURE;
    }

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");

    curl_easy_setopt(curl, CURLOPT_URL, webhook_url);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_body);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, discard_response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);
    /* 사내 망분리/프록시 환경 등에서 SSL 인증서 문제가 있다면 아래 옵션을 조정 (기본은 검증 유지 권장) */
    /* curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L); */

    CURLcode res = curl_easy_perform(curl);

    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    if (res != CURLE_OK)
    {
        fprintf(stderr, "전송 실패: %s\n", curl_easy_strerror(res));
    }
    else if (http_code == 204 || http_code == 200)
    {
        printf("메시지 전송 성공 (HTTP %ld)\n", http_code);
    }
    else
    {
        fprintf(stderr, "전송 실패 (HTTP %ld) - 웹훅 URL 또는 메시지 형식을 확인하세요.\n", http_code);

        if (retry_count > 0)
        {
            sleep(1); /* 잠시 대기 후 재시도 */
            fprintf(stderr, "재시도 중... (%d회 남음)\n", retry_count);
            for (int i = 0; i < retry_count; i++)
            {
                res = curl_easy_perform(curl);
                curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

                if (res == CURLE_OK && (http_code == 204 || http_code == 200))
                {
                    printf("메시지 전송 성공 (HTTP %ld) - 재시도 %d회\n", http_code, i + 1);
                    break;
                }
                else
                {
                    fprintf(stderr, "재시도 %d회 실패 (HTTP %ld)\n", i + 1, http_code);
                }
            }
        }

        res = CURLE_HTTP_RETURNED_ERROR;
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    curl_global_cleanup();
    free(escaped_message);
    free(json_body);

    return (res == CURLE_OK) ? EXIT_SUCCESS : EXIT_FAILURE;
}