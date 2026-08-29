package com.nl2sql.gateway.ratelimit;

import com.nl2sql.gateway.task.IntegrationTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.LocalDate;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** 真 Redis 集成：Lua 原子性是核心被测对象。 */
class RateLimiterServiceTest extends IntegrationTestBase {

    @Autowired
    RateLimiterService limiter;

    @Autowired
    QuotaService quota;

    @Test
    void allowUserPassesExactlyLimitThenRejects() {
        String tag = "rl-unit-01";
        assertThat(limiter.allowUser(tag, 3)).isTrue();
        assertThat(limiter.allowUser(tag, 3)).isTrue();
        assertThat(limiter.allowUser(tag, 3)).isTrue();
        assertThat(limiter.allowUser(tag, 3)).isFalse();
    }

    @Test
    void concurrent50ThreadsLimit10AdmitsExactly10() throws Exception {
        String tag = "rl-conc-01";
        int threads = 50;
        int limit = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger admitted = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    startGate.await();
                    if (limiter.allowUser(tag, limit)) {
                        admitted.incrementAndGet();
                    }
                } catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        startGate.countDown();
        assertThat(done.await(15, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();

        // Lua 原子性：并发下不多不少恰好放行 limit 个
        assertThat(admitted.get()).isEqualTo(limit);
    }

    @Test
    void tryConsumeCountsAgainstDailyQuotaAndSetsTtl() {
        String tag = "quota-unit-01";
        String key = "quota:" + tag + ":" + LocalDate.now().toString().replace("-", "");

        assertThat(quota.tryConsume(tag, 2)).isTrue();
        assertThat(quota.tryConsume(tag, 2)).isTrue();
        assertThat(quota.tryConsume(tag, 2)).isFalse();

        Long ttl = redisTemplate.getExpire(key, TimeUnit.SECONDS);
        assertThat(ttl).isNotNull().isPositive();
        assertThat(ttl).isLessThanOrEqualTo(Duration.ofHours(48).toSeconds());
    }

    @Test
    void globalLimiterSharesWindowAcrossCallers() {
        // 全局键与用户键隔离：清掉全局窗口后独立验证
        redisTemplate.delete("rl:__global__");
        int passed = 0;
        for (int i = 0; i < 5; i++) {
            if (limiter.allowGlobal(3)) {
                passed++;
            }
        }
        assertThat(passed).isEqualTo(3);
    }
}
