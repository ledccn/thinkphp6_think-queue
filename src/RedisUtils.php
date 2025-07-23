<?php

namespace Ledc\ThinkQueue;

use Predis\Client;
use Redis;
use RuntimeException;
use think\exception\ValidateException;
use think\facade\Cache;
use Throwable;

/**
 * Redis工具类
 */
class RedisUtils
{
    /**
     * Redis的setNx指令，支持设置ttl
     * @param string $key 缓存的key
     * @param string $value 缓存的值
     * @param int $ttl 缓存的过期时间,单位秒
     * @return bool
     */
    final public static function setNx(string $key, string $value, int $ttl = 10): bool
    {
        try {
            $result = self::handler()->set($key, $value, ['NX', 'EX' => $ttl]);
            return $result !== false;
        } catch (Throwable $throwable) {
            throw new RuntimeException($throwable->getMessage());
        }
    }

    /**
     * 自增
     * @param string $key
     * @param int $ttl 缓存的过期时间,单位秒
     * @return int
     */
    final public static function incr(string $key, int $ttl = 10): int
    {
        try {
            $script = <<<LUA
if redis.call('set', KEYS[1], ARGV[1], "EX", ARGV[2], "NX") then
    return ARGV[1]
else
    return redis.call('incr', KEYS[1])
end
LUA;
            $result = self::handler()->eval($script, [$key, 1, $ttl], 1);
            return (int)$result;
        } catch (Throwable $exception) {
            throw new RuntimeException($exception->getMessage());
        }
    }

    /**
     * 限流
     * - Redis Lua脚本实现的限流器，超过限制次数则抛出异常
     * @param string $key 限制资源：KEY
     * @param int $limit 访问限制次数
     * @param int $window_time 窗口时间,单位秒
     * @return int 已访问次数
     */
    final public static function rateLimiter(string $key, int $limit, int $window_time = 10): int
    {
        try {
            $script = <<<LUA
local current = redis.call('incr', KEYS[1])
if current == 1 then
    redis.call('expire', KEYS[1], ARGV[1])
end
if current > tonumber(ARGV[2]) then
    return 0
else
    return current
end
LUA;
            $result = self::handler()->eval($script, [$key, $window_time, $limit], 1);
            if (0 === (int)$result) {
                throw new ValidateException('访问次数过多');
            }

            return (int)$result;
        } catch (ValidateException $exception) {
            throw $exception;
        } catch (Throwable $exception) {
            throw new RuntimeException($exception->getMessage());
        }
    }

    /**
     * 获取redis句柄
     * @return Client|Redis
     */
    final public static function handler()
    {
        return Cache::store('redis')->handler();
    }
}
