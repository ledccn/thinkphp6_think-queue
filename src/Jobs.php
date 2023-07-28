<?php

namespace Ledc\ThinkQueue;

use RuntimeException;
use think\facade\Log;
use think\facade\Queue;
use think\queue\Job;
use Throwable;

/**
 * 对topthink/think-queue队列的封装
 */
trait Jobs
{
    /**
     * 重试间隔
     * @var int
     */
    protected static int $retry_seconds = 5;

    /**
     * 抽象方法
     * - 返回true，表示任务执行成功，会删除当前任务
     * - 抛出异常时，会根据attempts参数，决定是重试还是删除任务
     * @return bool|null
     */
    abstract public function execute(): ?bool;

    /**
     * topthink/think-queue默认执行的方法
     * @param Job $job
     * @param mixed $payload
     * @return void
     */
    final public function fire(Job $job, mixed $payload): void
    {
        $jobs = $payload['job'] ?? '';
        $data = $payload['args'] ?? null;
        $constructor = $payload['constructor'] ?? [];
        $attempts = $payload['attempts'] ?? 0;
        if (empty($jobs)) {
            return;
        }

        try {
            list($class, $method) = self::parseJob($jobs);
            $instance = $constructor ? (new $class(... array_values($constructor))) : (new $class);
            if (method_exists($instance, $method)) {
                if ($data && is_array($data)) {
                    // 非空数组
                    $result = $instance->{$method}(... array_values($data));
                } else {
                    // null/int/bool/string/空数组
                    $result = $instance->{$method}($data);
                }
                if (true === $result || !$attempts) {
                    $job->delete();
                    return;
                }
            } else {
                $job->delete();
            }
        } catch (Throwable $throwable) {
            Log::error('think-queue执行异常' . $throwable->getMessage());
        }

        if ($job->attempts() >= $attempts) {
            $job->delete();
        } else {
            $retry_seconds = max(1, static::$retry_seconds);
            $job->release($job->attempts() * $retry_seconds);
        }
    }

    /**
     * 解析类名与方法名
     * @param string $job
     * @return array
     */
    private static function parseJob(string $job): array
    {
        $segments = explode('@', $job);

        return 2 === count($segments) ? $segments : [$segments[0], 'execute'];
    }

    /**
     * 调度任务
     * - 默认执行当前类的execute方法
     * @param mixed $args 参数
     * @param int $delay 延时时间
     * @param int $attempts 重试次数
     * @param string|null $queue 队列名称
     * @return void
     */
    final public static function dispatch(mixed $args, int $delay = 0, int $attempts = 0, string $queue = null): void
    {
        $payload = [
            'job' => static::class . '@execute',
            'args' => $args,
            'attempts' => max(0, $attempts),
        ];
        if ($delay > 0) {
            Queue::later($delay, static::class, $payload, $queue);
        } else {
            Queue::push(static::class, $payload, $queue);
        }
    }

    /**
     * 调度任务
     * - 可以执行任意类方法
     * @param array $callable 可调用数组
     * @param mixed $args 参数
     * @param int $delay 延时时间
     * @param int $attempts 重试次数
     * @param string|null $queue 队列名称
     * @param array $constructor 构造函数参数
     * @return void
     */
    final public static function emit(array $callable, mixed $args = null, int $delay = 0, int $attempts = 0, string $queue = null, array $constructor = []): void
    {
        if (2 !== count($callable)) {
            throw new RuntimeException('参数callable错误');
        }
        list($class, $action) = $callable;
        if (!method_exists($class, $action)) {
            throw new RuntimeException($class . '不存在方法 ' . $action);
        }

        $payload = [
            'job' => $class . '@' . $action,
            'args' => $args,
            'attempts' => max(0, $attempts),
            'constructor' => $constructor,
        ];
        if ($delay > 0) {
            Queue::later($delay, static::class, $payload, $queue);
        } else {
            Queue::push(static::class, $payload, $queue);
        }
    }
}
