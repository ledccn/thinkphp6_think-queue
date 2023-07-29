<?php

namespace Ledc\ThinkQueue;

use RuntimeException;
use think\facade\Queue;
use think\queue\Job;

/**
 * 任务构建器
 * @link https://github.com/top-think/think-queue
 */
trait Builder
{
    /**
     * topthink/think-queue默认执行的方法
     * @param Job $job 任务对象
     * @param mixed $data 消息内容
     * @return void
     */
    abstract public function fire(Job $job, mixed $data): void;

    /**
     * 任务名与消费者类的映射关系
     * @return array
     */
    public static function consumerMaps(): array
    {
        // 示例
        return [
            // key为任务名，值为数组（索引0 类名，索引1 方法名）
            '' => [static::class, 'fire'],
        ];
    }

    /**
     * 发布任务到队列内
     * - 消费者为当前类的fire方法
     * @param mixed $data 消息内容
     * @param int $delay 延时时间
     * @param string|null $queue 队列名称
     * @return void
     */
    final public static function queue(mixed $data, int $delay = 0, string $queue = null): void
    {
        $job = static::class;
        if ($delay > 0) {
            Queue::later($delay, $job, $data, $queue);
        } else {
            Queue::push($job, $data, $queue);
        }
    }

    /**
     * 发布任务到队列内
     * @param string $name 任务名
     * @param mixed $data 消息内容
     * @param int $delay 延时时间
     * @param string|null $queue 队列名称
     * @return void
     */
    final public static function send(string $name, mixed $data, int $delay = 0, string $queue = null): void
    {
        $job = self::getJob($name);
        if ($delay > 0) {
            Queue::later($delay, $job, $data, $queue);
        } else {
            Queue::push($job, $data, $queue);
        }
    }

    /**
     * 获取消费者类
     * @param string $name
     * @return string
     */
    private static function getJob(string $name): string
    {
        $maps = static::consumerMaps();
        $callable = $maps[$name] ?? null;
        if (empty($callable)) {
            throw new RuntimeException('未找到任务名' . $name . '的消费者');
        }

        if (!is_array($callable) || 2 !== count($callable)) {
            throw new RuntimeException('参数callable错误');
        }

        list($class, $action) = $callable;
        if (static::class !== $class) {
            throw new RuntimeException('消费者只能为当前类');
        }
        if (!method_exists($class, $action)) {
            throw new RuntimeException($class . '不存在方法 ' . $action);
        }

        return $class . '@' . $action;
    }
}
