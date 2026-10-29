package com.hmdp.listener;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SpringRabbitListener {
    private final VoucherOrderServiceImpl voucherOrderService;

    /**
     * 消息队列测试
     * @param msg
     * @throws InterruptedException
     */
    // 利用RabbitListener来声明要监听的队列信息
    // 将来一旦监听的队列中有了消息，就会推送给当前服务，调用当前方法，处理消息。
    // 可以看到方法体中接收的就是消息体的内容
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "simple.queue", durable = "true"),
            exchange = @Exchange(name = "simple.direct", type = ExchangeTypes.DIRECT),
            key = {"test"}
    ))
    public void listenSimpleQueueMessage(String msg) {
        System.out.println("spring 消费者接收到消息：【" + msg + "】");
    }

    /**
     * 监听direct.queue1消息队列，如果不存在
     * 就创建相关交换机，队列和绑定关系，然后进行消费
     * 配置文件里开启了prefetch = 1 线程会抢占式获取消息队列信息
     * @param voucherOrder 优惠券订单信息
     */
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "direct.queue1",
                    durable = "true",
                    arguments = @Argument(name = "x-queue-mode", value = "lazy")),// 设置消息队列默认为持久化和开启lazy队列模式（消息会直接存入磁盘，避免内存爆仓。）
            exchange = @Exchange(name = "seckill.direct", type = ExchangeTypes.DIRECT),//设置交换机名称和类型，默认为DIRECT类型与自动持久化
            key = {"seckill.order"} //设定RoutingKey
    ),
            concurrency = "1-10" // 启动动态线程池（最低1个，最多10个）并发消费
    )
    public void receiveMessage(VoucherOrder voucherOrder, Message message) {
        log.debug("接收到的消息 ID:{} ",message.getMessageProperties().getMessageId());
        log.debug("线程: {} - \n收到优惠券订单消息：{}",Thread.currentThread().getName(), voucherOrder);
        voucherOrderService.handleVoucherOrder(voucherOrder);
    }
}
