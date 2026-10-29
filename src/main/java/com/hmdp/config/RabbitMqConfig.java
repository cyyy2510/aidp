package com.hmdp.config;

import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class RabbitMqConfig {
    @Bean
    public MessageConverter messageConverter() {
        // 使用Jackson2JsonMessageConverter注入MessageConverter作为消息转换器
        Jackson2JsonMessageConverter jjmc = new Jackson2JsonMessageConverter();
        // 2.配置自动创建消息id，用于识别不同消息，也可以在业务中基于ID判断是否是重复消息
        jjmc.setCreateMessageIds(true);
        return jjmc;
    }

    // 定义错误队列，交换机 和队列 绑定关系
    @Bean
    public DirectExchange directExchange() {
        return new DirectExchange("error.direct");
    }

    @Bean
    public Queue errorQueue() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-queue-mode", "lazy"); // 设置为 Lazy 队列
        return new Queue("error.queue", true, false, false, args);
    }

    @Bean
    public Binding binding(DirectExchange directExchange, Queue errorQueue) {
        return BindingBuilder.bind(errorQueue).to(directExchange).with("error");// 关键字RouteKey为error
    }

    /**
     * -  RejectAndDontRequeueRecoverer：重试耗尽后，直接reject，丢弃消息。默认就是这种方式
     * -  ImmediateRequeueMessageRecoverer：重试耗尽后，返回nack，消息重新入队
     * -  RepublishMessageRecoverer：重试耗尽后，将失败消息投递到指定的交换机
     *
     * @param
     * @return
     */
    // 替代原来的失败处理策略
    @Bean
    public MessageRecoverer messageRecoverer(RabbitTemplate rabbitTemplate) {
        return new RepublishMessageRecoverer(rabbitTemplate, "error.direct", "error");
    }
}
