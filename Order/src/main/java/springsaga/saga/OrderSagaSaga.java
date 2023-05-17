package springsaga.saga;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import springsaga.config.kafka.KafkaProcessor;
import springsaga.domain.*;
import springsaga.external.*;

@Service
public class OrderSagaSaga {

    @Autowired
    DeliveryService deliveryService;

    @Autowired
    StorageService storageService;

    @Autowired
    OrderService orderService;

    @Autowired
    FactoryService factoryService;

    @Autowired
    OverseasDeliveryService overseasDeliveryService;

    @StreamListener(
        value = KafkaProcessor.INPUT,
        condition = "headers['type']=='OrderPlaced'"
    )
    public void wheneverOrderPlaced_OrderSaga(
        @Payload OrderPlaced orderPlaced,
        @Header(KafkaHeaders.ACKNOWLEDGMENT) Acknowledgment acknowledgment,
        @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) byte[] messageKey
    ) {
        OrderPlaced event = orderPlaced;
        System.out.println(
            "\n\n##### listener OrderSaga : " + orderPlaced + "\n\n"
        );

        try {
            Delivery delivery = new Delivery();
            /* Logic */
            delivery.setOrderId(event.getId());

            deliveryService.startDelivery(delivery);
        } catch (Exception e) {
            OrderCancelCommand orderCancelCommand = new OrderCancelCommand();
            /* Logic */
            orderCancelCommand.setId(event.getId());

            orderService.orderCancel(event.getId(), orderCancelCommand);
        }

        // Manual Offset Commit //
        acknowledgment.acknowledge();
    }

    @StreamListener(
        value = KafkaProcessor.INPUT,
        condition = "headers['type']=='DeliveryStarted'"
    )
    public void wheneverDeliveryStarted_OrderSaga(
        @Payload DeliveryStarted deliveryStarted,
        @Header(KafkaHeaders.ACKNOWLEDGMENT) Acknowledgment acknowledgment,
        @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) byte[] messageKey
    ) {
        DeliveryStarted event = deliveryStarted;
        System.out.println(
            "\n\n##### listener OrderSaga : " + deliveryStarted + "\n\n"
        );

        try {
            DecreaseStockCommand decreaseStockCommand = new DecreaseStockCommand();
            /* Logic */
            decreaseStockCommand.setOrderId(event.getOrderId());

            storageService.decreaseStock(
                event.getOrderId(),
                decreaseStockCommand
            );
        } catch (Exception e) {
            CancelDeliveryCommand cancelDeliveryCommand = new CancelDeliveryCommand();
            /* Logic */
            cancelDeliveryCommand.setOrderId(event.getOrderId());

            deliveryService.cancelDelivery(
                event.getOrderId(),
                cancelDeliveryCommand
            );
        }

        // Manual Offset Commit //
        acknowledgment.acknowledge();
    }

    @StreamListener(
        value = KafkaProcessor.INPUT,
        condition = "headers['type']=='StockDecreased'"
    )
    public void wheneverStockDecreased_OrderSaga(
        @Payload StockDecreased stockDecreased,
        @Header(KafkaHeaders.ACKNOWLEDGMENT) Acknowledgment acknowledgment,
        @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) byte[] messageKey
    ) {
        StockDecreased event = stockDecreased;
        System.out.println(
            "\n\n##### listener OrderSaga : " + stockDecreased + "\n\n"
        );

        UpdateStatusCommand updateStatusCommand = new UpdateStatusCommand();
        /* Logic */
        updateStatusCommand.setId(event.getOrderId());

        orderService.updateStatus(event.getOrderId(), updateStatusCommand);

        // Manual Offset Commit //
        acknowledgment.acknowledge();
    }

    @StreamListener(
        value = KafkaProcessor.INPUT,
        condition = "headers['type']=='OrderCompleted'"
    )
    public void wheneverOrderCompleted_OrderSaga(
        @Payload OrderCompleted orderCompleted,
        @Header(KafkaHeaders.ACKNOWLEDGMENT) Acknowledgment acknowledgment,
        @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) byte[] messageKey
    ) {
        OrderCompleted event = orderCompleted;
        System.out.println(
            "\n\n##### listener OrderSaga : " + orderCompleted + "\n\n"
        );

        Factory factory = new Factory();
        /* Logic */
        factory.setOrderId(event.getId());
        factoryService.makeProduct(factory);

        // Manual Offset Commit //
        acknowledgment.acknowledge();
    }

    @StreamListener(
        value = KafkaProcessor.INPUT,
        condition = "headers['type']=='ProductManufactured'"
    )
    public void wheneverProductManufactured_OrderSaga(
        @Payload ProductManufactured productManufactured,
        @Header(KafkaHeaders.ACKNOWLEDGMENT) Acknowledgment acknowledgment,
        @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) byte[] messageKey
    ) {
        ProductManufactured event = productManufactured;
        System.out.println(
            "\n\n##### listener OrderSaga : " + productManufactured + "\n\n"
        );

        OverseasDelivery overseasDelivery = new OverseasDelivery();
        /* Logic */
        overseasDelivery.setOrderId(event.getOrderId());
        overseasDeliveryService.startOverseasDelivery(overseasDelivery);

        // Manual Offset Commit //
        acknowledgment.acknowledge();
    }

    @StreamListener(
        value = KafkaProcessor.INPUT,
        condition = "headers['type']=='OverseasDeliveryStarted'"
    )
    public void wheneverOverseasDeliveryStarted_OrderSaga(
        @Payload OverseasDeliveryStarted overseasDeliveryStarted,
        @Header(KafkaHeaders.ACKNOWLEDGMENT) Acknowledgment acknowledgment,
        @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) byte[] messageKey
    ) {
        OverseasDeliveryStarted event = overseasDeliveryStarted;
        System.out.println(
            "\n\n##### listener OrderSaga : " + overseasDeliveryStarted + "\n\n"
        );

        /* Logic */

        // Manual Offset Commit //
        acknowledgment.acknowledge();
    }
}
