/**
 * Copyright © 2016-2024 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.mqtt.broker.client.mqtt;

import com.google.common.collect.ImmutableSet;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.util.CharsetUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final class MqttChannelHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private final MqttClientImpl client;
    private final ReceivedMsgProcessor receivedMsgProcessor;
    private final ConnectCallback connectCallback;

    MqttChannelHandler(MqttClientImpl client, ConnectCallback connectCallback, ReceivedMsgProcessor receivedMsgProcessor) {
        this.client = client;
        this.receivedMsgProcessor = receivedMsgProcessor;
        this.connectCallback = connectCallback;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        switch (msg.fixedHeader().messageType()) {
            case CONNACK:
                handleConack(ctx.channel(), (MqttConnAckMessage) msg);
                break;
            case SUBACK:
                handleSubAck((MqttSubAckMessage) msg);
                break;
            case PUBLISH:
                handlePublish(ctx.channel(), (MqttPublishMessage) msg);
                break;
            case UNSUBACK:
                handleUnsuback((MqttUnsubAckMessage) msg);
                break;
            case PUBACK:
                handlePuback((MqttPubAckMessage) msg);
                break;
            case PUBREC:
                handlePubrec(ctx.channel(), msg);
                break;
            case PUBREL:
                handlePubrel(ctx.channel(), msg);
                break;
            case PUBCOMP:
                handlePubcomp(msg);
                break;
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);

        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader variableHeader = new MqttConnectVariableHeader(
                this.client.getClientConfig().getProtocolVersion().protocolName(),  // Protocol Name
                this.client.getClientConfig().getProtocolVersion().protocolLevel(), // Protocol Level
                this.client.getClientConfig().getUsername() != null,                // Has Username
                this.client.getClientConfig().getPassword() != null,                // Has Password
                this.client.getClientConfig().getLastWill() != null                 // Will Retain
                        && this.client.getClientConfig().getLastWill().isRetain(),
                this.client.getClientConfig().getLastWill() != null                 // Will QOS
                        ? this.client.getClientConfig().getLastWill().getQos().value()
                        : 0,
                this.client.getClientConfig().getLastWill() != null,                // Has Will
                this.client.getClientConfig().isCleanSession(),                     // Clean Session
                this.client.getClientConfig().getTimeoutSeconds()                   // Timeout
        );
        MqttConnectPayload payload = new MqttConnectPayload(
                this.client.getClientConfig().getClientId(),
                this.client.getClientConfig().getLastWill() != null ? this.client.getClientConfig().getLastWill().getTopic() : null,
                this.client.getClientConfig().getLastWill() != null ? this.client.getClientConfig().getLastWill().getMessage().getBytes(CharsetUtil.UTF_8) : null,
                this.client.getClientConfig().getUsername(),
                this.client.getClientConfig().getPassword() != null ? this.client.getClientConfig().getPassword().getBytes(CharsetUtil.UTF_8) : null
        );
        ctx.channel().writeAndFlush(new MqttConnectMessage(fixedHeader, variableHeader, payload));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    private void processIncomingMessage(MqttPublishMessage message) {
        receivedMsgProcessor.processIncomingMessage(message, this::invokeHandlers);
    }

    private void invokeHandlers(MqttPublishMessage message, long receivedTime) {
        boolean handlerInvoked = false;
        for (MqttSubscription subscription : ImmutableSet.copyOf(this.client.getSubscriptions().values())) {
            if (subscription.matches(message.variableHeader().topicName())) {
                if (subscription.isOnce() && subscription.isCalled()) {
                    continue;
                }
                message.payload().markReaderIndex();
                subscription.setCalled(true);
                subscription.getHandler().onMessage(message.variableHeader().topicName(), message.payload(), receivedTime);
                if (subscription.isOnce()) {
                    this.client.off(subscription.getTopic(), subscription.getHandler());
                }
                message.payload().resetReaderIndex();
                handlerInvoked = true;
            }
        }
        if (!handlerInvoked && client.getDefaultHandler() != null) {
            client.getDefaultHandler().onMessage(message.variableHeader().topicName(), message.payload(), receivedTime);
        }
    }

    private void handleConack(Channel channel, MqttConnAckMessage message) {
        switch (message.variableHeader().connectReturnCode()) {
            case CONNECTION_ACCEPTED:
                this.connectCallback.onSuccess(new MqttConnectResult(true, MqttConnectReturnCode.CONNECTION_ACCEPTED, channel.closeFuture()));

                this.client.getPendingSubscriptions().entrySet().stream().filter((e) -> !e.getValue().isSent()).forEach((e) -> {
                    channel.write(e.getValue().getSubscribeMessage());
                    e.getValue().setSent(true);
                });

                channel.flush();
                if (this.client.isReconnect()) {
                    this.client.onSuccessfulReconnect();
                }
                break;

            case CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD:
            case CONNECTION_REFUSED_IDENTIFIER_REJECTED:
            case CONNECTION_REFUSED_NOT_AUTHORIZED:
            case CONNECTION_REFUSED_SERVER_UNAVAILABLE:
            case CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION:
                this.connectCallback.onSuccess(new MqttConnectResult(false, message.variableHeader().connectReturnCode(), channel.closeFuture()));
                channel.close();
                // Don't start reconnect logic here
                break;
        }
    }

    private void handleSubAck(MqttSubAckMessage message) {
        MqttPendingSubscription pendingSubscription = this.client.getPendingSubscriptions().remove(message.variableHeader().messageId());
        if (pendingSubscription == null) {
            return;
        }
        pendingSubscription.onSubackReceived();
        for (MqttPendingSubscription.MqttPendingHandler handler : pendingSubscription.getHandlers()) {
            MqttSubscription subscription = new MqttSubscription(pendingSubscription.getTopic(), handler.getHandler(), handler.isOnce());
            this.client.getSubscriptions().put(pendingSubscription.getTopic(), subscription);
            this.client.getHandlerToSubscribtion().put(handler.getHandler(), subscription);
        }
        this.client.getPendingSubscribeTopics().remove(pendingSubscription.getTopic());

        this.client.getServerSubscriptions().add(pendingSubscription.getTopic());

        pendingSubscription.getCallback().onSuccess();
    }

    private void handlePublish(Channel channel, MqttPublishMessage message) {
        switch (message.fixedHeader().qosLevel()) {
            case AT_MOST_ONCE:
                processIncomingMessage(message);
                break;

            case AT_LEAST_ONCE:
                processIncomingMessage(message);
                if (message.variableHeader().packetId() != -1) {
                    MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
                    MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(message.variableHeader().packetId());
                    channel.writeAndFlush(new MqttPubAckMessage(fixedHeader, variableHeader));
                }
                break;

            case EXACTLY_ONCE:
                if (message.variableHeader().packetId() != -1) {
                    MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0);
                    MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(message.variableHeader().packetId());
                    MqttMessage pubrecMessage = new MqttMessage(fixedHeader, variableHeader);

                    MqttIncomingQos2Publish incomingQos2Publish = new MqttIncomingQos2Publish(message);

                    if (this.client.getQos2PendingIncomingPublishes().containsKey(message.variableHeader().packetId())) {
                        log.error("Duplicate PUBREC detected for packet ID {}. The packet is already in the QoS 2 pending incoming publishes map.", message.variableHeader().packetId());
                    }

                    this.client.getQos2PendingIncomingPublishes().put(message.variableHeader().packetId(), incomingQos2Publish);

                    channel.writeAndFlush(pubrecMessage);
                }
                break;
        }
    }

    private void handleUnsuback(MqttUnsubAckMessage message) {
        MqttPendingUnsubscription unsubscription = this.client.getPendingServerUnsubscribes().get(message.variableHeader().messageId());
        if (unsubscription == null) {
            return;
        }
        unsubscription.onUnsubackReceived();
        this.client.getServerSubscriptions().remove(unsubscription.getTopic());
        unsubscription.getFuture().setSuccess(null);
        this.client.getPendingServerUnsubscribes().remove(message.variableHeader().messageId());
    }

    private void handlePuback(MqttPubAckMessage message) {
        MqttPendingPublish pendingPublish = this.client.getPendingPublishes().remove(message.variableHeader().messageId());
        if (pendingPublish == null) {
            return;
        }
        // MQTT 5 marks a refused publish with a reason code of 0x80 or above (e.g. 0x97 QUOTA_EXCEEDED).
        // Treating those as successful acknowledgements would count messages the broker never accepted.
        byte reasonCode = extractReasonCode(message);
        if (isFailure(reasonCode)) {
            pendingPublish.getCallback().onFailure(new MqttPubAckFailureException("PUBACK", reasonCode));
        } else {
            pendingPublish.getCallback().onSuccess();
        }
        pendingPublish.onPubackReceived();
    }

    private static byte extractReasonCode(MqttMessage message) {
        // Netty only produces a reply variable header carrying a reason code for MQTT 5;
        // for 3.1.1 there is none, which is equivalent to success.
        return message.variableHeader() instanceof MqttPubReplyMessageVariableHeader replyHeader
                ? replyHeader.reasonCode()
                : (byte) 0x00;
    }

    private static boolean isFailure(byte reasonCode) {
        // unsigned comparison: 0x00 SUCCESS and 0x10 NO_MATCHING_SUBSCRIBERS are both successes
        return (reasonCode & 0xFF) >= 0x80;
    }

    private void handlePubrec(Channel channel, MqttMessage message) {
        MqttPubReplyMessageVariableHeader variableHeader = (MqttPubReplyMessageVariableHeader) message.variableHeader();
        int messageId = variableHeader.messageId();
        MqttPendingPublish pendingPublish = this.client.getPendingPublishes().get(messageId);
        if (pendingPublish == null) {
            // Nothing is pending under this packet id: a duplicate PUBREC, or one for a publish already
            // failed and removed. Dereferencing it threw from the event loop, taking down the channel and
            // with it every publish still in flight on that connection.
            log.debug("[{}] Received PUBREC for unknown message id {}.", this.client.getClientConfig().getClientId(), messageId);
            return;
        }
        pendingPublish.onPubackReceived();

        if (isFailure(variableHeader.reasonCode())) {
            pendingPublish.getCallback().onFailure(new MqttPubAckFailureException("PUBREC", variableHeader.reasonCode()));
            this.client.getPendingPublishes().remove(messageId);
            return;
        }

        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0);
        MqttMessage pubrelMessage = new MqttMessage(fixedHeader, variableHeader);
        channel.writeAndFlush(pubrelMessage);

        pendingPublish.setPubrelMessage(pubrelMessage);
        pendingPublish.startPubrelRetransmissionTimer(this.client.getEventLoop().next(), this.client::sendAndFlushPacket);
    }

    private void handlePubrel(Channel channel, MqttMessage message) {
        if (this.client.getQos2PendingIncomingPublishes().containsKey(((MqttMessageIdVariableHeader) message.variableHeader()).messageId())) {
            MqttIncomingQos2Publish incomingQos2Publish = this.client.getQos2PendingIncomingPublishes().get(((MqttMessageIdVariableHeader) message.variableHeader()).messageId());
            this.processIncomingMessage(incomingQos2Publish.getIncomingPublish());
            this.client.getQos2PendingIncomingPublishes().remove(incomingQos2Publish.getIncomingPublish().variableHeader().packetId());
        }
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(((MqttMessageIdVariableHeader) message.variableHeader()).messageId());
        channel.writeAndFlush(new MqttMessage(fixedHeader, variableHeader));
    }

    private void handlePubcomp(MqttMessage message) {
        MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) message.variableHeader();
        MqttPendingPublish pendingPublish = this.client.getPendingPublishes().get(variableHeader.messageId());
        if (pendingPublish == null) {
            log.debug("[{}] Received PUBCOMP for unknown message id {}.", this.client.getClientConfig().getClientId(), variableHeader.messageId());
            return;
        }
        pendingPublish.getCallback().onSuccess();
        this.client.getPendingPublishes().remove(variableHeader.messageId());
        pendingPublish.onPubcompReceived();
    }
}
