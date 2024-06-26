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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.concurrent.Future;
import org.thingsboard.mqtt.broker.util.BasicCallback;

import java.util.Iterator;

public interface MqttClient {

    /**
     * Connect to the specified hostname/ip. By default uses port 1883.
     * If you want to change the port number, see {@link #connect(String, int)}
     *
     * @param host The ip address or host to connect to
     * @return A future which will be completed when the connection is opened and we received an CONNACK
     */
    void connect(ConnectCallback connectCallback, String host);

    /**
     * Connect to the specified hostname/ip using the specified port
     *
     * @param host The ip address or host to connect to
     * @param port The tcp port to connect to
     * @return A future which will be completed when the connection is opened and we received an CONNACK
     */
    void connect(ConnectCallback connectCallback, String host, int port);

    /**
     *
     * @return boolean value indicating if channel is active
     */
    boolean isConnected();

    /**
     * Attempt reconnect to the host that was attempted with {@link #connect(String, int)} method before
     *
     * @return A future which will be completed when the connection is opened and we received an CONNACK
     * @throws IllegalStateException if no previous {@link #connect(String, int)} calls were attempted
     */
    void reconnect(ConnectCallback connectCallback);

    /**
     * Retrieve the netty {@link EventLoopGroup} we are using
     * @return The netty {@link EventLoopGroup} we use for the connection
     */
    EventLoopGroup getEventLoop();

    /**
     * By default we use the netty {@link NioEventLoopGroup}.
     * If you change the EventLoopGroup to another type, make sure to change the {@link Channel} class using {@link MqttClientConfig#setChannelClass(Class)}
     * If you want to force the MqttClient to use another {@link EventLoopGroup}, call this function before calling {@link #connect(String, int)}
     *
     * @param eventLoop The new eventloop to use
     */
    void setEventLoop(EventLoopGroup eventLoop);

    /**
     * Subscribe on the given topic. When a message is received, MqttClient will invoke the {@link MqttHandler#onMessage(String, ByteBuf)} function of the given handler
     *
     * @param topic The topic filter to subscribe to
     * @param handler The handler to invoke when we receive a message
     * @return A future which will be completed when the server acknowledges our subscribe request
     */
    void on(String topic, MqttHandler handler, BasicCallback callback);

    /**
     * Subscribe on the given topic, with the given qos. When a message is received, MqttClient will invoke the {@link MqttHandler#onMessage(String, ByteBuf)} function of the given handler
     *
     * @param topic The topic filter to subscribe to
     * @param handler The handler to invoke when we receive a message
     * @param qos The qos to request to the server
     * @return A future which will be completed when the server acknowledges our subscribe request
     */
    void on(String topic, MqttHandler handler, BasicCallback callback, MqttQoS qos);

    /**
     * Subscribe on the given topic. When a message is received, MqttClient will invoke the {@link MqttHandler#onMessage(String, ByteBuf)} function of the given handler
     * This subscription is only once. If the MqttClient has received 1 message, the subscription will be removed
     *
     * @param topic The topic filter to subscribe to
     * @param handler The handler to invoke when we receive a message
     * @return A future which will be completed when the server acknowledges our subscribe request
     */
    void once(String topic, MqttHandler handler, BasicCallback callback);

    /**
     * Subscribe on the given topic, with the given qos. When a message is received, MqttClient will invoke the {@link MqttHandler#onMessage(String, ByteBuf)} function of the given handler
     * This subscription is only once. If the MqttClient has received 1 message, the subscription will be removed
     *
     * @param topic The topic filter to subscribe to
     * @param handler The handler to invoke when we receive a message
     * @param qos The qos to request to the server
     * @return A future which will be completed when the server acknowledges our subscribe request
     */
    void once(String topic, MqttHandler handler, BasicCallback callback, MqttQoS qos);

    /**
     * Remove the subscription for the given topic and handler
     * If you want to unsubscribe from all handlers known for this topic, use {@link #off(String)}
     *
     * @param topic The topic to unsubscribe for
     * @param handler The handler to unsubscribe
     * @return A future which will be completed when the server acknowledges our unsubscribe request
     */
    Future<Void> off(String topic, MqttHandler handler);

    /**
     * Remove all subscriptions for the given topic.
     * If you want to specify which handler to unsubscribe, use {@link #off(String, MqttHandler)}
     *
     * @param topic The topic to unsubscribe for
     * @return A future which will be completed when the server acknowledges our unsubscribe request
     */
    Future<Void> off(String topic);

    /**
     * Publish a message to the given payload
     * @param topic The topic to publish to
     * @param payload The payload to send
     * @return A future which will be completed when the message is sent out of the MqttClient
     */
    ChannelFuture publish(String topic, ByteBuf payload, BasicCallback callback);

    /**
     * Publish a message to the given payload, using the given qos
     * @param topic The topic to publish to
     * @param payload The payload to send
     * @param qos The qos to use while publishing
     * @return A future which will be completed when the message is delivered to the server
     */
    ChannelFuture publish(String topic, ByteBuf payload, BasicCallback callback, MqttQoS qos);

    /**
     * Publish a message to the given payload, using optional retain
     * @param topic The topic to publish to
     * @param payload The payload to send
     * @param retain true if you want to retain the message on the server, false otherwise
     * @return A future which will be completed when the message is sent out of the MqttClient
     */
    ChannelFuture publish(String topic, ByteBuf payload, BasicCallback callback, boolean retain);

    /**
     * Publish a message to the given payload, using the given qos and optional retain
     * @param topic The topic to publish to
     * @param payload The payload to send
     * @param qos The qos to use while publishing
     * @param retain true if you want to retain the message on the server, false otherwise
     * @return A future which will be completed when the message is delivered to the server
     */
    ChannelFuture publish(String topic, ByteBuf payload, BasicCallback callback, MqttQoS qos, boolean retain);

    /**
     * Retrieve the MqttClient configuration
     * @return The {@link MqttClientConfig} instance we use
     */
    MqttClientConfig getClientConfig();


    /**
     * Construct the MqttClientImpl with additional config.
     * This config can also be changed using the {@link #getClientConfig()} function
     *
     * @param config The config object to use while looking for settings
     * @param defaultHandler The handler for incoming messages that do not match any topic subscriptions
     */
    static MqttClient create(MqttClientConfig config, MqttHandler defaultHandler,
                             ReceivedMsgProcessor receivedMsgProcessor, Iterator<String> ipAddrIterator) {
        return new MqttClientImpl(config, defaultHandler, receivedMsgProcessor, ipAddrIterator);
    }

    /**
     * Send disconnect
     *
     */
    void disconnect();

    /**
     * Send disconnect and close channel
     *
     */
    void disconnectAndClose();

    /**
     * Sets the {@see #MqttClientCallback} object for this MqttClient
     * @param clientCallback The callback to be set
     */
    void setClientCallback(MqttClientCallback clientCallback);

}
