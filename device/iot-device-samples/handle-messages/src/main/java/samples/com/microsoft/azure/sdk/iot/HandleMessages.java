// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package samples.com.microsoft.azure.sdk.iot;

import com.microsoft.azure.sdk.iot.device.DeviceClient;
import com.microsoft.azure.sdk.iot.device.IotHubClientProtocol;
import com.microsoft.azure.sdk.iot.device.IotHubMessageResult;
import com.microsoft.azure.sdk.iot.device.Message;
import com.microsoft.azure.sdk.iot.service.Device;
import com.microsoft.azure.sdk.iot.service.FeedbackBatch;
import com.microsoft.azure.sdk.iot.service.FeedbackReceiver;
import com.microsoft.azure.sdk.iot.service.FeedbackRecord;
import com.microsoft.azure.sdk.iot.service.IotHubServiceClientProtocol;
import com.microsoft.azure.sdk.iot.service.RegistryManager;
import com.microsoft.azure.sdk.iot.service.ServiceClient;
import com.microsoft.azure.sdk.iot.service.exceptions.IotHubException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Scanner;


/**
 * Handles messages from an IoT Hub. Default protocol is to use
 * MQTT transport.
 */
public class HandleMessages
{
    /** Used as a counter in the message callback. */
    protected static class Counter
    {
        protected int num;

        public Counter(int num)
        {
            this.num = num;
        }

        public int get()
        {
            return this.num;
        }

        public void increment()
        {
            this.num++;
        }

        @Override
        public String toString()
        {
            return Integer.toString(this.num);
        }
    }

    protected static class MessageCallback implements com.microsoft.azure.sdk.iot.device.MessageCallback
    {
        public IotHubMessageResult execute(Message msg, Object context)
        {
            Counter counter = (Counter) context;
            System.out.println(
                    "Received message " + counter.toString()
                            + " with content: " + new String(msg.getBytes(), Message.DEFAULT_IOTHUB_MESSAGE_CHARSET));

            int switchVal = counter.get() % 3;
            IotHubMessageResult res;
            switch (switchVal)
            {
                case 0:
                    res = IotHubMessageResult.COMPLETE;
                    break;
                case 1:
                    res = IotHubMessageResult.ABANDON;
                    break;
                case 2:
                    res = IotHubMessageResult.REJECT;
                    break;
                default:
                    // should never happen.
                    throw new IllegalStateException("Invalid message result specified.");
            }

            System.out.println("Responding to message " + counter.toString() + " with " + res.name());

            counter.increment();

            return res;
        }
    }

    // Our MQTT doesn't support abandon/reject, so we will only display the messaged received
    // from IoTHub and return COMPLETE
    protected static class MessageCallbackMqtt implements com.microsoft.azure.sdk.iot.device.MessageCallback
    {
        public IotHubMessageResult execute(Message msg, Object context)
        {
            Counter counter = (Counter) context;
            System.out.println(
                    "Received message " + counter.toString()
                            + " with content: " + new String(msg.getBytes(), Message.DEFAULT_IOTHUB_MESSAGE_CHARSET));

            counter.increment();

            return IotHubMessageResult.COMPLETE;
        }
    }

    /**
     * Receives requests from an IoT Hub. Default protocol is to use
     * MQTT transport.
     *
     * @param args 
     * args[0] = IoT Hub connection string
     * args[1] = protocol (optional, one of 'mqtt' or 'amqps' or 'https' or 'amqps_ws')
     */
    public static void main(String[] args) throws IOException, URISyntaxException
    {
        System.out.println("Starting...");
        System.out.println("Beginning setup.");

        if (args.length <= 0 || 3 <= args.length)
        {
            System.out.format(
                    "Expected 1 or 2 arguments but received %d.\n"
                     + "The program should be called with the following args: \n"
                     + "1. [Device connection string] - String containing Hostname, Device Id & Device Key in one of the following formats: HostName=<iothub_host_name>;DeviceId=<device_id>;SharedAccessKey=<device_key>\n"
                     + "2. (amqps | amqps_ws)\n",
                     args.length);
            return;
        }

        String connString = args[0];
        IotHubServiceClientProtocol protocol;
        if (true)
        {
            String protocolStr = args[1];
            if (protocolStr.equals("amqps"))
            {
                protocol = IotHubServiceClientProtocol.AMQPS;
            }
            else if (protocolStr.equals("amqps_ws"))
            {
                protocol = IotHubServiceClientProtocol.AMQPS_WS;
            }
            else
            {
                System.out.format(
                      "Expected argument 2 to be one of 'amqps' or 'amqps_ws' but received %s\n"
                            + "The program should be called with the following args: \n"
                            + "1. [Device connection string] - String containing Hostname, Device Id & Device Key in one of the following formats: HostName=<iothub_host_name>;DeviceId=<device_id>;SharedAccessKey=<device_key>\n"
                            + "2. (mqtt | https | amqps | amqps_ws | mqtt_ws)\n",
                           protocolStr);
                return;
            }
        }

        System.out.println("Successfully read input parameters.");
//        System.out.format("Using communication protocol %s.\n", protocol.name());

//        DeviceClient client = new DeviceClient(connString, protocol);

//        System.out.println("Successfully created an IoT Hub client.");

        RegistryManager registryManager = RegistryManager.createFromConnectionString(connString);

//        if (protocol == IotHubClientProtocol.MQTT)
//        {
//            MessageCallbackMqtt callback = new MessageCallbackMqtt();
//            Counter counter = new Counter(0);
//            client.setMessageCallback(callback, counter);
//        }
//        else
//        {
//            MessageCallback callback = new MessageCallback();
//            Counter counter = new Counter(0);
//            client.setMessageCallback(callback, counter);
//        }

        try {
            ArrayList<Device> devices = registryManager.getDevices(10000);

            System.out.println("Found " + devices.size() + " devices");

            for (Device device : devices) {
                System.out.println("Device ID: " + device.getDeviceId());
                System.out.println("Device last activity time: " + device.getLastActivityTime());
                System.out.println("");
            }
        } catch (IotHubException e) {
            e.printStackTrace();
        }

        System.out.format("Using communication protocol to query IoT Hub: %s.\n", protocol.name());

        ServiceClient client = ServiceClient.createFromConnectionString(connString, protocol);

        System.out.println("Successfully created an IoT Hub client.");

        client.open();

        System.out.println("Opened connection to IoT Hub.");

        FeedbackReceiver feedbackReceiver = client.getFeedbackReceiver();
        feedbackReceiver.open();

        System.out.println("Beginning to receive messages...");

        while (true) {
            try {
                System.out.println("Entering message receiver loop...");

                FeedbackBatch feedbackBatch = feedbackReceiver.receive(10000L);
                if (feedbackBatch == null) {
                    System.out.println("No messages received, polling again...");
                    continue;
                }

                for (FeedbackRecord record : feedbackBatch.getRecords()) {
                    System.out.println("Got message id: " + record.getOriginalMessageId());
                    System.out.println("Status code: " + record.getStatusCode());
                    System.out.println("Device id: " + record.getDeviceId());
                    System.out.println("Description: " + record.getDescription());
                }
            } catch (InterruptedException e) {
                break;
            }
        }

        client.close();

        System.out.println("Shutting down...");
    }
}