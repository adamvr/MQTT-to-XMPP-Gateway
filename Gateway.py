'''
Created on 26/08/2010

@author: maus
'''

import optparse

from MQTT import MQTTProtocol
from twisted.internet.protocol import ClientFactory
from twisted.application.service import Service
from twisted.internet import reactor
from twisted.words.xish.domish import Element
from wokkel.pubsub import PubSubClient, Item
from wokkel.client import XMPPClient
from twisted.words.protocols.jabber import jid

    
class Device:
    name = ""
    topicMap = {}
    
    def __init__(self, name="", **kwargs):
        self.name = name
        self.topicMap = kwargs
    

class MQTTListener(MQTTProtocol):
    
    def connectionMade(self):
        self.connect(self.factory.service.gatewayId, keepalive=1000000)
        reactor.callLater(2, self.pingreq)
        reactor.callLater(5, self.processMessages)
    
    def pingrespReceived(self):
        reactor.callLater(2, self.pingreq)

    def connackReceived(self, status):
        self.subscribe(self.factory.service.gatewayRegTopic)
        print 'connect'
        
    def processMessages(self):
        map = dict([(v,k) for k,v in self.factory.service.topicBindings])
        for node, message in self.factory.service.xmppMessageBuffer:
            if node in map:
                # TODO: Possible memory leak here when messages arrive to nodes that don't have a 
                # binding. Shouldn't happen but let's make a note of it
                print "Publishing MQTT - Topic: %s, Message: %s" % (map[node], message)
                self.publish(map[node], message)
                self.factory.service.xmppMessageBuffer.remove((node, message))
        reactor.callLater(5, self.processMessages)
        
    def publishReceived(self, topic, message, qos, dup, retain, messageId):
        if topic == self.factory.service.gatewayRegTopic:
            # REGISTRATION REQUEST
            # EXPECTED FORM:
            # registration -> name_field {"\n" topic_field}
            # topic_field -> type ": " id {" " id}
            # name_field -> "name" ": " id
            # type -> "input" | "output" | "status" | "description"
            # id -> ? all visible characters ?
            # Example:
            # name: deviceName
            # input: inputTopicOne inputTopicTwo
            # output: outputTopicOne outputTopicTwo
            # status: statusTopicOne statusTopicTwo
            # description: descriptionTopicOne
            
            map = {}
            for line in message.split('\n'):
                k, v = line.split(': ')
                map[k] = v.split(' ')
                
            
            newDevice = Device()
            for k in map.keys():
                if k not in ['name', 'input', 'output', 'status', 'description']:
                    # Bad registration request, ignore it
                    return
                
                if k == 'name':
                    newDevice.name = map[k][0]
                else:
                    newDevice.topicMap[k] = map[k]
            
            if 'output' in newDevice.topicMap:
                for topic in newDevice.topicMap['output']:
                    self.subscribe(topic)
            
            self.factory.service.addDevice(newDevice)
        
        else:
            # Received a publish on an output topic
            self.factory.service.mqttMessageBuffer.append((topic, message))

class MQTTListenerFactory(ClientFactory):
    protocol = MQTTListener
    
    def __init__(self, service = None):
        self.service = service

class XMPPPublishSubscriber(PubSubClient):
    def connectionInitialized(self):
        PubSubClient.connectionInitialized(self)
        reactor.callLater(5, self.processMessages)
        
    def printError(self, error):
        print error
        
    def itemsReceived(self, event):
        for item in event.items:
            if item.name == 'item':
                for child in item.children:
                    self.parent.parent.xmppMessageBuffer.append((event.nodeIdentifier,
                                                                  str(child.toXml())))
                
    def processMessages(self):
        # Check if there are any new devices to make nodes for
        for dev in self.parent.parent.devices:
            # Create each input and output node
            inputNodeList = []
            for topicType in dev.topicMap.keys():
                for topic in dev.topicMap[topicType]:
                    # Form the node's ID
                    node = unicode(self.parent.parent.gatewayId + '/' + dev.name + '/' + topic)
                    # Create a binding between the node ID and the MQTT topic
                    self.parent.parent.topicBindings.append((topic, node))
                    # Add the node to a list of topics to subscribe to
                    if topicType == 'input':
                        inputNodeList.append(node)
                 
                    # Create the node
                    # TODO: add error checking callbacks to this
                    # TODO: make a heirachy of nodes... maybe?
                    # TODO: set their expected XML namespace to EEML's
                    # TODO: WATCH OUT FOR NAMING CONFLICTS!
                    service = self.parent.parent.xmppServerJID
                    self.createNode(service, node, {'pubsub#type':''},
                                    self.parent.jid).addErrback(self.printError)
            
            # Subscribe to the input nodes
            for node in inputNodeList:
                self.subscribe(self.parent.parent.xmppServerJID, node,
                     self.parent.jid).addErrback(self.printError)
        
        # Empty the new device list
        self.parent.parent.devices = []        
        
        # Publish the messages in the MQTT buffer to their respective XMPP nodes
        map = dict(self.parent.parent.topicBindings)
        
        for topic, message in self.parent.parent.mqttMessageBuffer:
            if topic in map:
                self.publish(self.parent.parent.xmppServerJID, map[topic], [Item(None, message)]
                             ).addErrback(self.printError)
        
        reactor.callLater(5, self.processMessages)
        
        
        #while len(buffer):
            # Get the first topic in the message buffer
        #    workingTopic = buffer[0][0]
        #    itemList = []
            # Get the list of (topic,message)s that correspond to that topic
        #    for topic, message in filter(lambda x: x[0] == workingTopic, buffer):
                # Create an item using the message and add it to the item list
        #        itemList.append(Item(None, message))
                # Remove the tuple from the message buffer
        #        buffer.remove((topic, message))
            
            # Publish the message
        #    if topic in map:
        #        self.publish(self.parent.parent.xmppServerJID, map[topic], itemList
        #                     ).addErrback(self.printError)     
        

class GatewayService(Service):
    # Bindings: List of tuples. Tuple(0) refers to the MQTT side of the binding, namely the topic
    #           Tuple(1) refers to the XMPP node
    topicBindings = []
    
    devices = []
    
    mqttMessageBuffer = []
    xmppMessageBuffer = []
    
    def __init__(self, gatewayId, gatewayRegTopic, xmppServer, gatewayJID, gatewayPassword, 
                 mqttBroker):
        
        self.gatewayId = gatewayId
        self.gatewayRegTopic = gatewayRegTopic
        self.xmppServerJID = jid.JID(xmppServer)
        
        self.mqttFactory = MQTTListenerFactory(self)
        
        self.xmppClient = XMPPClient(jid.JID(gatewayJID), gatewayPassword)
        XMPPPublishSubscriber().setHandlerParent(self.xmppClient)
        self.xmppClient.parent = self
        
        reactor.connectTCP(mqttBroker, 1883, self.mqttFactory)
        reactor.connectTCP(xmppServer, 5222, self.xmppClient.factory)
        
        print 'Got ere'
        
        
    def addDevice(self, device):
        self.devices.append(device)
        
def main():
    
    parser = optparse.OptionParser(usage='Insert usage string here')
    parser.add_option('-m', '--mqttbroker', dest='mqttBroker', default=None, type='string',
                       help='Specify the MQTT broker to run on')
    parser.add_option('-x', '--xmppserver', dest='xmppServer', default=None, type='string',
                       help='Specify the XMPP pubsub server to run on')
    parser.add_option('-i', '--gatewayid', dest='gatewayId', default=None, type='string',
                       help='Specify the ID of the gateway on the MQTT and XMPP networks')
    parser.add_option('-t', '--registrationtopic', dest='registrationTopic', default=None,
                       type='string', help='Specify the MQTT topic that the gateway will listen for registration messages on')
    parser.add_option('-j', '--gatewayjid', dest='gatewayJid', default=None, type='string',
                       help='Specify the JID used to publish and subscribe to XMPP messages')
    parser.add_option('-p', '--gatewaypassword', dest='gatewayPassword', default=None, type='string',
                       help='Specify the password used to connect using the specified JID')
    
    (options, args) = parser.parse_args()
    
    # CHECK THIS AT SOME POINT
    gateway = GatewayService(options.gatewayId, options.registrationTopic, options.xmppServer, 
                             options.gatewayJid, options.gatewayPassword, options.mqttBroker)
    
    reactor.run()
    
if __name__ == '__main__':
    main()