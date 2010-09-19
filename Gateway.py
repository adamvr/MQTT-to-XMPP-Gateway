'''
Created on 26/08/2010

@author: maus
'''

from MQTT import MQTTProtocol
from twisted.internet.protocol import ClientFactory
from twisted.application.service import Service
from twisted.internet import reactor
from twisted.words.xish.domish import Element
from wokkel.pubsub import PubSubClient, Item
from wokkel.client import XMPPClient
from twisted.words.protocols.jabber import jid
from twisted.python import log

    
class Device:
    name = ""
    topicMap = {}
    
    def __init__(self, name="", **kwargs):
        self.name = name
        self.topicMap = kwargs
    

class MQTTListener(MQTTProtocol):
    
    def connectionMade(self):
	log.msg('MQTT Connected')
        self.connect(self.factory.service.gatewayId, keepalive=1000000)
        reactor.callLater(2, self.pingreq)
        reactor.callLater(5, self.processMessages)
    
    def pingrespReceived(self):
	log.msg('Ping received from MQTT broker')
        reactor.callLater(2, self.pingreq)

    def connackReceived(self, status):
	if status == 0:
	    self.subscribe(self.factory.service.gatewayRegTopic)
	else:
	    log.msg('Connecting to MQTT broker failed')
        
    def processMessages(self):
        map = dict([(v,k) for k,v in self.factory.service.topicBindings])
        for node, message in self.factory.service.xmppMessageBuffer:
            if node in map:
                # TODO: Possible memory leak here when messages arrive to nodes that don't have a 
                # binding. Shouldn't happen but let's make a note of it
		log.msg('Publishing message to MQTT - Topic %s, Message %s' % (map[node], message))	
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
            
	    log.msg('Registration request received:\n %s' % message)
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
	    log.msg('Output message received\nTopic: %s, Message%s' % (topic, message))
            self.factory.service.mqttMessageBuffer.append((topic, message))

class MQTTListenerFactory(ClientFactory):
    protocol = MQTTListener
    
    def __init__(self, service = None):
        self.service = service

class XMPPPublishSubscriber(PubSubClient):
    def connectionInitialized(self):
	log.msg('XMPP initialized')
        PubSubClient.connectionInitialized(self)
        reactor.callLater(5, self.processMessages)
        
    def printError(self, error):
        log.msg(str(error)) 

    def itemsReceived(self, event):
        for item in event.items:
            if item.name == 'item':
                for child in item.children:
		    log.msg('Input message received\nNode: %s, Message: %s' % (event.nodeIdentifier, str(child.toXml())))
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
		    # TODO: log the creation or failed creation of the nodes
                    service = self.parent.parent.xmppServerJID
		    log.msg('Creating node: %s' % node)
                    self.createNode(service, node, {'pubsub#type':''},
                                    self.parent.jid).addErrback(self.printError)
            
            # Subscribe to the input nodes
            for node in inputNodeList:
		log.msg('Subscribing to node: %s' % node)
                self.subscribe(self.parent.parent.xmppServerJID, node,
                     self.parent.jid).addErrback(self.printError)
        
        # Empty the new device list
        self.parent.parent.devices = []        
        
        # Publish the messages in the MQTT buffer to their respective XMPP nodes
        map = dict(self.parent.parent.topicBindings)
        buffer = self.parent.parent.mqttMessageBuffer
        
        for topic, message in buffer:
            if topic in map:
		log.msg('Publishing output message\nNode: %s, Message: %s' % (map[topic], message)) 
                self.publish(self.parent.parent.xmppServerJID, map[topic], [Item(None, message)]
                             ).addErrback(self.printError)
                
        buffer = []
        
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
    
    def __init__(self, gatewayId, gatewayRegTopic, xmppServerJID, mqttFactory, xmppClient ):
        self.gatewayId = gatewayId
        self.gatewayRegTopic = gatewayRegTopic
        self.xmppServerJID = xmppServerJID
        
        self.mqttFactory = mqttFactory
        self.mqttFactory.service = self
        
        self.xmppClient = xmppClient
        self.xmppClient.parent = self

	log.msg('Starting gateway\nGateway ID: %s\nGateway registration topic: %s\nXMPP Server: %s\n' % (gatewayId, gatewayRegTopic, xmppServerJID))
    
        
    def addDevice(self, device):
        self.devices.append(device)
        
def main():
    
    mqttFactory = MQTTListenerFactory()
    xmppClient = XMPPClient(jid.JID("ceit_sensors@talkr.im/GATEWAY"), 'avrud0')
    xmppClient.logTraffic = True
    XMPPPublishSubscriber().setHandlerParent(xmppClient)

    gateway = GatewayService('gateway', 'gateway/registration', jid.JID('pubsub.talkr.im'), 
                             mqttFactory, xmppClient)
    
    reactor.connectTCP('192.168.1.150', 1883, gateway.mqttFactory)
    reactor.connectTCP('pubsub.talkr.im', 5222, gateway.xmppClient.factory)
    log.startLogging(open('./loglog', 'w'))
    
    reactor.run()

if __name__ == '__main__':
    main()
