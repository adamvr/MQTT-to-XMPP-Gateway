'''
Created on 26/08/2010

@author: Adam Rudd
'''

# TODO:
# 1. Fix the MQTTProtocol and PubSubClients so that their pointers to
#    the gateway service's message buffers and binding lists are direct
#    rather than self.parent.parent.x and self.factory.service.x

from MQTT import MQTTProtocol
from twisted.internet.protocol import ClientFactory
from twisted.application.service import Service
from twisted.internet import reactor
from twisted.words.xish import domish
from wokkel.pubsub import PubSubClient, Item
from wokkel.client import XMPPClient
from twisted.words.protocols.jabber import jid
from twisted.python import log

import DeviceDescriptor

# Do some more with this class.    
class Device:
    name = ""
    topicMap = {}
    
    def __init__(self, name="", **kwargs):
        self.name = name
        self.topicMap = kwargs
    
# Parser utility
class __ParseElementFromRawXml(object):
    def __call__(self, xml):
        self.result = None
        def onStart(el):
            self.result = el
        def onEnd():
            pass
        def onElement(el):
            self.result.addChild(el)

        parser = domish.elementStream()
        parser.DocumentStartEvent = onStart
        parser.ElementEvent = onElement
        parser.DocumentEndEvent = onEnd
        parser.parse(xml)
        return self.result

parseElementFromRawXml = __ParseElementFromRawXml()

class MQTTListener(MQTTProtocol):
    pingPeriod = 60000
    
    def connectionMade(self):
        log.msg('MQTT Connected')
        self.connect(self.factory.service.gatewayId, keepalive=self.pingPeriod)
        # TODO: make these constants configurable
        reactor.callLater(self.pingPeriod//1000, self.pingreq)
        reactor.callLater(5, self.processMessages)
    
    def pingrespReceived(self):
        log.msg('Ping received from MQTT broker')
        reactor.callLater(self.pingPeriod//1000, self.pingreq)

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
        
        # Create the gateway root node and registry
        self.gatewayRootId = self.parent.parent.gatewayId
        self.registryId = self.gatewayRootId + '/registry'
        
        self.createNode(self.parent.parent.xmppServerJID, self.gatewayRootId, 
                        {'pubsub#node_type':'collection'}).addErrback(self.printError)
        # TODO: make maxitems configurable
        self.createNode(self.parent.parent.xmppServerJID, self.registryId,
                        {'pubsub#node_type':'leaf', 'pubsub#collection':self.gatewayRootId,
                         'pubsub#max_items':'100'}
                        ).addErrback(self.printError)
        reactor.callLater(5, self.processMessages)
        
    def printError(self, error):
        log.msg(str(error)) 

    def itemsReceived(self, event):
        # TODO: this tends to choke if things are published from pidgin
        #       it has to do with pidgin introducing spurious whitespace
        #       and that not getting parsed away. A way around this would be
        #       to parse the xml that comes out of the items or ignoring
        #       whitespace items.
        for item in event.items:
            if item.name == 'item':
                for child in item.children:
                    log.msg('''Input message received 
                    Node: %s, Message: %s''' % (event.nodeIdentifier, str(child.toXml())))
                    self.parent.parent.xmppMessageBuffer.append((event.nodeIdentifier,
                                                                  str(child.toXml())))
                
    def processMessages(self):
        # Check if there are any new devices to make nodes for
        for dev in self.parent.parent.devices:
            # Create the root node for this device
            
            # TODO: hilarious as this is to look at, it should probably be in a
            #       helper function.
            rootNodeName = unicode(hex(abs(hash(dev.name))))
            self.createNode(self.parent.parent.xmppServerJID, rootNodeName,
                            {'pubsub#node_type':'collection', 
                             'pubsub#collection': self.gatewayRootId}
                            ).addErrback(self.printError)
            # Create the description document
            description = DeviceDescriptor.Device(dev.name, rootNodeName)
            inputNodeList = []
            for type in dev.topicMap.keys():
                for topic in dev.topicMap[type]:
                    # Form the node's id
                    node = unicode(hex(abs(hash(dev.name + '/' + topic))))
                    
                    # Add the node to this node's description
                    # TODO: might want to check if all of these values are sane
                    description.addFeed(DeviceDescriptor.Feed(topic, type, node))
                    
                    # If it's an input topic add the node id to the 
                    # list of nodes to subscribe to
                    if type == 'input':
                        inputNodeList.append(node)
                    
                    
                    # Create a binding from this MQTT topic to the node
                    # we're going to create
                    self.parent.parent.topicBindings.append((topic, node))
                    
                    log.msg('Creating node: %s' % node)
                    # Create the node
                    self.createNode(self.parent.parent.xmppServerJID, node,
                                    {'pubsub#collection': rootNodeName}
                                    ).addErrback(self.printError)
            
            for node in inputNodeList:
                log.msg('Subscribing to node: %s' % node)
                self.subscribe(self.parent.parent.xmppServerJID, node, self.parent.jid
                               ).addErrback(self.printError)
                               
            # Publish the device description to the registry
            log.msg('Publishing registration:\n %s' % description.toXml())
            self.publish(self.parent.parent.xmppServerJID, self.registryId,
                        [Item(rootNodeName, description)]).addErrback(self.printError)
                     
        self.parent.parent.devices = []        
        
        # Publish the messages in the MQTT buffer to their respective XMPP nodes
        map = dict(self.parent.parent.topicBindings)
        
        for topic, message in self.parent.parent.mqttMessageBuffer:
            if topic in map:
                # Parse the incoming message into a domish.Element
                # TODO: Check if it belongs to the appropriate namespace (http://www.eeml.org/xsd/005)
                # IMPORTANT NOTE: This does not accept XML documents that begin with an XML
                #                 declaration (i.e. <?xml version="1.0 ?>)
                #                 
                # WAIT NO IT WORKS FINE!
                messageElement = None
                try:
                    messageElement = parseElementFromRawXml(message)
                except:
                    log.err()
                    messageElement = None
                
                # If it properly parsed, publish it    
                if messageElement is not None:
                    log.msg('Publishing output message\nNode: %s, Message: %s' % (map[topic], messageElement.toXml())) 
                    self.publish(self.parent.parent.xmppServerJID, map[topic], [Item(None, messageElement)]
                             ).addErrback(self.printError)
                # Remove the message regardless
                self.parent.parent.mqttMessageBuffer.remove((topic, message))
                
        
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
        self.xmppClient.logTraffic = True
        
        # TODO: Should connect outside of the service
        reactor.connectTCP(mqttBroker, 1883, self.mqttFactory)
        reactor.connectTCP(xmppServer, 5222, self.xmppClient.factory)
        

        log.msg('Starting gateway\nGateway ID: %s\nGateway registration topic: %s\nXMPP Server: %s\n' % (gatewayId, gatewayRegTopic, self.xmppServerJID))
    
        
    def addDevice(self, device):
        self.devices.append(device)
        
def main():
    
    import optparse

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
    import sys
    log.startLogging(sys.stdout)
    
    reactor.run()

if __name__ == '__main__':
    main()

