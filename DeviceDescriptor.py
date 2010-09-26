'''
Created on 26/09/2010

@author: maus
'''

from twisted.words.xish.domish import Element

class Device(Element):
    '''
    classdocs
    '''
    
    def __init__(self, deviceName, rootUri, feeds = None, tags = None):
        Element.__init__(self, (None, 'device'))
        self._deviceName = Element((None, 'name'))
        self._deviceName.addContent(deviceName)
        self.addChild(self._deviceName)
        
        self._rootUri = Element((None, 'rootUri'))
        self._rootUri.addContent(rootUri)
        self.addChild(self._rootUri)
        
        self._feeds = []
        if feeds is not None:
            for f in feeds:
                self._feeds.append(f)
                self.addChild(f)
        
        self._tags = []
        if tags is not None:
            for t in tags:
                tag = Tag(t)
                self.addChild(tag)
                self._tags.append(tag)
                
        
    def addFeed(self, feed):
        self._feeds.append(feed)
        self.addChild(feed)

    def addTag(self, tag):
        eTag = Tag(tag)
        self._tags.append(eTag)
        self.addChild(eTag)        

class Feed(Element):
    def __init__(self, name, type, uri, tags = None):
        Element.__init__(self, (None, 'feed'), attribs={'type':type})
        self._feedName = Element((None, 'name'))
        self._feedName.addContent(name)
        self.addChild(self._feedName)
       
        self._feedUri = Element((None, 'feedUri'))
        self._feedUri.addContent(uri)
        self.addChild(self._feedUri)

        self._tags = []
        if tags is not None:
            for t in tags:
                tag = Tag(t)
                self.addChild(tag)
                self._tags.append(tag)
              
    def addTag(self, tag):
        eTag = Tag(tag)
        self._tags.append(eTag)
        self.addChild(eTag)        

class Tag(Element):
    def __init__(self, value):
        Element.__init__(self, (None, 'tag'))
        self.addContent(value)
        
        
if __name__ == '__main__':
    d = Device('devdev', 'xmpp:pubsub.talkr.im?;node=what', [Feed('a','input','xmpp:::', ['abcd'])])
    d.addTag('asdf')
    print d.toXml()