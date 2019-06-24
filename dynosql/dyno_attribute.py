import logging

logger = logging.getLogger(__name__)

class DynoAttribute(object):

    def __init__(self, name):
        self.name = name
        self.query = None

    def __eq__(self, value):
        logger.info('%s = %s' % (self.name, value))
        self.query = (self.name, '=', value)
        return self.query

    def __ne__(self, value):
        logger.info('%s != %s' % (self.name, value))
        self.query = (self.name, '<>', value)
        return self.query

    def __gt__(self, value):
        logger.info('%s  %s' % (self.name, value))
        self.query = (self.name, '>', value)
        return self.query

    def __ge__(self, value):
        logger.info('%s  %s' % (self.name, value))
        self.query = (self.name, '>=', value)
        return self.query

    def __lt__(self, value):
        logger.info('%s  %s' % (self.name, value))
        self.query = (self.name, '<', value)
        return self.query

    def __le__(self, value):
        logger.info('%s  %s' % (self.name, value))
        self.query = (self.name, '<=', value)
        return self.query

    # def __and__(self, value):
    #     logger.info('%s and %s' % (self.name, value))
        
