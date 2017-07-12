import json,sys; 
import urllib2;
import logging;
import timeit;

class MyLogging(object):
    @staticmethod
    def initialize_logger(file_name, log_level=logging.INFO, append_log=False):
        logging_format = '%(asctime)s %(levelname)s %(filename)s(%(lineno)d) %(name)s %(message)s'
        file_mode = 'a' if append_log else 'w'
        logging.basicConfig(format=logging_format, filename=file_name, filemode=file_mode, level=log_level)
        stdLogger = logging.StreamHandler()
        stdLogger.setFormatter(logging.Formatter(logging_format))
        logging.getLogger().addHandler(stdLogger)

class Populator(object):
	def __init__(self, es_server, es_index, es_type, sample_document_file, total_documents, documents_per_batch):
		MyLogging.initialize_logger('populator.log')
		self.logger = logging.getLogger('populator')
		self.es_server = es_server
		self.es_index = es_index
		self.es_type = es_type
		self.sample_document_file = sample_document_file
		self.total_documents = total_documents
		self.documents_per_batch = documents_per_batch
		self.total_batches = total_documents / documents_per_batch
		self.logger.info('Total batches = %d', self.total_batches);

		with open(sample_document_file) as sampleFile:
			self.sample_document_file_contents = sampleFile.read();

	def populate(self):
		# Start population
		startTime = timeit.default_timer();
		self.logger.info('Population started...');

		es_url = 'http://%s/%s/%s/_bulk' %(self.es_server, self.es_index, self.es_type)

		self.logger.info('es_url = %s', es_url)

		for nIndex in range(0, self.total_batches):
			bulkCreateOperationAsString = self.getBulkCreateOperationPayload(nIndex);	
			self.httpRequest(es_url, 'POST', bulkCreateOperationAsString);
			self.logger.info('Created %d documents of %d', (nIndex + 1) * self.documents_per_batch, self.total_documents);

		self.logger.info('Population done...');
		self.logger.info('Time taken: %s', self.formatElapsedTime(timeit.default_timer() - startTime));

	def getBulkCreateOperationPayload(self, current_batch):

		bulkCreateOperationList = [];    
		for nIndex in range(0, self.documents_per_batch):
		
			document_id = current_batch * self.documents_per_batch + nIndex + 1
			indexOperation = '{ "index" : {"_id": "%s_%d"} }' %(self.es_type, document_id);
	
			sampleJson = self.sample_document_file_contents.replace('#NUMERIC_INDEX_01#', str(document_id));
			sampleJson = json.dumps(json.loads(sampleJson));
				
			bulkCreateOperationList.append(indexOperation);
			bulkCreateOperationList.append('\n');
			bulkCreateOperationList.append(sampleJson);
			bulkCreateOperationList.append('\n');

		bulkCreateOperationAsString = ''.join(bulkCreateOperationList);
		self.logger.debug('bulkCreateOperationAsString: %s', bulkCreateOperationAsString);
		return bulkCreateOperationAsString;

	def httpRequest(self, url, method, payload):
		self.logger.debug('httpUrl: %s', url);
		self.logger.debug('httpMethod: %s', method);
		#print 'payload: ' + payload;

		request = urllib2.Request(url, data=payload);
		request.get_method = lambda: method;

		response = urllib2.urlopen(request);

		return json.loads(response.read());

	def formatElapsedTime(self, timeElapsedInSeconds):
	    hours = int(timeElapsedInSeconds / (60 * 60))
	    minutes = int((timeElapsedInSeconds % (60 * 60)) / 60)
	    seconds = timeElapsedInSeconds % 60.
	    return '{}:{:>02}:{:>05.2f}'.format(hours, minutes, seconds)

if __name__ == '__main__':
    populator = Populator('localhost:7200', 'endpoints', 'host', 'sample_host_data.json', 10000, 1000)
    populator.populate()

