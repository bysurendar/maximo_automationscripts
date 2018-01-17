from psdi.server import MXServer;
from java.sql import Statement;
from java.sql import PreparedStatement;
from java.sql import Connection;
from java.sql import ResultSet;
from psdi.mbo import Mbo;
import time;

mxserver = MXServer.getMXServer();
userInfo = mxserver.getSystemUserInfo();

# Get a Database COnnection from a Maximo Server
con = currentMbo.getMboServer().getDBConnection(userInfo.getConnectionKey());

# Get a schema name of the server to know from which instance it is running
schema = currentMbo.getMboServer().getSchemaOwner();
schema = schema.upper();

# ' is not acceptable as part of String. it is included in Backlash \ in List and converted to String
reportListQuery = ['select jobnum,filename, filetype, content, isStored from reportoutputcnt where isStored = 0'];
reportListQuery = ''.join(reportListQuery);

try:
	s = con.createStatement();
	rs1 = s.executeQuery(reportListQuery);
	
	while(rs1.next()):		
		jobNumber = rs1.getString('jobnum');
		fileName = rs1.getString('filename');
		fileType = rs1.getString('filetype');
		content = rs1.getBlob('content');		
	rs1.close();
	fileName = fileName + currentMbo.getMboServer().getDate().toString() + '.' + 'fileType';
	storeFileFromBlob (content,'filePath',fileName);
		
except	 Exception, e:
	print "Error on report output to folder processing...."
finally:
	cursor = con.cursor();
	cursor.execute ("update reportoutputcnt set isstored = 1 where jobnum = (?) ",(jobNumber)) ;
	cursor.commit();	 
	s.close();
	
def storeFileFromBlob (blobContent, outputFilePath, outputFileName):		
	fileGen = open(os.path.join(outputFilePath,outputFileName),'wb') ;
	fileGen.write(blobContent.decode('base64')) ;
	fileGen.close() ;