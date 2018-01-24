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
currentSet = mxserver.getMboSet("REPORTOUTPUTCNT",userInfo);
currentMbo = currentSet.getMbo(0);
con = currentMbo.getMboServer().getDBConnection(userInfo.getConnectionKey());

# ' is not acceptable as part of String. it is included in Backlash \ in List and converted to String
reportListQuery = ['select jobnum,filename, filetype, content, isstored from reportoutputcnt where isStored = 0'];
reportListQuery = ''.join(reportListQuery);

configData = MXServer.getMXServer().getConfig();
outputFilePath = configData.getProperty("custom.rptooutputfolder");

def storeFileFromBlob (contentInBytes, filePath):
	try: 
		print 'File Abs Name - ' + filePath
		fileGen = open(filePath,'wb') ;
		fileGen.write( contentInBytes ) ;
		fileGen.close() ;
	except Exception, e1:
		print "Error on file processing..."
	finally:
		fileGen.close() ;

try:
	s = con.createStatement();
	rs1 = s.executeQuery(reportListQuery);
	
	while(rs1.next()):		
		jobNumber = rs1.getString('jobnum');
		fileName = rs1.getString('filename');
		fileType = rs1.getString('filetype');
		content = rs1.getBlob('content');		
	rs1.close();
	fileName = fileName + jobNumber + '.' + fileType;
	print fileName
		
	absoluteFilePath = outputFilePath + fileName
	contentInBytes = content.getBytes(1,content.length()) ;
	
	storeFileFromBlob (contentInBytes, absoluteFilePath) ;
						
except	 Exception, e:
	print "Error on report output to folder processing...."
finally:

	reportUpdateQuery = ['update reportoutputcnt set isstored = 1 where jobnum=','\'',jobNumber,'\''];
	reportUpdateQuery = ''.join(reportUpdateQuery);
	
	updateStatment = s.execute(reportUpdateQuery);
	con.commit();	 
	s.close();
	con.close();