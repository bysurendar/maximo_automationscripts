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
currentSet = mxserver.getMboSet("COMPANIES",userInfo);
currentMbo = currentSet.getMbo(0);
con = currentMbo.getMboServer().getDBConnection(userInfo.getConnectionKey());

# Get a schema name of the server to know from which instance it is running
schema = currentMbo.getMboServer().getSchemaOwner();
schema = schema.upper();

str = ['Maximo 7.5 Database ',schema,' is working!','\n','\n'];

# ' is not acceptable as part of String. it is included in Backlash \ in List and converted to String
companyQuery = ['select companysetid, max(changedate) "changedate" from compmaster where changeby = ','\'','MXINTADM','\'','group by companysetid'];
companyQuery = ''.join(companyQuery);

outboundQuery = ['select orgid, siteid, count(*) "count" from INVOICE where issent = 0 and status = ','\'','APPR','\'', ' and orgid <>', '\'','BEDFORD','\'' ,' group by orgid, siteid']
outboundQuery = ''.join(outboundQuery);


try:
	s= con.createStatement();

	rs1 = s.executeQuery(companyQuery);
	str.append('--------------------------------------');
	str.append('\n');	
	str.append(' Company Set  |  Last Change Date');
	str.append('\n');
	str.append('--------------------------------------');
	str.append('\n');
	while(rs1.next()):
		companyset = rs1.getString('companysetid');
		date = rs1.getString('changedate');
		# below line changes the date String got from DB to a time
		#changetime = time.strptime(date, "%Y-%m-%d %I:%M:%S.0") 
		
		# Converts a time to a String of readable format
		#changetimeString = time.strftime("%d-%b-%Y %I:%M:%S",changetime)
		# Conversion of date to string depends on Database DATE format
		
		str.append(companyset);
		str.append('\t');
		str.append(changetimeString);
		str.append('\n');
	rs1.close();
	
	str.append('\n');


	rs2 = s.executeQuery(outboundQuery);
	str.append('--------------------------------------');
	str.append('\n');	
	str.append('ORGID    | SITEID   | COUNT');
	str.append('\n');
	str.append('--------------------------------------');
	str.append('\n');
	if (rs2.next()== False):
		str.append('--NO RECORDS--');
		str.append('\n');
	else:
		
		while True:
			orgid = rs2.getString('orgid');
			siteid = rs2.getString('siteid');
			count = rs2.getString('count');

			str.append(orgid);
			str.append('\t');
			str.append(siteid);
			str.append('\t');
			str.append(count);
			str.append('\n');
			if (rs3.next() == False) :
				break	
		rs2.close();
	str.append('\n');
    	s.close();
except	 Exception, e:
	str.append("error in db");
	
emailBody = ''.join(str);
emailTo = ['surendar_balasundaram@xxxxx.com'] ;
emailFrom = 'donotreply@in.ibm.com';
emailSubject = 'Daily Monitoring Check List';

MXServer.sendEMail(emailTo, emailFrom, emailSubject, emailBody);
