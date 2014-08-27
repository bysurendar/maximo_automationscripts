from psdi.server import MXServer;
from java.sql import Statement;
from java.sql import PreparedStatement;
from java.sql import Connection;
from java.sql import ResultSet;
from psdi.mbo import Mbo;
import time;

emailTo = ['surendar_balasundaram@xxxxx.com'] ;

emailFrom = 'donotreply@in.ibm.com';
emailSubject = 'Daily Monitoring Check List';

mxserver = MXServer.getMXServer();
userInfo = mxserver.getSystemUserInfo();

currentSet = mxserver.getMboSet("COMPANIES",userInfo);
currentMbo = currentSet.getMbo(0);

con = currentMbo.getMboServer().getDBConnection(userInfo.getConnectionKey());
schema = currentMbo.getMboServer().getSchemaOwner();
schema = schema.upper();

str = ['Maximo 7.5 Database ',schema,' is working!','\n','\n'];


companyQuery = ['select companysetid, max(changedate) "changedate" from compmaster where changeby = ','\'','MXINTADM','\'','group by companysetid'];
companyQuery = ''.join(companyQuery);
ldapQuery = 'select max(STATUSDATE) "statusdate" from person'; 
outboundQuery = ['select orgid, siteid, count(*) "count" from INVOICE where issent = 0 and status = ','\'','APPR','\'', ' and orgid <>', '\'','BEDFORD','\'' ,' group by orgid, siteid']
outboundQuery = ''.join(outboundQuery);
publicReportList = ['select a.reportname,b.importedby,d.emailaddress from reportauth a , reportdesign b ,person c, email d where a.reportname = b.reportname and a.groupname = ','\'','EVERYONE','\'','  and a.reportnum > 1597 and b.importedby = c.personid and c.personid = D.PERSONID order by a.reportnum desc']
publicReportList = ''.join(publicReportList);

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
		changetime = time.strptime(date, "%Y-%m-%d %I:%M:%S.0") 		
		changetimeString = time.strftime("%d-%b-%Y %I:%M:%S",changetime)
		str.append(companyset);
		str.append('\t');
		str.append(changetimeString);
		str.append('\n');
	rs1.close();
	
	str.append('\n');

	rs2 = s.executeQuery(ldapQuery);
	str.append('--------------------------------------');
	str.append('\n');	
	str.append('DSID LDAP Last Updated at');
	str.append('\n');
	str.append('--------------------------------------');
	str.append('\n');
	while(rs2.next()):
		personDate = rs2.getString('statusdate');		
		statustime = time.strptime(personDate, "%Y-%m-%d %I:%M:%S.0") 		
		statustimeString = time.strftime("%d-%b-%Y %I:%M:%S",statustime)
		str.append(statustimeString);
		str.append('\n');
	rs2.close();
	str.append('\n');

	rs3 = s.executeQuery(outboundQuery);
	str.append('--------------------------------------');
	str.append('\n');	
	str.append('ORGID    | SITEID   | COUNT');
	str.append('\n');
	str.append('--------------------------------------');
	str.append('\n');
	if (rs3.next()== False):
		str.append('--PASS--');
		str.append('\n');
	else:
		#str.append('--NOT WORKING--');
                #str.append('\n');

		while True:
			orgid = rs3.getString('orgid');
			siteid = rs3.getString('siteid');
			count = rs3.getString('count');

			str.append(orgid);
			str.append('\t');
			str.append(siteid);
			str.append('\t');
			str.append(count);
			str.append('\n');
			if (rs3.next() == False) :
				break	
		rs3.close();
	str.append('\n');
    
	rs4 = s.executeQuery(publicReportList);
	str.append('--------------------------------------');
	str.append('\n');	
	str.append('REPORT ');
	str.append('\t');
	str.append('| CREATEDBY | EMAILID');
	str.append('\n');
	str.append('--------------------------------------');
	str.append('\n');
	if (rs4.next()== False):
		str.append('--PASS--');
	else:		
		while True:
			report = rs4.getString('reportname');
			
			person = rs4.getString('importedby');
			email = rs4.getString('emailaddress');

			str.append(report);
			str.append('\t');
			
			str.append(person);
			str.append('\t');
			str.append(email);
			str.append('\n');
			if (rs4.next() == False) :
				break	
		rs4.close();
	str.append('\n');
	s.close();
except	 Exception, e:
	str.append("error in db");
	
emailBody = ''.join(str);
MXServer.sendEMail(emailTo, emailFrom, emailSubject, emailBody);
