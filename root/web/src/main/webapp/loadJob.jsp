<%-- 
    Copyright (c) 2024 mahendra.chavan@syncLite.io, all rights reserved.

    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
    in compliance with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software distributed under the License
    is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
    or implied.  See the License for the specific language governing permissions and limitations
    under the License.
--%>

<%@page import="java.nio.file.Files"%>
<%@page import="java.nio.file.Path"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>
<title>Load SyncLite DB Reader Job</title>
</head>

<% 
	String syncliteDeviceDir = request.getParameter("synclite-device-dir");
	String errorMsg = request.getParameter("errorMsg");
	String jobName = request.getParameter("job-name");
	
	if (jobName != null) {
		//Check if specified jobName is in correct format
		
		if (jobName.length() > 16 ) {
			errorMsg = "Job name must be upto 16 characters in length";
		}
		if (!jobName.matches("[a-zA-Z0-9-_]+")) {
			errorMsg = "Specified job name is invalid. Allowed characters are alphanumeric, hyphen or underscrore characters.";
		}		
	} else {
		if (session.getAttribute("job-name") != null) {
			jobName = session.getAttribute("job-name").toString();
		} else {
			jobName = "job1";
		}
	}

	//Path rootDir = Path.of(getServletContext().getRealPath("/")).getRoot();
	//Path defaultSyncLiteDeviceDir = Path.of(rootDir.toString(), "synclite", "db");
	Path defaultSyncLiteDeviceDir = Path.of(System.getProperty("user.home"), "synclite", jobName, "db");
	syncliteDeviceDir = defaultSyncLiteDeviceDir.toString();
%>

<body>
	<%@include file="html/menu.html"%>
	<div class="main">
		<h2>Load DBReader Job</h2>
		<%	
		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">Failed to load job : " + errorMsg + "</h4>");
		}
		%>
	
		<form action="${pageContext.request.contextPath}/loadJob" method="post">
			<table>
				<tbody>
					<tr>
						<td>Job Name</td>
						<td><input type="text" size = 30 id="job-name" name="job-name" value="<%=jobName%>" onchange="this.form.action='loadJob.jsp'; this.form.submit();" title="Specify SyncLite dbreader job name. Make sure that the job name specified here is same as the one specified in SyncLite consolidator"/></td>
					</tr>
				
					<tr>
						<td>SyncLite Device Directory</td>
						<td><input type="text" size=60 id="synclite-device-dir"
							name="synclite-device-dir"
							value="<%=syncliteDeviceDir%>"
							title="Specify SyncLite device directory"/>
						</td>
					</tr>

				</tbody>
			</table>
			<center>
				<button type="submit" name="next">Load</button>
			</center>			
		</form>
	</div>
</body>
</html>