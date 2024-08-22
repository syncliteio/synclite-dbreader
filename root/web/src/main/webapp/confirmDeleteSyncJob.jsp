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

<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>
<title>Confirm Run Delete Sync Job</title>
</head>
<body>
	<%@include file="html/menu.html"%>
	<div class="main">
		<h2>Confirm Run Delete Sync Job</h2>
		<table>
			<tbody>
				<tr>
					<td>
						Notes:<br> 
						1. It is recommended to run Delete Synchronization Job during maintenance hours as it may take longer to finish depending on object sizes.<br>
						2. If you need to run Delete Synchronization only for specific tables then keep only those specific tables enabled on Manage Objects page before running this job.
					</td>	
				</tr>

				<% 
				if (session.getAttribute("syncite-dbreader-job-starter-scheduler") != null) {
					out.println("<h4 style=\"color: red;\"> Please stop the DBReader job scheduler to proceed with this operation.</h4>");
					throw new javax.servlet.jsp.SkipPageException();		
				}
				%>
				
				<tr>
					<td>
						Do you want to run Delete Sync job now ?
					</td>
				</tr>
			</tbody>
		</table>
		<form action="${pageContext.request.contextPath}/startDeleteSyncJob"	method="post">	
			<center>
				<button type="submit" name="next">Run</button>
			</center>			
		</form>
		
	</div>
</body>	
</html>