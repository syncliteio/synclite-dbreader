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
<%@page import="java.time.Instant"%>
<%@page import="java.io.File"%>
<%@page import="java.nio.file.Path"%>
<%@page import="java.io.BufferedReader"%>
<%@page import="java.io.FileReader"%>
<%@page import="java.util.List"%>
<%@page import="java.util.ArrayList"%>
<%@page import="java.util.HashMap"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<%@ page import="java.sql.*"%>
<%@ page import="org.sqlite.*"%>
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>

<script type="text/javascript">
</script>	
<title>Reset DBReader Job</title>
</head>


<%
String errorMsg = request.getParameter("errorMsg");

String keepJobConfiguration = "true";
if (request.getParameter("dbreader-keep-job-configuration") != null) {
	keepJobConfiguration = request.getParameter("dbreader-keep-job-configuration");
} 

String keepObjectConfiguration = "true";
if (request.getParameter("dbreader-keep-object-configuration") != null) {
	keepObjectConfiguration = request.getParameter("dbreader-keep-object-configuration");
} 

%>

<body>
	<%@include file="html/menu.html"%>	

	<div class="main">
		<h2>Reset DBReader Job</h2>
		<%
		if ((session.getAttribute("job-status") == null) || (session.getAttribute("synclite-device-dir") == null)) {
			out.println("<h4 style=\"color: red;\"> Please configure and start/load a DBReader job.</h4>");
			throw new javax.servlet.jsp.SkipPageException();		
		}

		if (session.getAttribute("syncite-dbreader-job-starter-scheduler") != null) {
			out.println("<h4 style=\"color: red;\"> Please stop the DBReader job scheduler to proceed with this operation.</h4>");
			throw new javax.servlet.jsp.SkipPageException();		
		}

		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		}
		%>

		<form action="${pageContext.request.contextPath}/resetJob" method="post">
			<table>
				<tbody>

					<tr>
						<td>
							Please note that resetting a job implies restarting replication for source DB tables/views from scratch upon reconfiguration and restarting the job. 
						</td>	
					</tr>

					<tr>
						<td>Keep Job Configuration</td>
						<td><select id="dbreader-keep-job-configuration" name="dbreader-keep-job-configuration" value="<%=keepJobConfiguration%>" title="Specify if the job configuration should be preserved.">
								<%
								if (keepJobConfiguration.equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}

								if (keepJobConfiguration.equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
						</select></td>
					</tr>				

					<tr>
						<td>Keep Table/View Configurations</td>
						<td><select id="dbreader-keep-object-configuration" name="dbreader-keep-object-configuration" value="<%=keepObjectConfiguration%>" title="Specify if the table/view configurations should be preserved.">
								<%
								if (keepObjectConfiguration.equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}

								if (keepObjectConfiguration.equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
						</select></td>
					</tr>				

				</table>
			<center>
				<button type="submit" name="reset">Reset Job</button>
			</center>
		</form>
	</div>
</body>
</html>