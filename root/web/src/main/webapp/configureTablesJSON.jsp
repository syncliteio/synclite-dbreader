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

<%@page import="java.net.URLEncoder"%>
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

<title>Configure Source DB Tables/Views</title>
</head>


<%
String errorMsg = request.getParameter("errorMsg");
Path syncLiteDeviceDir;
if (session.getAttribute("synclite-device-dir") != null) {
	syncLiteDeviceDir = Path.of(session.getAttribute("synclite-device-dir").toString());
} else {
	response.sendRedirect("syncLiteTerms.jsp");
	throw new javax.servlet.jsp.SkipPageException();
}

Path syncLiteDBReaderMetadataJsonPath = syncLiteDeviceDir.resolve("synclite_dbreader_metadata.json");

%>

<body>
	<%@include file="html/menu.html"%>	

	<div class="main">
		<h2>Configure DB Tables/Views</h2>
		<%
		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		}
		%>

		<form action="${pageContext.request.contextPath}/validateDBTables"	method="post">
			<table>
				<tbody>
					<tr>
						<td>
						You have chosen Table/View Configuration Method = JSON. Please follow below instructions to configure tables/views: <br><br>
						1. Open JSON file <b><%= syncLiteDBReaderMetadataJsonPath%></b> containing source DB table/view metadata in your favorite JSON editor <br>
						2. Specify/edit configurations for individual objects, save the file and click Next to validate the inputs. <br><br>
						Notes: <br>
						&nbsp;&nbsp;a. <b>allowed_columns</b> must be a JSON array of valid column name, data type, null constraint triplets. <br>
						&nbsp;&nbsp;b. <b>unique_key_columns, incremental_key_columns, mask_columns</b> must be comma separated list of valid column names. <br>
						&nbsp;&nbsp;c. <b>delete_condition</b> must be a valid equality predicate (representing soft delete condition for the given object) with lhs as a valid table column name and rhs as a constant value. <br>
						&nbsp;&nbsp;d. <b>select_conditions</b> must be a JSON array of valid SQL predicates on the given object. 
						&nbsp;&nbsp;e. <b>group_name</b> must be an alphanumeric string of length upto 64 characters. <br>
						&nbsp;&nbsp;f. <b>group_position</b> must be numeric value starting from 1. <br>
						&nbsp;&nbsp;g. <b>enable</b> must be a numeric value 1 or 0. <br>
						</td>
					</tr>
			</table>
			<center>
				<button type="submit" name="next">Next</button>
			</center>			
		</form>
	</div>
</body>
</html>