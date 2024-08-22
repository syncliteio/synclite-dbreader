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
<title>Confirm DBReader Job</title>
</head>
<% 
String numEnabledObjects = "0";
if (session.getAttribute("num-enabled-objects") != null) {
	numEnabledObjects = session.getAttribute("num-enabled-objects").toString();	
}
%>
<body>
	<%@include file="html/menu.html"%>
	<div class="main">
		<h2>Confirm DBReader Job</h2>
		<table>
			<tbody>
				<tr>
					<td>
						Selected object count : <%=numEnabledObjects%>
					</td>
				</tr>
				<tr>	
					<td>
						Please confirm to start the DBReader job
					</td>
				</tr>
			</tbody>
		</table>
		<form action="${pageContext.request.contextPath}/startJob?jobArgs=true"	method="post">	
			<center>
				<button type="submit" name="next">Start</button>
			</center>			
		</form>
		
	</div>
</body>	
</html>