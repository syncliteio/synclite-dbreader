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

<title>Manage Tables/Views</title>
</head>
<body>
	<%@include file="html/menu.html"%>	

	<div class="main">
		<h2>Manage Tables/Views</h2>
		Note: Please stop the job and scheduler if running before executing this operation.<br>
		<%
			
			if ((session.getAttribute("job-status") == null) || (session.getAttribute("synclite-device-dir") == null)) {
				out.println("<h4 style=\"color: red;\"> Please configure and start/load a DBReader job.</h4>");
				throw new javax.servlet.jsp.SkipPageException();		
			}
		
			if (session.getAttribute("syncite-dbreader-job-starter-scheduler") != null) {
				out.println("<h4 style=\"color: red;\"> Please stop the DBReader job scheduler to proceed with this operation.</h4>");
				throw new javax.servlet.jsp.SkipPageException();		
			}
		
			Path readerMetadataDBPath = Path.of(session.getAttribute("synclite-device-dir").toString(), "synclite_dbreader_metadata.db");		
			if (!Files.exists(readerMetadataDBPath)) {
				out.println("<h4 style=\"color: red;\"> Metadata file for DBReader job is missing .</h4>");
				throw new javax.servlet.jsp.SkipPageException();				
			}		
		%>
		
		<%
		String errorMsg = request.getParameter("errorMsg");
		%>

		<%
		//read table configs
		
		HashMap<String, Integer> enabled = new HashMap<String, Integer>();
		HashMap<String, Integer> reloadSchemaOnNextRestart= new HashMap<String, Integer>();
		HashMap<String, Integer> reloadSchemaOnEachRestart = new HashMap<String, Integer>();
		HashMap<String, Integer> reloadObjectOnNextRestart= new HashMap<String, Integer>();
		HashMap<String, Integer> reloadObjectOnEachRestart = new HashMap<String, Integer>();
		
		Path metdataFilePath = Path.of(session.getAttribute("synclite-device-dir").toString(), "synclite_dbreader_metadata.db");					
		Class.forName("org.sqlite.JDBC");

		try(Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metdataFilePath)) {
			try(Statement stat = conn.createStatement()){					
				try(ResultSet rs = stat.executeQuery("SELECT object_name, enable FROM src_object_info")) {
					while (rs.next()) {
						String objName = rs.getString(1);
						Integer enable = rs.getInt(2);
						enabled.put(objName, enable);
						reloadSchemaOnNextRestart.put(objName, 0);
						reloadSchemaOnEachRestart.put(objName, 0);
						reloadObjectOnNextRestart.put(objName, 0);
						reloadObjectOnEachRestart.put(objName, 0);						
					}
				}
				
				try(ResultSet rs = stat.executeQuery("SELECT object_name, reload_schema_on_next_restart, reload_schema_on_each_restart, reload_object_on_next_restart, reload_object_on_each_restart FROM src_object_reload_configurations")) {
					while (rs.next()) {
						String objName = rs.getString(1);
						Integer rsnr = rs.getInt(2);
						Integer rser = rs.getInt(3);
						Integer ronr = rs.getInt(4);
						Integer roer = rs.getInt(5);						
						reloadSchemaOnNextRestart.put(objName, rsnr);
						reloadSchemaOnEachRestart.put(objName, rser);
						reloadObjectOnNextRestart.put(objName, ronr);
						reloadObjectOnEachRestart.put(objName, roer);
					}
				}				
			}
		} catch (Exception e) {
			errorMsg = "Failed to load source DB object metadata info from metadata file : " + e.getMessage() + ". Please try reloading the page.";	
		}
		
		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		}

		%>

		<form action="${pageContext.request.contextPath}/manageObjects" method="post">
			<table>
				<tbody>
				<tr></tr>
				<tr>
					<th>Object</th>
					<th>Enable <input type="checkbox" id="enable-all" name="enable-all"></th>
					<th>Reload Schema On Next Restart <input type="checkbox" id="rsnr-all" name="rsnr-all"></th>
					<th>Reload Schema On Each Restart <input type="checkbox" id="rser-all" name="rser-all"></th>
					<th>Reload Object On Next Restart <input type="checkbox" id="ronr-all" name="rsor-all"></th>
					<th>Reload Object On Each Restart <input type="checkbox" id="roer-all" name="roer-all"></th>
				</tr>
				<%
					int idx = 0;
					for (HashMap.Entry<String, Integer> entry : enabled.entrySet()) {
						out.println("<tr>");									
						String objectName = entry.getKey();
						out.println("<td>" + objectName + "</td>");
						out.println("<input type=\"hidden\" id=\"object-name-" + idx + "\" name=\"object-name-" + idx + "\" value=\"" + objectName + "\" />");

						Integer enable = entry.getValue();
						if (enable == 1) {
							out.println("<td><input type=\"checkbox\" id=\"enable-" + idx + "\" name=\"enable-" + idx + "\" value=\"" + "1" + "\" checked/></td>");
						} else {
							out.println("<td><input type=\"checkbox\" id=\"enable-" + idx + "\" name=\"enable-" + idx + "\" value=\"" + "0" + "\"/></td>");
						}

						Integer rsnr = reloadSchemaOnNextRestart.get(objectName);
						if (rsnr == 1) {
							out.println("<td><input type=\"checkbox\" id=\"rsnr-" + idx + "\" name=\"rsnr-" + idx + "\" value=\"" + "1" + "\" checked/></td>");
						} else {
							out.println("<td><input type=\"checkbox\" id=\"rsnr-" + idx + "\" name=\"rsnr-" + idx + "\" value=\"" + "0" + "\"/></td>");
						}

						Integer rser = reloadSchemaOnEachRestart.get(objectName);
						if (rser == 1) {
							out.println("<td><input type=\"checkbox\" id=\"rser-" + idx + "\" name=\"rser-" + idx + "\" value=\"" + "1" + "\" checked/></td>");
						} else {
							out.println("<td><input type=\"checkbox\" id=\"rser-" + idx + "\" name=\"rser-" + idx + "\" value=\"" + "0" + "\"/></td>");
						}

						Integer ronr = reloadObjectOnNextRestart.get(objectName);
						if (ronr == 1) {
							out.println("<td><input type=\"checkbox\" id=\"ronr-" + idx + "\" name=\"ronr-" + idx + "\" value=\"" + "1" + "\" checked/></td>");
						} else {
							out.println("<td><input type=\"checkbox\" id=\"ronr-" + idx + "\" name=\"ronr-" + idx + "\" value=\"" + "0" + "\"/></td>");
						}

						Integer roer = reloadObjectOnEachRestart.get(objectName);
						if (roer == 1) {
							out.println("<td><input type=\"checkbox\" id=\"roer-" + idx + "\" name=\"roer-" + idx + "\" value=\"" + "1" + "\" checked/></td>");
						} else {
							out.println("<td><input type=\"checkbox\" id=\"roer-" + idx + "\" name=\"roer-" + idx + "\" value=\"" + "0" + "\"/></td>");
						}

						out.println("</tr>");
						++idx;
					}
					out.println("<input type=\"hidden\" id=\"num-objects\" name=\"num-objects\" value=\"" + idx + "\"/>");
					%>
				</tbody>
			</table>
			<center>
				<button type="submit" name="save">Save</button>
			</center>
		</form>
	</div>
	
<script type="text/javascript">
	// Get references to the checkboxes
	const enableAllCheckbox = document.getElementById("enable-all");
	const enableIndividualCheckboxes = document.querySelectorAll('input[type="checkbox"][name^="enable-"]');
	
	// Add an event listener to the "select-all" checkbox
	enableAllCheckbox.addEventListener("change", function() {
	  const isChecked = enableAllCheckbox.checked;
	  // Set the state of individual checkboxes to match the "select-all" checkbox
	  enableIndividualCheckboxes.forEach(function(checkbox) {
	    checkbox.checked = isChecked;
	  });
	});

	
	const rsnrAllCheckbox = document.getElementById("rsnr-all");
	const rsnrIndividualCheckboxes = document.querySelectorAll('input[type="checkbox"][name^="rsnr-"]');
	
	// Add an event listener to the "select-all" checkbox
	rsnrAllCheckbox.addEventListener("change", function() {
	  const isChecked = rsnrAllCheckbox.checked;
	  // Set the state of individual checkboxes to match the "select-all" checkbox
	  rsnrIndividualCheckboxes.forEach(function(checkbox) {
	    checkbox.checked = isChecked;
	  });
	});

	const rserAllCheckbox = document.getElementById("rser-all");
	const rserIndividualCheckboxes = document.querySelectorAll('input[type="checkbox"][name^="rser-"]');
	
	// Add an event listener to the "select-all" checkbox
	rserAllCheckbox.addEventListener("change", function() {
	  const isChecked = rserAllCheckbox.checked;
	  // Set the state of individual checkboxes to match the "select-all" checkbox
	  rserIndividualCheckboxes.forEach(function(checkbox) {
	    checkbox.checked = isChecked;
	  });
	});

	const ronrAllCheckbox = document.getElementById("ronr-all");
	const ronrIndividualCheckboxes = document.querySelectorAll('input[type="checkbox"][name^="ronr-"]');
	
	// Add an event listener to the "select-all" checkbox
	ronrAllCheckbox.addEventListener("change", function() {
	  const isChecked = ronrAllCheckbox.checked;
	  // Set the state of individual checkboxes to match the "select-all" checkbox
	  ronrIndividualCheckboxes.forEach(function(checkbox) {
	    checkbox.checked = isChecked;
	  });
	});

	const roerAllCheckbox = document.getElementById("roer-all");
	const roerIndividualCheckboxes = document.querySelectorAll('input[type="checkbox"][name^="roer-"]');
	
	// Add an event listener to the "select-all" checkbox
	roerAllCheckbox.addEventListener("change", function() {
	  const isChecked = roerAllCheckbox.checked;
	  // Set the state of individual checkboxes to match the "select-all" checkbox
	  roerIndividualCheckboxes.forEach(function(checkbox) {
	    checkbox.checked = isChecked;
	  });
	});

</script>
	
</body>
</html>