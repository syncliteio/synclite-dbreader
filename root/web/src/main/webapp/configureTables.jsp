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

String configureIncrementalKeys = "false";
if (session.getAttribute("dbreader-configure-incremental-keys") != null) {
	configureIncrementalKeys = session.getAttribute("dbreader-configure-incremental-keys").toString();
}

String configureSoftDeleteConditions = "false";
if (session.getAttribute("dbreader-configure-soft-delete-conditions") != null) {
	configureSoftDeleteConditions = session.getAttribute("dbreader-configure-soft-delete-conditions").toString();
}

String configureMaskColumns = "false";
if (session.getAttribute("dbreader-configure-mask-columns") != null) {
	configureMaskColumns = session.getAttribute("dbreader-configure-mask-columns").toString();
}

String configureSelectConditions = "false";
if (session.getAttribute("dbreader-configure-select-conditions") != null) {
	configureSelectConditions = session.getAttribute("dbreader-configure-select-conditions").toString();
}

String configureObjectGroups = "false";
if (session.getAttribute("dbreader-configure-object-groups") != null) {
	configureObjectGroups = session.getAttribute("dbreader-configure-object-groups").toString();
}

String srcObjectNamePattern = "%";
if (request.getSession().getAttribute("src-object-name-pattern") != null) {
	srcObjectNamePattern = request.getSession().getAttribute("src-object-name-pattern").toString();
}

String selectConditionsPlaceHolderText = "[\n" +
        "  \"col1 < 100 AND col2 == 'HR' '\",\n" +
        "  \"col1 >=100 AND col2 == 'HR' \",\n" +
        "]";

selectConditionsPlaceHolderText = selectConditionsPlaceHolderText.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
.replace("\"", "&quot;").replace("'", "&#39;");

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
				<tr></tr>
				<tr>
					<th>Enable <input type="checkbox" id="enable-all" name="enable-all"></th>
					<th>Object</th>
					<th>Type</th>
					<th>Allowed Columns</th>
					<th>Primary/Unique Key Columns</th>
					<%	
						if (configureIncrementalKeys.equals("true")) {
							out.println("<th>Incremental Key Columns</th>");
						}
						if (configureObjectGroups.equals("true")) {
							out.println("<th>Group</th>");
							out.println("<th>Position</th>");
						}
						if (configureSoftDeleteConditions.equals("true")) {
							out.println("<th>Soft Delete Condition</th>");
						}			
						if (configureMaskColumns.equals("true")) {
							out.println("<th>Mask Columns</th>");
						}
						if (configureSelectConditions.equals("true")) {
							out.println("<th>Select Conditions</th>");
						}
					 %>
				</tr>

				<%
					Path metdataFilePath = Path.of(session.getAttribute("synclite-device-dir").toString(), "synclite_dbreader_metadata.db");					
					Class.forName("org.sqlite.JDBC");
					int idx = 0;
					try(Connection conn = DriverManager.getConnection("jdbc:sqlite:" + metdataFilePath)) {
						try(Statement stat = conn.createStatement()) {				
							try(ResultSet rs = stat.executeQuery("SELECT object_name, object_type, allowed_columns, unique_key_columns, incremental_key_columns, group_name, group_position, delete_condition, mask_columns, select_conditions, enable FROM src_object_info WHERE object_name LIKE '" + srcObjectNamePattern + "' ORDER BY object_name")) {
								while (rs.next()) {
									out.println("<tr>");
									if (request.getParameter("name-" + idx) != null) {
										if (request.getParameter("enable-" + idx) != null) {
											out.println("<td><input type=\"checkbox\" id=\"enable-" + idx + "\" name=\"enable-" + idx + "\" value=\"" + "1" + "\" checked/></td>");					
										} else {
											out.println("<td><input type=\"checkbox\" id=\"enable-" + idx + "\" name=\"enable-" + idx + "\" value=\"" + "0" + "\"/></td>");	
										}
									} else {
										if (rs.getInt("enable") == 1) {
											out.println("<td><input type=\"checkbox\" id=\"enable-" + idx + "\" name=\"enable-" + idx + "\" value=\"" + rs.getInt("enable") + "\" checked/></td>");							
										} else {
											out.println("<td><input type=\"checkbox\" id=\"enable-" + idx + "\" name=\"enable-" + idx + "\" value=\"" + rs.getInt("enable") + "\" /></td>");
										}
									}
									out.println("<td>" + rs.getString("object_name") + "</td>");
									out.println("<td>" + rs.getString("object_type") + "</td>");

									out.println("<input type=\"hidden\" id=\"object-name-" + idx + "\" name=\"object-name-" + idx + "\" value=\"" + rs.getString("object_name") + "\"/>");
									out.println("<input type=\"hidden\" id=\"object-type-" + idx + "\" name=\"object-type-" + idx + "\" value=\"" + rs.getString("object_type") + "\"/>");
									
									if (request.getParameter("allowed-columns-" + idx) != null) {
										String val = request.getParameter("allowed-columns-" + idx);
										out.println("<td><textarea id=\"allowed-columns-" + idx + "\" name=\"allowed-columns-" + idx + "\" rows=\"4\" cols=\"30\"" + "\" value=\"" + val + "\" title=\"Specify a JSON array of column names along with their schema definitions. If you need to block certain columns from publishing, please remove them from this list.\"/>" + val + "</textarea></td>");
									} else {
										out.println("<td><textarea id=\"allowed-columns-" + idx + "\" name=\"allowed-columns-" + idx + "\" rows=\"4\" cols=\"30\"" + "\" value=\"" + rs.getString("allowed_columns") + "\" title=\"Specify a JSON array of column names along with their schema definitions. If you need to block certain columns from publishing, please remove them from this list.\"/>" + rs.getString("allowed_columns") + "</textarea></td>");							
									}
			
									if (request.getParameter("unique-key-columns-" + idx) != null) {
										String val = request.getParameter("unique-key-columns-" + idx);
										out.println("<td><input type=\"text\" size=\"20\" id=\"unique-key-columns-" + idx + "\" name=\"unique-key-columns-" + idx + "\" value=\"" + val + "\" title=\"Specify a comma separated list of columns which define a unique key for this object, even if it is not explicitly defined as primary/unique key in the source DB for this object.\"/></td>");
									} else {
										String uniqueKeyCols = "";
										if (rs.getString("unique_key_columns") != null) {
											uniqueKeyCols = rs.getString("unique_key_columns");
										}		
										out.println("<td><input type=\"text\" size=\"20\" id=\"unique-key-columns-" + idx + "\" name=\"unique-key-columns-" + idx + "\" value=\"" + uniqueKeyCols + "\" title=\"Specify a comma separated list of columns which define a unique key for this object, even if it is not explicitly defined as primary/unique key in the source DB for this object.\"/></td>");
									}

									if (configureIncrementalKeys.equals("true")) {
										if (request.getParameter("incremental-key-columns-" + idx) != null) {
											String val = request.getParameter("incremental-key-columns-" + idx);
											out.println("<td><input type=\"text\" size=\"20\" id=\"incremental-key-columns-" + idx + "\" name=\"incremental-key-columns-" + idx + "\" value=\"" + val + "\" title=\"Specify a comma separated ist of incremental key columns such as timestamp or numeric columns which are assigned monotonically increasing value on each UPDATE and INSERT operation executed on this object. SyncLite strongly recommends to create indexes on these columns for improved replication performance.\"/></td>");
										} else {
											String incrementalKeyCols = "";
											if (rs.getString("incremental_key_columns") != null) {
												incrementalKeyCols = rs.getString("incremental_key_columns");
											}						
											out.println("<td><input type=\"text\" size=\"20\" id=\"incremental-key-columns-" + idx + "\" name=\"incremental-key-columns-" + idx + "\" value=\"" + incrementalKeyCols + "\" title=\"Specify a comma separated ist of incremental key columns such as timestamp or numeric columns which are assigned monotonically increasing value on each UPDATE and INSERT operation executed on this object. SyncLite strongly recommends to create indexes on these columns for improved replication performance.\"/></td>");
										}
									}

									if (configureObjectGroups.equals("true")) {
										if (request.getParameter("group-name-" + idx) != null) {
											String val = request.getParameter("group-name-" + idx);
											out.println("<td><input type=\"text\" size=\"10\" id=\"group-name-" + idx + "\" name=\"group-name-" + idx + "\" value=\"" + val + "\" title=\"Specify object group name. You can group dependent object in the same group to enforce serialized replication of all the objects in the same group.\"/></td>");
										} else {
											String group = "";
											if (rs.getString("group_name") != null) {
												group = rs.getString("group_name");
											}						
											out.println("<td><input type=\"text\" size=\"10\" id=\"group-name-" + idx + "\" name=\"group-name-" + idx + "\" value=\"" + group + "\" title=\"Specify object group name. You can group dependent objects in the same group to enforce serialized replication of all the objects in the same group.\"/></td>");
										}
	
										if (request.getParameter("group-position-" + idx) != null) {
											String val = request.getParameter("group-position-" + idx);
											out.println("<td><input type=\"number\" size=\"10\" id=\"group-position-" + idx + "\" name=\"group-position-" + idx + "\" value=\"" + val + "\" title=\"Specify order/position of this object in the specified object group.\"/></td>");
										} else {
											String position = "";
											if (rs.getString("group_position") != null) {
												position = rs.getString("group_position");
											}						
											out.println("<td><input type=\"number\" size=\"10\" id=\"group-position-" + idx + "\" name=\"group-position-" + idx + "\" value=\"" + position + "\" title=\"Specify order/position of this object in the specified object group.\"/></td>");
										}
									}

									if (configureSoftDeleteConditions.equals("true")) {
										if (request.getParameter("delete-condition-" + idx) != null) {
											String val = request.getParameter("delete-condition-" + idx);
											out.println("<td><input type=\"text\" size=\"20\" id=\"delete-condition-" + idx + "\" name=\"delete-condition-" + idx + "\" value=\"" + val + "\" title=\"Specify deletion propagation condition as an equality SQL predicate. E.g. if you have a column is_deleted CHAT(1) and you are implementing soft deletion mechanism by just marking these records as 'Y' is_deleted then specify is_deleted = 'Y'. This criteria will be used to delete the records periodically on destination database.\"/></td>");
										} else {
											String deleteCondition = "";
											if (rs.getString("delete_condition") != null) {
												deleteCondition = rs.getString("delete_condition");
											}
											out.println("<td><input type=\"text\" size=\"20\" id=\"delete-condition-" + idx + "\" name=\"delete-condition-" + idx + "\" value=\"" + deleteCondition + "\" title=\"Specify deletion propagation condition as an equality SQL predicate. E.g. if you have a column is_deleted CHAT(1) and you are implementing soft deletion mechanism by just marking these records as 'Y' is_deleted then specify is_deleted = 'Y'. This condition will be used to delete records periodically on destination database.\"/></td>");
										}								
									}
									
									if (configureMaskColumns.equals("true")) {
										if (request.getParameter("mask-columns-" + idx) != null) {
											String val = request.getParameter("mask-columns-" + idx);
											out.println("<td><input type=\"text\" size=\"20\" id=\"mask-columns-" + idx + "\" name=\"mask-columns-" + idx + "\" value=\"" + val + "\" title=\"Specify a comma separated list of columns to be masked during data extraction\"/></td>");
										} else {
											String maskCols = "";
											if (rs.getString("mask_columns") != null) {
												maskCols = rs.getString("mask_columns");
											}		
											out.println("<td><input type=\"text\" size=\"20\" id=\"mask-columns-" + idx + "\" name=\"mask-columns-" + idx + "\" value=\"" + maskCols + "\" title=\"Specify a comma separated list of columns to be masked during data extraction\"/></td>");
										}
									}

									if (configureSelectConditions.equals("true")) {
										if (request.getParameter("select-conditions-" + idx) != null) {
											String val = request.getParameter("select-conditions-" + idx);
											out.println("<td><textarea placeholder=\""+ selectConditionsPlaceHolderText + "\"" + " id=\"select-conditions-" + idx + "\" name=\"select-conditions-" + idx + "\" rows=\"4\" cols=\"35\"" + "\" value=\"" + val + "\" title=\"Specify SQL predicates in JSON array format to selectively extract data. If you specify multiple entries in this JSON array, then parallel data extraction is employed on this object for each specified condition.\"/>" + val + "</textarea></td>");
										} else {
											String selectConditions = "";
											String selectConditionsTxt = "";
											if (rs.getString("select_conditions") != null) {
												selectConditions = rs.getString("select_conditions");
												selectConditionsTxt = URLEncoder.encode(selectConditions);
											}
											
											out.println("<td><textarea placeholder=\""+ selectConditionsPlaceHolderText + "\"" + " id=\"select-conditions-" + idx + "\" name=\"select-conditions-" + idx + "\" rows=\"4\" cols=\"35\"" + "\" value=\"" + selectConditionsTxt + "\" title=\"Specify SQL predicates in JSON array format to selectively extract data. If you specify multiple predicates, then parallel data extraction will be employed for each specified condition for this object.\"/>" + selectConditions + "</textarea></td>");					
										}
									}
									out.println("</tr>");
									
									++idx;
								}
							}
						}
					}

					out.println("<input type=\"hidden\" id=\"num-objects\" name=\"num-objects\" value=\"" + idx + "\"/>");
					%>
					<tr>
						<td colspan="3">
							<b>Total <%=idx%> Objects</b>
						</td>
					</tr>
				</tbody>
			</table>
			<center>
				<button type="submit" name="next">Next</button>
			</center>			
		</form>
	</div>
	
<script type="text/javascript">
	// Get references to the checkboxes
	const enableAllCheckbox = document.getElementById("enable-all");
	const individualCheckboxes = document.querySelectorAll('input[type="checkbox"][name^="enable-"]');
	
	// Add an event listener to the "enable-all" checkbox
	enableAllCheckbox.addEventListener("change", function() {
	  const isChecked = enableAllCheckbox.checked;
	  // Set the state of individual checkboxes to match the "enable-all" checkbox
	  individualCheckboxes.forEach(function(checkbox) {
	    checkbox.checked = isChecked;
	  });
	});
</script>
	
</body>
</html>