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

<%@page import="java.nio.charset.Charset"%>
<%@page import="java.net.URLEncoder"%>
<%@page import="java.nio.file.Path"%>
<%@page import="java.nio.file.Files"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<%@ page import="java.sql.*"%>
<%@ page import="org.sqlite.*"%>
<%@page import="org.apache.commons.io.FileUtils"%>
<%@page import="java.time.Instant"%>
<%@page import="java.time.LocalDateTime"%>
<%@page import="java.time.LocalDate"%>
<%@page import="java.time.ZoneId"%>
<%@page import="java.time.format.DateTimeFormatter"%>

<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>

<title>SyncLite DBReader Table/View Statistics</title>
</head>

<script type="text/javascript">

function autoRefreshSetTimeout() {
    const refreshInterval = parseInt(document.getElementById("refresh-interval").value);
    
    if (!isNaN(refreshInterval)) {
    	const val = refreshInterval * 1000;
    	if (val === 0) {
    		const timeoutObj = setTimeout("autoRefresh()", 1000);
    		clearTimeout(timeoutObj);    		
    	} else {    		
    		setTimeout("autoRefresh()", val);
    	}
	}	
}

function autoRefresh() {
	document.forms['tableForm'].submit();
}

</script>

<body onload="autoRefreshSetTimeout()">
	<%@include file="html/menu.html"%>	
	<div class="main">
		<h2>SyncLite DBReader Table/View Statistics</h2>
		<%
			if (session.getAttribute("job-status") == null) {
				out.println("<h4 style=\"color: red;\"> Please configure and start/load a dbreader job.</h4>");
				throw new javax.servlet.jsp.SkipPageException();						
			}

	    	if ((request.getSession().getAttribute("dbreader-enable-statistics-collector") == null) || (!request.getSession().getAttribute("dbreader-enable-statistics-collector").toString().equals("true"))) {
				out.println("<h4 style=\"color: red;\"> DBReader statistics collector not enabled.</h4>");
				throw new javax.servlet.jsp.SkipPageException();						
			}

			Path statsFilePath = Path.of(session.getAttribute("synclite-device-dir").toString(), "synclite_dbreader_statistics.db");		
			if (!Files.exists(statsFilePath)) {
				out.println("<h4 style=\"color: red;\">DBReader statistics file is missing</h4>");
				throw new javax.servlet.jsp.SkipPageException();
			}

			Long numRecords = 100L;
			if (request.getParameter("numRecords") != null) {
				try {
					numRecords = Long.valueOf(request.getParameter("numRecords").trim());
				} catch (NumberFormatException e) {
					numRecords = 100L;
				}
			}			

			String showDetailedStatistics = "false";
			if (request.getParameter("showDetailedStatistics") != null) {
				showDetailedStatistics = request.getParameter("showDetailedStatistics").toString();
			}
			int refreshInterval = 5;
			if (request.getParameter("refresh-interval") != null) {
				try {
					refreshInterval = Integer.valueOf(request.getParameter("refresh-interval").toString());
				} catch (Exception e) {
					refreshInterval = 5;
				}
			}
		%>
			<center>
				<form name="tableForm" id="tableForm" method="post">
					<table>
						<tr>
						<td>
						Please note that DBReader statistics may differ from real-time data due to asynchronous updates. 
						</td>
						</tr>
						<tr>
							<td>
								Showing Top <input type="number" name = "numRecords" id = "numRecords" value = <%= numRecords%>> Records						 
							</td>
							<td>
								Show Detailed Statistics
								<select id="showDetailedStatistics" name = "showDetailedStatistics"  value = <%= showDetailedStatistics%>>
								<% 
									if (showDetailedStatistics.equals("true")) {
										out.println("<option value=\"true\" selected>true</option>");
									} else {
										out.println("<option value=\"true\">true</option>");
									}
									if (showDetailedStatistics.equals("false")) {
										out.println("<option value=\"false\" selected>false</option>");
									} else {
										out.println("<option value=\"false\">false</option>");
									}
								%>	
								</select>						 
							</td>
							<td>				
								<input type="button" name="Go" id="Go" value="Go" onclick = "this.form.submit()">
							</td>
							<td>				
								<div class="pagination">
									REFRESH IN <input type="text" id="refresh-interval" name="refresh-interval" value ="<%=refreshInterval%>" size="1" onchange="autoRefreshSetTimeout()"> SECONDS
								</div>
							</td>
						</tr>
					</table>
					<table>
					<tr></tr>
					<tr>
						<th>ID</th>
						<th>Object</th>
						<th>Read Type</th>
						<th>Start Time</th>
						<th>End Time</th>
						<th>Status</th>
						<th>Records Fetched</th>
						<th>Incremental Key Values</th>
						<th>Last Published SyncLite TS</th>
						<% 
							if (showDetailedStatistics.equals("true")) {
								out.println("<th>Total Records Fetched</th>");
								out.println("<th>Total Object Reloads</th>");
								out.println("<th>Total Schema Reloads</th>");
								out.println("<th>Total Inferred DDLs</th>");
								out.println("<th>Last Delete Sync Start Time</th>");
								out.println("<th>Last Delete Sync End Time</th>");
								out.println("<th>Last Delete Sync Keys Fetched</th>");
							}
						%>
					</tr>
					<%
						String sql = "SELECT object, read_type, last_read_start_time, last_read_end_time, last_read_records_fetched, last_read_incremental_key_column_values, last_delete_sync_start_time, last_delete_sync_end_time, last_delete_sync_keys_fetched, last_read_status, last_read_status_description, total_records_fetched, total_inferred_ddls, total_full_reload_count, total_schema_reload_count, last_published_commit_id FROM object_statistics ORDER BY object ASC LIMIT " + numRecords;
						long idx = 1;
					    DateTimeFormatter fullDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
					    DateTimeFormatter fullDateTimeFormatterMillis = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
					    DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
					    DateTimeFormatter timeFormatterMillis = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
					    LocalDate today = LocalDate.now();

					    Class.forName("org.sqlite.JDBC");

						try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + statsFilePath)) {
							try (Statement stmt = conn.createStatement()) {
								try (ResultSet rs = stmt.executeQuery(sql)) {
									while (rs.next()) {
										out.println("<tr>");
										out.println("<td>" + idx + "</td>");
										out.println("<td>" + rs.getString("object") + "</td>");
										String readType = rs.getString("read_type");
										out.println("<td>" + readType + "</td>");

										String lastReadStartTimeStr = "";
										long lastReadStartTime = rs.getLong("last_read_start_time");
										if (lastReadStartTime > 0) {
											LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(lastReadStartTime), ZoneId.systemDefault());											 
										    if (localDateTime.toLocalDate().isEqual(today)) {
												lastReadStartTimeStr = localDateTime.format(timeFormatter);
										    } else {
												lastReadStartTimeStr = localDateTime.format(fullDateTimeFormatter);
										    }
										}
										out.println("<td>" + lastReadStartTimeStr + "</td>");

										String lastReadEndTimeStr = "";
										long lastReadEndTime = rs.getLong("last_read_end_time");
										if (lastReadEndTime > 0) {
											LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(lastReadEndTime), ZoneId.systemDefault());											 
										    if (localDateTime.toLocalDate().isEqual(today)) {
										    	lastReadEndTimeStr = localDateTime.format(timeFormatter);
										    } else {
										    	lastReadEndTimeStr = localDateTime.format(fullDateTimeFormatter);
										    }
										}
										out.println("<td>" + lastReadEndTimeStr + "</td>");

										String statusDesc = rs.getString("last_read_status_description");
										if (statusDesc.isBlank()) {
											out.println("<td>" + rs.getString("last_read_status") + "</td>");
										} else {
											out.println("<td>" + rs.getString("last_read_status") +  " (" + statusDesc +")" + "</td>");
										}
										out.println("<td>" + rs.getLong("last_read_records_fetched") + "</td>");
										if (readType.equals("INCREMENTAL")) {
											out.println("<td>" + rs.getString("last_read_incremental_key_column_values") + "</td>");
										} else {
											out.println("<td></td>");
										}
										String lastPublishedChangeTSStr = "";
										long lastPublishedChangeTS = rs.getLong("last_published_commit_id");
										if (lastPublishedChangeTS > 0) {
											LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(lastPublishedChangeTS), ZoneId.systemDefault());											 
										    if (localDateTime.toLocalDate().isEqual(today)) {
										    	lastPublishedChangeTSStr = localDateTime.format(timeFormatterMillis);
										    } else {
										    	lastPublishedChangeTSStr = localDateTime.format(fullDateTimeFormatterMillis);
										    }
										}
										out.println("<td>" + lastPublishedChangeTSStr + "</td>");

										if (showDetailedStatistics.equals("true")) {											 
											out.println("<td>" + rs.getLong("total_records_fetched") + "</td>");
											out.println("<td>" + rs.getLong("total_full_reload_count") + "</td>");
											out.println("<td>" + rs.getLong("total_schema_reload_count") + "</td>");
											out.println("<td>" + rs.getLong("total_inferred_ddls") + "</td>");
											
											String lastDeleteSyncStartTimeStr = "";
											long lastDeleteSyncStartTime = rs.getLong("last_delete_sync_start_time");
											if (lastDeleteSyncStartTime > 0) {
												LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(lastDeleteSyncStartTime), ZoneId.systemDefault());											 
											    if (localDateTime.toLocalDate().isEqual(today)) {
											    	lastDeleteSyncStartTimeStr = localDateTime.format(timeFormatter);
											    } else {
											    	lastDeleteSyncStartTimeStr = localDateTime.format(fullDateTimeFormatter);
											    }
											}
											out.println("<td>" + lastDeleteSyncStartTimeStr + "</td>");

											String lastDeleteSyncEndTimeStr = "";
											long lastDeleteSyncEndTime = rs.getLong("last_delete_sync_end_time");
											if (lastDeleteSyncEndTime > 0) {
												LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.valueOf(lastDeleteSyncEndTime)), ZoneId.systemDefault());											 
											    if (localDateTime.toLocalDate().isEqual(today)) {
											    	lastDeleteSyncEndTimeStr = localDateTime.format(timeFormatter);
											    } else {
											    	lastDeleteSyncEndTimeStr = localDateTime.format(fullDateTimeFormatter);
											    }
											}
											out.println("<td>" + lastDeleteSyncEndTimeStr + "</td>");
											out.println("<td>" + rs.getLong("last_delete_sync_keys_fetched") + "</td>");											
										}
										++idx;
									}
								}
							}
						} catch(Exception e) {
							out.println("<h4 style=\"color: red;\">Failed to read dbreader statistics : " + e.getMessage() + ". Please tryr refreshing the page.</h4>");
						}
					%>
			</table>			
		</center>
	</div>
</body>
</html>	