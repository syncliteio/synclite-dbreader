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
<title>Configure Source DB Tables/Views</title>
</head>


<%
String errorMsg = request.getParameter("errorMsg");
HashMap properties = new HashMap<String, String>();

if (session.getAttribute("job-name") != null) {
	properties.put("job-name", session.getAttribute("job-name").toString());
} else {
	response.sendRedirect("syncLiteTerms.jsp");
}

if (session.getAttribute("synclite-device-dir") != null) {
	properties.put("synclite-device-dir", session.getAttribute("synclite-device-dir").toString());
} else {
	response.sendRedirect("syncLiteTerms.jsp");
}

if (request.getParameter("dbreader-configure-incremental-keys") != null) {
	properties.put("dbreader-configure-incremental-keys", request.getParameter("dbreader-configure-incremental-keys"));
} else {
	properties.put("dbreader-configure-incremental-keys", "true");
	if (session.getAttribute("src-dbreader-method") != null) {
		if (session.getAttribute("src-dbreader-method").equals("LOG_BASED")) {
			properties.put("dbreader-configure-incremental-keys", "false");
		}
	}		
}

if (request.getParameter("dbreader-configure-soft-delete-conditions") != null) {
	properties.put("dbreader-configure-soft-delete-conditions", request.getParameter("dbreader-configure-soft-delete-conditions"));
} else {
	properties.put("dbreader-configure-soft-delete-conditions", "false");	
}

if (request.getParameter("dbreader-configure-mask-columns") != null) {
	properties.put("dbreader-configure-mask-columns", request.getParameter("dbreader-configure-mask-columns"));
} else {
	properties.put("dbreader-configure-mask-columns", "false");	
}

if (request.getParameter("dbreader-configure-select-conditions") != null) {
	properties.put("dbreader-configure-select-conditions", request.getParameter("dbreader-configure-select-conditions"));
} else {
	properties.put("dbreader-configure-select-conditions", "false");	
}

if (request.getParameter("dbreader-configure-object-groups") != null) {
	properties.put("dbreader-configure-object-groups", request.getParameter("dbreader-configure-object-groups"));
} else {
	properties.put("dbreader-configure-object-groups", "false");	
}

if (request.getParameter("dbreader-skip-object-configurations") != null) {
	properties.put("dbreader-skip-object-configurations", request.getParameter("dbreader-skip-object-configurations"));
} else {
	properties.put("dbreader-skip-object-configurations", "false");
}

if (request.getParameter("dbreader-object-configuration-method") != null) {
	properties.put("dbreader-object-configuration-method", request.getParameter("dbreader-object-configuration-method"));
} else {
	properties.put("dbreader-object-configuration-method", "GUI");
}

if (request.getParameter("dbreader-skip-object-configurations") == null) {
	//Read configs from conf file if they are present
	Path propsPath = Path.of(properties.get("synclite-device-dir").toString(), "synclite_dbreader.conf");
	BufferedReader reader = null;
	try {
		if (Files.exists(propsPath)) {
			reader = new BufferedReader(new FileReader(propsPath.toFile()));
			String line = reader.readLine();
			while (line != null) {
				line = line.trim();
				if (line.trim().isEmpty()) {
					line = reader.readLine();
					continue;
				}
				if (line.startsWith("#")) {
					line = reader.readLine();
					continue;
				}
				String[] tokens = line.split("=");
				if (tokens.length < 2) {
					if (tokens.length == 1) {
						if (!line.startsWith("=")) {
							properties.put(tokens[0].trim().toLowerCase(), line.substring(line.indexOf("=") + 1, line.length()).trim());
						}
					}
				} else {
					properties.put(tokens[0].trim().toLowerCase(), line.substring(line.indexOf("=") + 1, line.length()).trim());
				}
				line = reader.readLine();
			}
			reader.close();
		}
	} catch (Exception e) {
		if (reader != null) {
			reader.close();
		}
		throw e;
	}
}

%>

<body>
	<%@include file="html/menu.html"%>	

	<div class="main">
		<h2>Configure Source DB Tables/Views</h2>
		<%
		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		}
		%>

		<form action="${pageContext.request.contextPath}/validateDBTableOptions" method="post">
			<table>
				<tbody>

					<tr>
						<td>Skip Table/View Configurations</td>
						<td><select id="dbreader-skip-object-configurations" name="dbreader-skip-object-configurations" value="<%=properties.get("dbreader-skip-object-configurations")%>" title="Specify if you want to skip configuring individual tables/views for data extractions. Setting this to true will select all discovered tables/views for replication.">
								<%
								if (properties.get("dbreader-skip-object-configurations").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}

								if (properties.get("dbreader-skip-object-configurations").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">true</option>");
								}
								%>
						</select></td>
					</tr>				

					<tr>
						<td>Table/View Configuration Method</td>
						<td><select id="dbreader-object-configuration-method" name="dbreader-object-configuration-method" value="<%=properties.get("dbreader-object-configuration-method")%>" title="Specify tables/views objet configuration method as either SyncLite GUI or JSON.">
								<%
								if (properties.get("dbreader-object-configuration-method").equals("GUI")) {
									out.println("<option value=\"GUI\" selected>GUI</option>");
								} else {
									out.println("<option value=\"GUI\">GUI</option>");
								}

								if (properties.get("dbreader-object-configuration-method").equals("JSON")) {
									out.println("<option value=\"JSON\" selected>JSON</option>");
								} else {
									out.println("<option value=\"JSON\">JSON</option>");
								}
								%>
						</select></td>
					</tr>				

					<tr>
						<td>Configure Incremental Key Columns</td>
						<td><select id="dbreader-configure-incremental-keys" name="dbreader-configure-incremental-keys" value="<%=properties.get("dbreader-configure-incremental-keys")%>"  title="Specify if you need to configure per object incremental key columns on the Configure DB Tables/Views page.">
								<%
								if (properties.get("dbreader-configure-incremental-keys").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("dbreader-configure-incremental-keys").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Configure Soft Delete Conditions</td>
						<td><select id="dbreader-configure-soft-delete-conditions" name="dbreader-configure-soft-delete-conditions" value="<%=properties.get("dbreader-configure-soft-delete-conditions")%>"  title="Specify if you need to configure per object soft delete conditions on the Configure DB Tables/Views page.">
								<%
								if (properties.get("dbreader-configure-soft-delete-conditions").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("dbreader-configure-soft-delete-conditions").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Configure Mask Columns</td>
						<td><select id="dbreader-configure-mask-columns" name="dbreader-configure-mask-columns" value="<%=properties.get("dbreader-configure-mask-columns")%>"  title="Specify if you need to configure per object mask columns on the Configure DB Tables/Views page.">
								<%
								if (properties.get("dbreader-configure-mask-columns").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("dbreader-configure-mask-columns").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Configure Select Conditions</td>
						<td><select id="dbreader-configure-select-conditions" name="dbreader-configure-select-conditions" value="<%=properties.get("dbreader-configure-select-conditions")%>"  title="Specify if you need to configure per object select conditions on the Configure DB Tables/Views page.">
								<%
								if (properties.get("dbreader-configure-select-conditions").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("dbreader-configure-select-conditions").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Configure Object Groups</td>
						<td><select id="dbreader-configure-object-groups" name="dbreader-configure-object-groups" value="<%=properties.get("dbreader-configure-object-groups")%>"  title="Specify if you need to configure object grouping for encforcing serialized replication of dependent objects on the Configure DB Tables/Views page.">
								<%
								if (properties.get("dbreader-configure-object-groups").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("dbreader-configure-object-groups").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
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