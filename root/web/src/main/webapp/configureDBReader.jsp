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

<%@page import="com.synclite.dbreader.web.SrcType"%>
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
<%@page import="java.nio.file.StandardCopyOption"%>

<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href=css/SyncLiteStyle.css>

<script type="text/javascript">
	function resetFields() {
		var element = document.getElementById("src-connection-string");		
		if (element) {
		  element.parentNode.removeChild(element);
		}
		element = document.getElementById("src-user");		
		if (element) {
		  element.parentNode.removeChild(element);
		}
		element = document.getElementById("src-password");		
		if (element) {
		  element.parentNode.removeChild(element);
		}
		element = document.getElementById("src-database");		
		if (element) {
		  element.parentNode.removeChild(element);
		}
		element = document.getElementById("src-schema");		
		if (element) {
		  element.parentNode.removeChild(element);
		}
		element = document.getElementById("src-dblink");		
		if (element) {
		  element.parentNode.removeChild(element);
		}

	}
	
	function resetDBReaderInterval() {
		var element = document.getElementById("src-dbreader-interval-s");		
		if (element) {
		  element.parentNode.removeChild(element);
		}
	}
</script>	
<title>Configure DBReader Job</title>
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

if (request.getParameter("synclite-logger-configuration-file") != null) {
	properties.put("synclite-logger-configuration-file", request.getParameter("synclite-logger-configuration-file"));
} else {
	Path defaultLoggerConfPath = Path.of(properties.get("synclite-device-dir").toString(), "synclite_logger.conf");
	properties.put("synclite-logger-configuration-file", defaultLoggerConfPath);	
}

if (request.getParameter("dbreader-retry-failed-objects") != null) {
	properties.put("dbreader-retry-failed-objects", request.getParameter("dbreader-retry-failed-objects"));
} else {
	properties.put("dbreader-retry-failed-objects", "true");
}

if (request.getParameter("dbreader-failed-object-retry-interval-s") != null) {
	properties.put("dbreader-failed-object-retry-interval-s", request.getParameter("dbreader-failed-object-retry-interval-s"));
} else {
	properties.put("dbreader-failed-object-retry-interval-s", "10");
}

if (request.getParameter("src-dbreader-batch-size") != null) {
	properties.put("src-dbreader-batch-size", request.getParameter("src-dbreader-batch-size"));
} else {
	properties.put("src-dbreader-batch-size", "100000");	
}

if (request.getParameter("src-dbreader-method") != null) {
	properties.put("src-dbreader-method", request.getParameter("src-dbreader-method"));
} else {
	properties.put("src-dbreader-method", "INCREMENTAL");
}

if (request.getParameter("src-dbreader-interval-s") != null) {
	properties.put("src-dbreader-interval-s", request.getParameter("src-dbreader-interval-s"));
} else {
	if (properties.get("src-dbreader-method").equals("INCREMENTAL")) {
		properties.put("src-dbreader-interval-s", "300");
	} else {
		properties.put("src-dbreader-interval-s", "5");
	}
}

if (request.getParameter("dbreader-stop-after-first-iteration") != null) {
	properties.put("dbreader-stop-after-first-iteration", request.getParameter("dbreader-stop-after-first-iteration"));
} else {
	properties.put("dbreader-stop-after-first-iteration", "false");
}

if (request.getParameter("src-dbreader-processors") != null) {
	properties.put("src-dbreader-processors", request.getParameter("src-dbreader-processors"));
} else {
	properties.put("src-dbreader-processors", Runtime.getRuntime().availableProcessors());	
}

if (request.getParameter("src-connection-timeout-s") != null) {
	properties.put("src-connection-timeout-s", request.getParameter("src-connection-timeout-s"));
} else {
	properties.put("src-connection-timeout-s", 30);	
}

if (request.getParameter("src-type") != null) {
	properties.put("src-type", request.getParameter("src-type"));
} else {
	properties.put("src-type", "SQLITE");
}

if (request.getParameter("src-connection-initialization-stmt") != null) {
	properties.put("src-connection-initialization-stmt", request.getParameter("src-connection-initialization-stmt"));
} else {
	properties.put("src-connection-initialization-stmt", "");
}

if (request.getParameter("src-csv-files-with-headers") != null) {
	properties.put("src-csv-files-with-headers", request.getParameter("src-csv-files-with-headers"));
} else {
	properties.put("src-csv-files-with-headers", "true");
}

if (request.getParameter("src-csv-files-field-delimiter") != null) {
	properties.put("src-csv-files-field-delimiter", request.getParameter("src-csv-files-field-delimiter"));
} else {
	properties.put("src-csv-files-field-delimiter", ",");
}

if (request.getParameter("src-csv-files-record-delimiter") != null) {
	properties.put("src-csv-files-record-delimiter", request.getParameter("src-csv-files-record-delimiter"));
} else {
	properties.put("src-csv-files-record-delimiter", "\\r\\n");
}

if (request.getParameter("src-csv-files-escape-character") != null) {
	properties.put("src-csv-files-escape-character", request.getParameter("src-csv-files-escape-character"));
} else {
	properties.put("src-csv-files-escape-character", "&quot;");
}

if (request.getParameter("src-csv-files-quote-character") != null) {
	properties.put("src-csv-files-quote-character", request.getParameter("src-csv-files-quote-character"));
} else {
	properties.put("src-csv-files-quote-character", "&quot;");
}

if (request.getParameter("src-csv-files-null-string") != null) {
	properties.put("src-csv-files-null-string", request.getParameter("src-csv-files-null-string"));
} else {
	properties.put("src-csv-files-null-string", "null");
}

if (request.getParameter("src-csv-files-ignore-empty-lines") != null) {
	properties.put("src-csv-files-ignore-empty-lines", request.getParameter("src-csv-files-ignore-empty-lines"));
} else {
	properties.put("src-csv-files-ignore-empty-lines", "true");
}

if (request.getParameter("src-csv-files-trim-fields") != null) {
	properties.put("src-csv-files-trim-fields", request.getParameter("src-csv-files-trim-fields"));
} else {
	properties.put("src-csv-files-trim-fields", "false");
}

if (request.getParameter("src-file-storage-type") != null) {
	properties.put("src-file-storage-type", request.getParameter("src-file-storage-type"));
} else {
	properties.put("src-file-storage-type", "LOCAL_FS");
}

if (request.getParameter("src-file-storage-local-fs-directory") != null) {
	properties.put("src-file-storage-local-fs-directory", request.getParameter("src-file-storage-local-fs-directory"));
} else {
	properties.put("src-file-storage-local-fs-directory", "");
}

if (request.getParameter("src-file-storage-s3-url") != null) {
	properties.put("src-file-storage-s3-url", request.getParameter("src-file-storage-s3-url"));
} else {
	properties.put("src-file-storage-s3-url", "");
}

if (request.getParameter("src-file-storage-s3-bucket-name") != null) {
	properties.put("src-file-storage-s3-bucket-name", request.getParameter("src-file-storage-s3-bucket-name"));
} else {
	properties.put("src-file-storage-s3-bucket-name", "");
}

if (request.getParameter("src-file-storage-s3-access-key") != null) {
	properties.put("src-file-storage-s3-access-key", request.getParameter("src-file-storage-s3-access-key"));
} else {
	properties.put("src-file-storage-s3-access-key", "");
}

if (request.getParameter("src-file-storage-s3-secret-key") != null) {
	properties.put("src-file-storage-s3-secret-key", request.getParameter("src-file-storage-s3-secret-key"));
} else {
	properties.put("src-file-storage-s3-secret-key", "");
}

if (request.getParameter("src-file-storage-sftp-host") != null) {
	properties.put("src-file-storage-sftp-host", request.getParameter("src-file-storage-sftp-host"));
} else {
	properties.put("src-file-storage-sftp-host", "localhost");
}

if (request.getParameter("src-file-storage-sftp-port") != null) {
	properties.put("src-file-storage-sftp-port", request.getParameter("src-file-storage-sftp-port"));
} else {
	properties.put("src-file-storage-sftp-port", "22");
}

if (request.getParameter("src-file-storage-sftp-directory") != null) {
	properties.put("src-file-storage-sftp-directory", request.getParameter("src-file-storage-sftp-directory"));
} else {
	properties.put("src-file-storage-sftp-directory", "");
}

if (request.getParameter("src-file-storage-sftp-user") != null) {
	properties.put("src-file-storage-sftp-user", request.getParameter("src-file-storage-sftp-user"));
} else {
	properties.put("src-file-storage-sftp-user", "");
}

if (request.getParameter("src-file-storage-sftp-password") != null) {
	properties.put("src-file-storage-sftp-password", request.getParameter("src-file-storage-sftp-password"));
} else {
	properties.put("src-file-storage-sftp-password", "");
}

if (request.getParameter("src-connection-string") != null) {
	properties.put("src-connection-string", request.getParameter("src-connection-string"));
} else {
	switch (properties.get("src-type").toString()) {
	case "DUCKDB":
		String defaultConnStrDuckDB = "jdbc:duckdb:"
		+ Path.of(System.getProperty("user.home"), "synclite", "src_db.duckdb");
		properties.put("src-connection-string", defaultConnStrDuckDB);
		break;
	case "CSV":
		String defaultConnStrCSV = Path.of(System.getProperty("user.home"), "synclite", "dataFiles").toString();
		properties.put("src-connection-string", defaultConnStrCSV);
		break;
	case "MONGODB":
		String defaultConnStrMongoDB = 	"mongodb://localhost:27017/?readConcernLevel=majority";
		properties.put("src-connection-string", defaultConnStrMongoDB);
		break;
	case "MYSQL":
		String defaultConnStrMySQL = "jdbc:mysql://127.0.0.1:3306/sourceschema?user=synclite&password=synclite";
		properties.put("src-connection-string", defaultConnStrMySQL);
		break;
	case "POSTGRESQL":
		String defaultConnStrPG = "jdbc:postgresql://127.0.0.1:5432/sourcedb?user=synclite&password=synclite";
		properties.put("src-connection-string", defaultConnStrPG);
		break;
	case "SQLITE":
		String defaultConnStrSQLite = "jdbc:sqlite:"
		+ Path.of(System.getProperty("user.home"), "synclite", "src_db.sqlite");
		properties.put("src-connection-string", defaultConnStrSQLite);
		break;
	default:
		properties.put("src-connection-string", "");
	}
}

if (request.getParameter("src-database") != null) {
	properties.put("src-database", request.getParameter("src-database"));
} else {
	properties.put("src-database", "sourcedb");
}

if (request.getParameter("src-schema") != null) {
	properties.put("src-schema", request.getParameter("src-schema"));
} else {
	properties.put("src-schema", "sourceschema");
}

if (request.getParameter("src-dblink") != null) {
	properties.put("src-dblink", request.getParameter("src-dblink"));
} else {
	properties.put("src-dblink", "");
}

if (request.getParameter("src-user") != null) {
	properties.put("src-user", request.getParameter("src-user"));
} else {
	properties.put("src-user", "");
}

if (request.getParameter("src-password") != null) {
	properties.put("src-password", request.getParameter("src-password"));
} else {
	properties.put("src-password", "");
}

if (request.getParameter("src-object-type") != null) {
	properties.put("src-object-type", request.getParameter("src-object-type"));
} else {
	properties.put("src-object-type", "TABLE");
}

if (request.getParameter("src-object-name-pattern") != null) {
	properties.put("src-object-name-pattern", request.getParameter("src-object-name-pattern"));
} else {
	properties.put("src-object-name-pattern", "%");
}

if (request.getParameter("src-object-metadata-read-method") != null) {
	properties.put("src-object-metadata-read-method", request.getParameter("src-object-metadata-read-method"));
} else {
	properties.put("src-object-metadata-read-method", "NATIVE");
}

if (request.getParameter("src-column-metadata-read-method") != null) {
	properties.put("src-column-metadata-read-method", request.getParameter("src-column-metadata-read-method"));
} else {
	properties.put("src-column-metadata-read-method", "NATIVE");
}

if (request.getParameter("src-constraint-metadata-read-method") != null) {
	properties.put("src-constraint-metadata-read-method", request.getParameter("src-constraint-metadata-read-method"));
} else {
	properties.put("src-constraint-metadata-read-method", "NATIVE");
}

if (request.getParameter("dbreader-trace-level") != null) {
	properties.put("dbreader-trace-level", request.getParameter("dbreader-trace-level"));
} else {
	properties.put("dbreader-trace-level", "INFO");
}

if (request.getParameter("dbreader-enable-statistics-collector") != null) {
	properties.put("dbreader-enable-statistics-collector",
	request.getParameter("dbreader-enable-statistics-collector"));
} else {
	properties.put("dbreader-enable-statistics-collector", "true");
}

if (request.getParameter("dbreader-update-statistics-interval-s") != null) {
	properties.put("dbreader-update-statistics-interval-s",
	request.getParameter("dbreader-update-statistics-interval-s"));
} else {
	properties.put("dbreader-update-statistics-interval-s", "5");
}

if (request.getParameter("jvm-arguments") != null) {
	properties.put("jvm-arguments", request.getParameter("jvm-arguments"));
} else {
	properties.put("jvm-arguments", "");
}

if (request.getParameter("src-infer-schema-changes") != null) {
	properties.put("src-infer-schema-changes", request.getParameter("src-infer-schema-changes"));
} else {
	properties.put("src-infer-schema-changes", "false");
}

if (request.getParameter("src-infer-object-drop") != null) {
	properties.put("src-infer-object-drop", request.getParameter("src-infer-object-drop"));
} else {
	properties.put("src-infer-object-drop", "false");
}

if (request.getParameter("src-infer-object-create") != null) {
	properties.put("src-infer-object-create", request.getParameter("src-infer-object-create"));
} else {
	properties.put("src-infer-object-create", "false");
}

if (request.getParameter("src-reload-objects") != null) {
	properties.put("src-reload-objects", request.getParameter("src-reload-objects"));
} else {
	properties.put("src-reload-objects", "false");
}

if (request.getParameter("src-reload-objects-on-each-job-restart") != null) {
	properties.put("src-reload-objects-on-each-job-restart",
	request.getParameter("src-reload-objects-on-each-job-restart"));
} else {
	properties.put("src-reload-objects-on-each-job-restart", "false");
}

if (request.getParameter("src-reload-object-schemas") != null) {
	properties.put("src-reload-object-schemas", request.getParameter("src-reload-object-schemas"));
} else {
	properties.put("src-reload-object-schemas", "false");
}

if (request.getParameter("src-reload-object-schemas-on-each-job-restart") != null) {
	properties.put("src-reload-object-schemas-on-each-job-restart",
	request.getParameter("src-reload-object-schemas-on-each-job-restart"));
} else {
	properties.put("src-reload-object-schemas-on-each-job-restart", "false");
}

if (request.getParameter("src-read-null-incremental-key-records") != null) {
	properties.put("src-read-null-incremental-key-records",
	request.getParameter("src-read-null-incremental-key-records"));
} else {
	properties.put("src-read-null-incremental-key-records", "false");
}

if (request.getParameter("src-compute-max-incremental-key-in-db") != null) {
	properties.put("src-compute-max-incremental-key-in-db",
	request.getParameter("src-compute-max-incremental-key-in-db"));
} else {
	properties.put("src-compute-max-incremental-key-in-db", "true");
}

if (request.getParameter("src-quote-object-names") != null) {
	properties.put("src-quote-object-names", request.getParameter("src-quote-object-names"));
} else {
	properties.put("src-quote-object-names", "false");
}

if (request.getParameter("src-quote-column-names") != null) {
	properties.put("src-quote-column-names", request.getParameter("src-quote-column-names"));
} else {
	properties.put("src-quote-column-names", "false");
}

if (request.getParameter("src-use-catalog-scope-resolution") != null) {
	properties.put("src-use-catalog-scope-resolution", request.getParameter("src-use-catalog-scope-resolution"));
} else {
	properties.put("src-use-catalog-scope-resolution", "true");
}

if (request.getParameter("src-use-schema-scope-resolution") != null) {
	properties.put("src-use-schema-scope-resolution", request.getParameter("src-use-schema-scope-resolution"));
} else {
	properties.put("src-use-schema-scope-resolution", "true");
}

if (request.getParameter("src-default-unique-key-column-list") != null) {
	properties.put("src-default-unique-key-column-list", request.getParameter("src-default-unique-key-column-list"));
} else {
	properties.put("src-default-unique-key-column-list", "");
}

if (request.getParameter("src-default-incremental-key-column-list") != null) {
	if (properties.get("src-type").toString().equals("CSV")) {
		if (request.getParameter("src-default-incremental-key-column-list").toString().isBlank()) {
			properties.put("src-default-incremental-key-column-list", "FILE_CREATION_TIME");
		} else {
			properties.put("src-default-incremental-key-column-list", request.getParameter("src-default-incremental-key-column-list"));
		}
	} else {
		properties.put("src-default-incremental-key-column-list", request.getParameter("src-default-incremental-key-column-list"));
	}
} else {
	if (properties.get("src-type").toString().equals("CSV")) {
		properties.put("src-default-incremental-key-column-list", "FILE_CREATION_TIME");
	} else {
		properties.put("src-default-incremental-key-column-list", "");
	}
}

if (request.getParameter("src-default-soft-delete-condition") != null) {
	properties.put("src-default-soft-delete-condition", request.getParameter("src-default-soft-delete-condition"));
} else {
	properties.put("src-default-soft-delete-condition", "");
}

if (request.getParameter("src-default-mask-column-list") != null) {
	properties.put("src-default-mask-column-list", request.getParameter("src-default-mask-column-list"));
} else {
	properties.put("src-default-mask-column-list", "");
}

if (request.getParameter("src-numeric-value-mask") != null) {
	properties.put("src-numeric-value-mask", request.getParameter("src-numeric-value-mask"));
} else {
	properties.put("src-numeric-value-mask", "9");
}

if (request.getParameter("src-alphabetic-value-mask") != null) {
	properties.put("src-alphabetic-value-mask", request.getParameter("src-alphabetic-value-mask"));
} else {
	properties.put("src-alphabetic-value-mask", "X");
}

if (request.getParameter("src-query-timestamp-conversion-function") != null) {
	properties.put("src-query-timestamp-conversion-function", request.getParameter("src-query-timestamp-conversion-function"));
} else {
	properties.put("src-query-timestamp-conversion-function", "");
}

if (request.getParameter("src-timestamp-incremental-key-initial-value") != null) {
	properties.put("src-timestamp-incremental-key-initial-value", request.getParameter("src-timestamp-incremental-key-initial-value"));
} else {
	properties.put("src-timestamp-incremental-key-initial-value", "0001-01-01 00:00:00");
}

if (request.getParameter("src-numeric-incremental-key-initial-value") != null) {
	properties.put("src-numeric-incremental-key-initial-value", request.getParameter("src-numeric-incremental-key-initial-value"));
} else {
	properties.put("src-numeric-incremental-key-initial-value", "0");
}

if (request.getParameter("src-dbreader-object-record-limit") != null) {
	properties.put("src-dbreader-object-record-limit", request.getParameter("src-dbreader-object-record-limit"));
} else {
	properties.put("src-dbreader-object-record-limit", "0");
}


if (request.getParameter("src-dbreader-interval-s") == null) {
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
					properties.put(tokens[0].trim().toLowerCase(),
							line.substring(line.indexOf("=") + 1, line.length()).trim());
				}
			}
		} else {
			properties.put(tokens[0].trim().toLowerCase(),
					line.substring(line.indexOf("=") + 1, line.length()).trim());
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

	//Set this to false by default on reconfiguring the job and let user explcitly turn this ON.
	properties.put("src-reload-objects", "false");
	properties.put("src-reload-object-schemas", "false");

}

String conf = "";
if (request.getParameter("synclite-logger-configuration") != null) {
	conf = request.getParameter("synclite-logger-configuration");
} else if (Files.exists(Path.of(properties.get("synclite-logger-configuration-file").toString()))) {
	conf = Files.readString(Path.of(properties.get("synclite-logger-configuration-file").toString()));
} else {
	StringBuilder confBuilder = new StringBuilder();
	String newLine = System.getProperty("line.separator");

	String stageDir = Path
	.of(System.getProperty("user.home"), "synclite", properties.get("job-name").toString(), "stageDir")
	.toString();
	String commandDir = Path
	.of(System.getProperty("user.home"), "synclite", properties.get("job-name").toString(), "commandDir")
	.toString();

	confBuilder.append("#==============Device Stage Properties==================");
	confBuilder.append(newLine);
	confBuilder.append("local-data-stage-directory=").append(stageDir);
	confBuilder.append(newLine);
	confBuilder.append("#local-data-stage-directory=<path/to/local/data/stage/directory>");
	confBuilder.append(newLine);
	confBuilder.append("local-command-stage-directory=").append(commandDir);
	confBuilder.append(newLine);
	confBuilder.append(
	"#local-command-stage-directory=<path/to/local/command/stage/directory  #specify if device command handler is enabled>");
	confBuilder.append(newLine);
	confBuilder.append("destination-type=FS");
	confBuilder.append(newLine);
	confBuilder.append("#destination-type=<FS|MS_ONEDRIVE|GOOGLE_DRIVE|SFTP|MINIO|KAFKA|S3>");
	confBuilder.append(newLine);
	confBuilder.append(newLine);
	confBuilder.append("#==============SFTP Configuration=================");
	confBuilder.append(newLine);
	confBuilder.append("#sftp:host=<host name of SFTP server to receive shipped devices and device logs>");
	confBuilder.append(newLine);
	confBuilder.append("#sftp:port=<port number of SFTP server>");
	confBuilder.append(newLine);
	confBuilder.append("#sftp:user-name=<user name to connect to remote host>");
	confBuilder.append(newLine);
	confBuilder.append("#sftp:password=<password>");
	confBuilder.append(newLine);
	confBuilder.append(
	"#sftp:remote-data-stage-directory=<remote data stage directory name that will stage device directories>");
	confBuilder.append(newLine);
	confBuilder.append(
	"#sftp:remote-command-stage-directory=<remote command directory name which will hold command files sent by consolidator if device command handler is enabled>");
	confBuilder.append(newLine);
	confBuilder.append(newLine);
	confBuilder.append("#==============MinIO  Configuration=================");
	confBuilder.append(newLine);
	confBuilder.append("#minio:endpoint=<MinIO endpoint to upload devices>");
	confBuilder.append(newLine);
	confBuilder.append("#minio:access-key=<MinIO access key>");
	confBuilder.append(newLine);
	confBuilder.append("#minio:secret-key=<MinIO secret key>");
	confBuilder.append(newLine);
	confBuilder
	.append("#minio:data-stage-bucket-name=<MinIO data stage bucket name that will host device directories>");
	confBuilder.append(newLine);
	confBuilder.append(
	"#minio:command-stage-bucket-name=<MinIO command stage bucket name that will hold command files sent by SyncLite Consolidator>");
	confBuilder.append(newLine);
	confBuilder.append(newLine);
	confBuilder.append("#==============S3 Configuration=====================");
	confBuilder.append(newLine);
	confBuilder.append("#s3:endpoint=https://s3-<region>.amazonaws.com");
	confBuilder.append(newLine);
	confBuilder.append("#s3:access-key=<S3 access key>");
	confBuilder.append(newLine);
	confBuilder.append("#s3:secret-key=<S3 secret key>");
	confBuilder.append(newLine);
	confBuilder.append("#s3:data-stage-bucket-name=<S3 data stage bucket name that will hold device directories>");
	confBuilder.append(newLine);
	confBuilder.append(
	"#s3:command-stage-bucket-name=<S3 command stage bucket name that will hold command files sent by SyncLite Consolidator>");
	confBuilder.append(newLine);
	confBuilder.append(newLine);
	confBuilder.append("#==============Kafka Configuration=================");
	confBuilder.append(newLine);
	confBuilder.append("#kafka-producer:bootstrap.servers=localhost:9092,localhost:9093,localhost:9094");
	confBuilder.append(newLine);
	confBuilder.append("#kafka-producer:<any_other_kafka_producer_property> = <kafka_producer_property_value>");
	confBuilder.append(newLine);
	confBuilder.append("#kafka-producer:<any_other_kafka_producer_property> = <kafka_producer_property_value>");
	confBuilder.append(newLine);
	confBuilder.append("#kafka-consumer:bootstrap.servers=localhost:9092,localhost:9093,localhost:9094");
	confBuilder.append(newLine);
	confBuilder.append("#kafka-consumer:<any_other_kafka_consumer_property> = <kafka_consumer_property_value>");
	confBuilder.append(newLine);
	confBuilder.append("#kafka-consumer:<any_other_kafka_consumer_property> = <kafka_consumer_property_value>");
	confBuilder.append(newLine);
	confBuilder.append(newLine);
	confBuilder.append("#==============Table filtering Configuration=================");
	confBuilder.append(newLine);
	confBuilder.append("#include-tables=<comma separate table list>");
	confBuilder.append(newLine);
	confBuilder.append("#exclude-tables=<comma separate table list>");
	confBuilder.append(newLine);
	confBuilder.append(newLine);
	confBuilder.append("#==============Logger Configuration==================");
	confBuilder.append(newLine);
	confBuilder.append("#log-queue-size=2147483647");
	confBuilder.append(newLine);
	confBuilder.append("#log-segment-flush-batch-size=1000000");
	confBuilder.append(newLine);
	confBuilder.append("#log-segment-switch-log-count-threshold=1000000");
	confBuilder.append(newLine);
	confBuilder.append("#log-segment-switch-duration-threshold-ms=5000");
	confBuilder.append(newLine);
	confBuilder.append("#log-segment-shipping-frequency-ms=5000");
	confBuilder.append(newLine);
	confBuilder.append("#log-segment-page-size=4096");
	confBuilder.append(newLine);
	confBuilder.append("#log-max-inlined-arg-count=16");
	confBuilder.append(newLine);
	confBuilder.append("#use-precreated-data-backup=false");
	confBuilder.append(newLine);
	confBuilder.append("#vacuum-data-backup=true");
	confBuilder.append(newLine);
	confBuilder.append("#skip-restart-recovery=false");
	confBuilder.append(newLine);
	confBuilder.append(newLine);
	confBuilder.append("#==============Command Handler Configuration==================");
	confBuilder.append(newLine);
	confBuilder.append("#enable-command-handler=false|true");
	confBuilder.append(newLine);
	confBuilder.append("#command-handler-type=INTERNAL|EXTERNAL");
	confBuilder.append(newLine);
	confBuilder.append("#external-command-handler=synclite_command_processor.bat <COMMAND> <COMMAND_FILE>");
	confBuilder.append(newLine);
	confBuilder.append("#external-command-handler=synclite_command_processor.sh <COMMAND> <COMMAND_FILE>");
	confBuilder.append(newLine);
	confBuilder.append("#command-handler-frequency-ms=10000");
	confBuilder.append(newLine);
	confBuilder.append(newLine);
	confBuilder.append("#==============Device Configuration==================");
	confBuilder.append(newLine);
	String deviceEncryptionKeyFile = Path.of(System.getProperty("user.home"), ".ssh", "synclite_public_key.der")
	.toString();
	confBuilder.append("#device-encryption-key-file=" + deviceEncryptionKeyFile);
	confBuilder.append(newLine);
	confBuilder.append("#device-name=");
	confBuilder.append(newLine);

	conf = confBuilder.toString();
}

properties.put("synclite-logger-configuration", conf);

String srcDatabaseStatus = "";
String srcSchemaStatus = "";

switch (properties.get("src-type").toString()) {
case "DUCKDB":
	srcDatabaseStatus = "";
	srcSchemaStatus = "";
	break;
case "CSV":
	srcDatabaseStatus = "disabled";
	srcSchemaStatus = "disabled";
	break;
case "MONGODB":
	srcDatabaseStatus = "";
	srcSchemaStatus = "disabled";
	break;
case "MYSQL":
	srcDatabaseStatus = "disabled";
	srcSchemaStatus = "";
	break;
case "POSTGRESQL":
	srcDatabaseStatus = "";
	srcSchemaStatus = "";
	break;
case "SQLITE":
	srcDatabaseStatus = "disabled";
	srcSchemaStatus = "disabled";
	break;
}
%>

<body>
	<%@include file="html/menu.html"%>	

	<div class="main">
		<h2>Configure SyncLite DB Reader</h2>
		<%
		if (errorMsg != null) {
			out.println("<h4 style=\"color: red;\">" + errorMsg + "</h4>");
		}
		%>

		<form action="${pageContext.request.contextPath}/validateDBReader"	method="post">
			<table>
				<tbody>
					<tr>
						<td>SyncLite Device Directory</td>
						<td><input type="text" size=50 id="synclite-device-dir"
							name="synclite-device-dir"
							value="<%=properties.get("synclite-device-dir")%>"
							title="Specify SyncLite device directory" readonly/>
						</td>
					</tr>

					<tr>
						<td colspan=5>
						    <div class="divider">
						        <span class="divider-text">Source DB Connection Configurations</span>
						    </div>
					    </td>
				    </tr>

					<tr>
						<td>Source DB Type</td>
						<td><select id="src-type" name="src-type" value="<%=properties.get("src-type")%>" onchange="this.form.action='configureDBReader.jsp'; resetFields(); this.form.submit();" title="Select source database type.">
								<%
								if (properties.get("src-type").equals("CSV")) {
									out.println("<option value=\"CSV\" selected>CSV Files</option>");
								} else {
									out.println("<option value=\"CSV\">CSV Files</option>");
								}
								if (properties.get("src-type").equals("DUCKDB")) {
									out.println("<option value=\"DUCKDB\" selected>DuckDB</option>");
								} else {
									out.println("<option value=\"DUCKDB\">DuckDB</option>");
								}
								if (properties.get("src-type").equals("MONGODB")) {
									out.println("<option value=\"MONGODB\" selected>MongoDB</option>");
								} else {
									out.println("<option value=\"MONGODB\">MongoDB</option>");
								}
								if (properties.get("src-type").equals("MYSQL")) {
									out.println("<option value=\"MYSQL\" selected>MySQL</option>");
								} else {
									out.println("<option value=\"MYSQL\">MySQL</option>");
								}
								if (properties.get("src-type").equals("POSTGRESQL")) {
									out.println("<option value=\"POSTGRESQL\" selected>PostgreSQL</option>");
								} else {
									out.println("<option value=\"POSTGRESQL\">PostgreSQL</option>");
								}
								if (properties.get("src-type").equals("SQLITE")) {
									out.println("<option value=\"SQLITE\" selected>SQLite</option>");
								} else {
									out.println("<option value=\"SQLITE\">SQLite</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<% 
						if (!properties.get("src-type").equals("CSV")) {
							out.println("<tr>");
							out.println("<td>Source DB (JDBC) Connection String</td>");
							out.println("<td><input type=\"text\" size=50 id=\"src-connection-string\" name=\"src-connection-string\"  value=\"" + properties.get("src-connection-string") + "\"title=\"Specify source database connection string\"/></td>");
							out.println("</tr>");
						}

						if (properties.get("src-type").equals("CSV")) {
							out.println("<tr><td>File Storage</td>");
							out.println("<td><select id=\"src-file-storage-type\" name=\"src-file-storage-type\" value=\"" + properties.get("src-file-storage-type") + "\" onchange=\"this.form.action='configureDBReader.jsp'; resetFields(); this.form.submit();\" title=\"Specify source file storage type\">");
							if (properties.get("src-file-storage-type").equals("LOCAL_FS")) {
								out.println("<option value=\"LOCAL_FS\" selected>Local File System</option>");
							} else {
								out.println("<option value=\"LOCAL_FS\">Local File System</option>");
							}
							if (properties.get("src-file-storage-type").equals("S3")) {
								out.println("<option value=\"S3\" selected>Amazon S3</option>");
							} else {
								out.println("<option value=\"S3\">Amazon S3</option>");
							}
							if (properties.get("src-file-storage-type").equals("SFTP")) {
								out.println("<option value=\"SFTP\" selected>SFTP Directory</option>");
							} else {
								out.println("<option value=\"SFTP\">SFTP Directory</option>");
							}
							out.println("</select></td></tr>");
							
							if (properties.get("src-file-storage-type").equals("LOCAL_FS")) {
								out.println("<tr><td>Local File System Directory</td>");
								out.println("<td><input type=\"text\" size=50 name=\"src-file-storage-local-fs-directory\" id=\"src-file-storage-local-fs-directory\" value=\"" + properties.get("src-file-storage-local-fs-directory") + "\" title=\"Specify local file system directory holding data files\"></td></tr>");
							} else if (properties.get("src-file-storage-type").equals("S3")) {

								out.println("<tr><td>Local File System Directory</td>");
								out.println("<td><input type=\"text\" size=50 name=\"src-file-storage-local-fs-directory\" id=\"src-file-storage-local-fs-directory\" value=\"" + properties.get("src-file-storage-local-fs-directory") + "\" title=\"Specify local file system directory holding data files\"></td></tr>");

								out.println("<tr><td>Source S3 URL </td>");
								out.println("<td><input type=\"text\" size=50 name=\"src-file-storage-s3-url\" id=\"src-file-storage-s3-url\" value=\"" + properties.get("src-file-storage-s3-url") + "\" title=\"Specify S3 URL\"></td></tr>");

								out.println("<tr><td>Source S3 Bucket Name</td>");
								out.println("<td><input type=\"text\" name=\"src-file-storage-s3-bucket-name\" id=\"src-file-storage-s3-bucket-name\" value=\"" + properties.get("src-file-storage-s3-bucket-name") + "\" title=\"Specify S3 bucket name\"></td></tr>");

								out.println("<tr><td>Source S3 Access Key </td>");
								out.println("<td><input type=\"password\" name=\"src-file-storage-s3-access-key\" id=\"src-file-storage-s3-access-key\" value=\"" + properties.get("src-file-storage-s3-access-key") + "\" title=\"Specify S3 access key\"></td></tr>");
								
								out.println("<tr><td>Source S3 Secret Key </td>");
								out.println("<td><input type=\"password\" name=\"src-file-storage-s3-secret-key\" id=\"src-file-storage-s3-secret-key\" value=\"" + properties.get("src-file-storage-s3-secret-key") + "\" title=\"Specify S3 secret key\"></td></tr>");
								
							} else if (properties.get("src-file-storage-type").equals("SFTP")) {

								out.println("<tr><td>Local File System Directory</td>");
								out.println("<td><input type=\"text\" size=50 name=\"src-file-storage-local-fs-directory\" id=\"src-file-storage-local-fs-directory\" value=\"" + properties.get("src-file-storage-local-fs-directory") + "\" title=\"Specify local file system directory holding data files\"></td></tr>");

								out.println("<tr><td>Source SFTP Host</td>");
								out.println("<td><input type=\"text\" name=\"src-file-storage-sftp-host\" id=\"src-file-storage-sftp-host\" value=\"" + properties.get("src-file-storage-sftp-host") + "\" title=\"Specify source SFTP host name\"></td></tr>");

								out.println("<tr><td>Source SFTP Port</td>");
								out.println("<td><input type=\"number\" name=\"src-file-storage-sftp-port\" id=\"src-file-storage-sftp-port\" value=\"" + properties.get("src-file-storage-sftp-port") + "\" title=\"Specify source SFTP host port\"></td></tr>");

								out.println("<tr><td>Source SFTP Directory</td>");
								out.println("<td><input type=\"text\" name=\"src-file-storage-sftp-directory\" id=\"src-file-storage-sftp-directory\" value=\"" + properties.get("src-file-storage-sftp-directory") + "\" title=\"Specify source SFTP directory\"></td></tr>");

								out.println("<tr><td>Source SFTP User</td>");
								out.println("<td><input type=\"text\" name=\"src-file-storage-sftp-user\" id=\"src-file-storage-sftp-user\" value=\"" + properties.get("src-file-storage-sftp-user") + "\" title=\"Specify source SFTP user\"></td></tr>");

								out.println("<tr><td>Source SFTP User Password</td>");
								out.println("<td><input type=\"password\" name=\"src-file-storage-sftp-password\" id=\"src-file-storage-sftp-password\" value=\"" + properties.get("src-file-storage-sftp-password") + "\" title=\"Specify source SFTP password\"></td></tr>");

							}
								
							out.println("<tr><td>Source CSV Files with Headers</td>");
							out.println("<td><select id=\"src-csv-files-with-headers\" name=\"src-csv-files-with-headers\" value=\"" + properties.get("src-csv-files-with-headers") + "\" title=\"Specify if source CSV files have header\">");
							if (properties.get("src-csv-files-with-headers").equals("true")) {
								out.println("<option value=\"true\" selected>true</option>");
							} else {
								out.println("<option value=\"true\">true</option>");
							}
							if (properties.get("src-csv-files-with-headers").equals("false")) {
								out.println("<option value=\"false\" selected>false</option>");
							} else {
								out.println("<option value=\"false\">false</option>");
							}
							out.println("</select></td></tr>");


							out.println("<tr><td>Source CSV File Field Delimiter</td>");
							out.println("<td><input type=\"text\" name=\"src-csv-files-field-delimiter\" id=\"src-csv-files-field-delimiter\" value=\"" + properties.get("src-csv-files-field-delimiter") + "\" title=\"Specify field delimiter used in source CSV files\"></td></tr>");

							out.println("<tr><td>Source CSV File Record Delimiter</td>");
							out.println("<td><input type=\"text\" name=\"src-csv-files-record-delimiter\" id=\"src-csv-files-record-delimiter\" value=\"" + properties.get("src-csv-files-record-delimiter") + "\" title=\"Specify record delimiter used in source CSV files\"></td></tr>");

							out.println("<tr><td>Source CSV File Escape Character</td>");
							out.println("<td><input type=\"text\" name=\"src-csv-files-escape-character\" id=\"src-csv-files-escape-character\" value=\"" + properties.get("src-csv-files-escape-character").toString().replace("\"", "&quot;") + "\" title=\"Specify escape character used in source CSV files\"></td></tr>");

							out.println("<tr><td>Source CSV File Quote Character</td>");
							out.println("<td><input type=\"text\" name=\"src-csv-files-quote-character\" id=\"src-csv-files-quote-character\" value=\"" + properties.get("src-csv-files-quote-character").toString().replace("\"", "&quot;") + "\" title=\"Specify quote character used in source CSV files\"></td></tr>");

							out.println("<tr><td>Source CSV File Null String</td>");
							out.println("<td><input type=\"text\" name=\"src-csv-files-null-string\" id=\"src-csv-files-null-string\" value=\"" + properties.get("src-csv-files-null-string") + "\" title=\"Specify null string used in source CSV files\"></td></tr>");

							out.println("<tr><td>Source CSV File Ignore Empty Lines</td>");
							out.println("<td><select id=\"src-csv-files-ignore-empty-lines\" name=\"src-csv-files-ignore-empty-lines\" value=\"" + properties.get("src-csv-files-ignore-empty-lines") + "\" title=\"Specify if dbreader should ignore empty lines in source CSV files\">");
							if (properties.get("src-csv-files-ignore-empty-lines").equals("true")) {
								out.println("<option value=\"true\" selected>true</option>");
							} else {
								out.println("<option value=\"true\">true</option>");
							}
							if (properties.get("src-csv-files-ignore-empty-lines").equals("false")) {
								out.println("<option value=\"false\" selected>false</option>");
							} else {
								out.println("<option value=\"false\">false</option>");
							}
							out.println("</select></td></tr>");

							out.println("<tr><td>Source CSV File Trim Fields</td>");
							out.println("<td><select id=\"src-csv-files-trim-fields\" name=\"src-csv-files-trim-fields\" value=\"" + properties.get("src-csv-files-trim-fields") + "\" title=\"Specify if dbreader should trim leading and trailing spaces from CSV record fields\">");
							if (properties.get("src-csv-files-trim-fields").equals("true")) {
								out.println("<option value=\"true\" selected>true</option>");
							} else {
								out.println("<option value=\"true\">true</option>");
							}
							if (properties.get("src-csv-files-trim-fields").equals("false")) {
								out.println("<option value=\"false\" selected>false</option>");
							} else {
								out.println("<option value=\"false\">false</option>");
							}
							out.println("</select></td></tr>");
						}						
					%>

					<tr>
						<td>Source DB Catalog/Database</td>
						<td><input type="text" size=30 id="src-database"
							name="src-database"
							value="<%=properties.get("src-database")%>"
							title="Specify source catalog/database" <%=srcDatabaseStatus%>/>
						</td>
					</tr>

					<tr>
						<td>Source DB Schema</td>
						<td><input type="text" size=30 id="src-schema"
							name="src-schema"
							value="<%=properties.get("src-schema")%>"
							title="Specify source schema"/ <%=srcSchemaStatus%>>
						</td>
					</tr>

					<tr>
						<td>Source DB User</td>
						<td><input type="text" size=30 id="src-user"
							name="src-user"
							value="<%=properties.get("src-user")%>"
							title="Specify source database user name"/>
						</td>
					</tr>

					<tr>
						<td>Source DB Password</td>
						<td><input type="password" size=30 id="src-password"
							name="src-password"
							value="<%=properties.get("src-password")%>"
							title="Specify source database password"/>
						</td>
					</tr>

					<tr>
						<td>Source DB Connection Initialization Statement</td>
						<td><input type="text" size=50 id="src-connection-initialization-stmt"
							name="src-connection-initialization-stmt"
							value="<%=properties.get("src-connection-initialization-stmt")%>"
							title="Specify source connection intiailization statement if any which you would want SyncLite to execute upon getting a connection from source DB. e.g. setting session variables etc."/>
						</td>
					</tr>

					<tr>
						<td>Source DB Connection Timeout (s)</td>
						<td><input type="number" size=30 id="src-connection-timeout-s"
							name="src-connection-timeout-s"
							value="<%=properties.get("src-connection-timeout-s")%>"
							title="Specify source DB connection timeout in seconds"/>
						</td>
					</tr>

					<tr>
						<td colspan=5>
				    		<div class="divider">
				        		<span class="divider-text">Source DB Object Configurations</span>
				    		</div>
						</td>
					</tr>
					
					<tr>
						<td>Source DB Object Type</td>
						<td><select id="src-object-type" name="src-object-type" value="<%=properties.get("src-object-type")%>" title="Specify object types TABLE/VIEW/ALL to extract">
								<%
								if (properties.get("src-object-type").equals("TABLE")) {
									out.println("<option value=\"TABLE\" selected>TABLE</option>");
								} else {
									out.println("<option value=\"TABLE\">TABLE</option>");
								}
								if (properties.get("src-object-type").equals("VIEW")) {
									out.println("<option value=\"VIEW\" selected>VIEW</option>");
								} else {
									out.println("<option value=\"VIEW\">VIEW</option>");
								}
								if (properties.get("src-object-type").equals("ALL")) {
									out.println("<option value=\"ALL\" selected>ALL</option>");
								} else {
									out.println("<option value=\"ALL\">ALL</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Source DB Table/View Name Pattern</td>
						<td><input type="text" size=30 id="src-object-name-pattern"
							name="src-object-name-pattern"
							value="<%=properties.get("src-object-name-pattern")%>"
							title="Specify a pattern (as supported by SQL Like predicates) of table/view names to fetch metadata from source DB"/>
						</td>
					</tr>

					<tr>
						<td>Source DB Object Metadata Read Method</td>
						<td><select id="src-object-metadata-read-method" name="src-object-metadata-read-method" value="<%=properties.get("src-object-metadata-read-method")%>" title="Select metadata read method as NATIVE or JDBC. NATIVE method uses native SQL queries on the source DB to extract tables/views metadata information. JDBC method uses JDBC Metadata Object to extract metadata.">
								<%
								if (!properties.get("src-type").equals("MONGODB") && !properties.get("src-type").equals("CSV")) {
									if (properties.get("src-object-metadata-read-method").equals("JDBC")) {
										out.println("<option value=\"JDBC\" selected>JDBC</option>");
									} else {
										out.println("<option value=\"JDBC\">JDBC</option>");
									}
								}
								if (properties.get("src-object-metadata-read-method").equals("NATIVE")) {
									out.println("<option value=\"NATIVE\" selected>NATIVE</option>");
								} else {
									out.println("<option value=\"NATIVE\">NATIVE</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Source DB Column Metadata Read Method</td>
						<td><select id="src-column-metadata-read-method" name="src-column-metadata-read-method" value="<%=properties.get("src-column-metadata-read-method")%>" title="Select metadata read method as NATIVE or JDBC. NATIVE method uses native SQL queries on the source DB to extract column metadata information. JDBC method uses JDBC Metadata Object to extract metadata.">
								<%
								if (!properties.get("src-type").equals("MONGODB") && !properties.get("src-type").equals("CSV")) {
									if (properties.get("src-column-metadata-read-method").equals("JDBC")) {
										out.println("<option value=\"JDBC\" selected>JDBC</option>");
									} else {
										out.println("<option value=\"JDBC\">JDBC</option>");
									}
								}
								if (properties.get("src-column-metadata-read-method").equals("NATIVE")) {
									out.println("<option value=\"NATIVE\" selected>NATIVE</option>");
								} else {
									out.println("<option value=\"NATIVE\">NATIVE</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Source DB Constraint Metadata Read Method</td>
						<td><select id="src-constraint-metadata-read-method" name="src-constraint-metadata-read-method" value="<%=properties.get("src-constraint-metadata-read-method")%>" title="Select DB constraint metadata read method as NATIVE or JDBC. NATIVE method uses native SQL queries on the source DB to extract constraint metadata information. JDBC method uses JDBC Metadata Object to extract metadata.">
								<%
								if (!properties.get("src-type").equals("MONGODB") && !properties.get("src-type").equals("CSV")) {
									if (properties.get("src-constraint-metadata-read-method").equals("JDBC")) {
										out.println("<option value=\"JDBC\" selected>JDBC</option>");
									} else {
										out.println("<option value=\"JDBC\">JDBC</option>");
									}
								}
								if (properties.get("src-constraint-metadata-read-method").equals("NATIVE")) {
									out.println("<option value=\"NATIVE\" selected>NATIVE</option>");
								} else {
									out.println("<option value=\"NATIVE\">NATIVE</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Source DB Default Unique Key Column List</td>
						<td><input type="text" size=30 id="src-default-unique-key-column-list"
							name="src-default-unique-key-column-list"
							value="<%=properties.get("src-default-unique-key-column-list")%>"
							title="Specify (a comma separated list) if you have a set of unique key columns such as ID in all or most source DB tables/views which may or may not have been defined exclicitly on those tables/views, which you want SyncLite to leverage for efficient replication. If you specify this default configuration then you do not need to specify the per object unique key coniguration for all such tables/views ( which do not have PK/UK explicitly defined) on the Configure Tables/Views page."/>
						</td>
					</tr>

					<tr>
						<td>Source DB Default Incremental Key Column List</td>
						<td><input type="text" size=30 id="src-default-incremental-key-column-list"
							name="src-default-incremental-key-column-list"
							value="<%=properties.get("src-default-incremental-key-column-list")%>"
							title="Specify if you have incremental key columns such as last_update_time in all or most source DB tables/views which you want SyncLite to use for performing incremental replication. If you specify this default configuration then you do not need to specify the per object incremental keys for all such tables/views on the Configure Tables/Views page."/>
						</td>
					</tr>

					<tr>
						<td>Source DB Default Soft Delete Condition</td>
						<td><input type="text" size=30 id="src-default-soft-delete-condition"
							name="src-default-soft-delete-condition"
							value="<%=properties.get("src-default-soft-delete-condition")%>"
							title="If you have a standard mechanism to perform soft deletes on the source database by marking a column say is_deleted = 'Y' for deleted records on all or most source DB tables/views then SyncLite can leverage that to replicate deletes. If you specify this default configuration then you do not need to specify the per object soft delete condition for all such tables/views on the Configure Tables/Views page."/>
						</td>
					</tr>

					<tr>
						<td>Source DB Default Mask Column List</td>
						<td><input type="text" size=30 id="src-default-mask-column-list"
							name="src-default-mask-column-list"
							value="<%=properties.get("src-default-mask-column-list")%>"
							title="Specify a comma separated list of columns if you have a particular set of columns containing sensitive data in all or most source tables/views and you want SyncLite to mask them before replication. If you specify this default configuration then you do not need to specify the per table mask columns configuration for all such tables/viws on the Configure Tables/Views page."/>
						</td>
					</tr>

					<tr>
						<td>Source DB Query Timestamp Conversion Function/Expression</td>
						<td><input type="text" size=50 id="src-query-timestamp-conversion-function"
							name="src-query-timestamp-conversion-function"
							value="<%=properties.get("src-query-timestamp-conversion-function")%>"
							title="Specify an appropriate timestamp conversion function compatible with your source database which should be used by SyncLite while constructing incremental read queries. Please note that the function expression you specify must contain a $ character which will be replaced by DBreader with an actual value."/>
						</td>
					</tr>

					<tr>
						<td>Source DB Initial Timestamp Incremental Key Value</td>
						<td><input type="text" size=30 id="src-timestamp-incremental-key-initial-value"
							name="src-timestamp-incremental-key-initial-value"
							value="<%=properties.get("src-timestamp-incremental-key-initial-value")%>"
							title="Specify an appropriate initial timestamp incremental key value compatible with your source database which should be used by SyncLite as a low watermark for starting incremental replication of source DB objects."/>
						</td>
					</tr>


					<tr>
						<td>Source DB Initial Numeric Incremental Key Value</td>
						<td><input type="text" size=30 id="src-numeric-incremental-key-initial-value"
							name="src-numeric-incremental-key-initial-value"
							value="<%=properties.get("src-numeric-incremental-key-initial-value")%>"
							title="Specify an appropriate initial numeric incremental key value compatible with your source database which should be used by SyncLite as a low watermark for starting incremental replication of source DB objects."/>
						</td>
					</tr>

					<tr>
						<td>Source DB Numeric Value Mask</td>
						<td><input type="number" size=5 id="src-numeric-value-mask"
							name="src-numeric-value-mask"
							value="<%=properties.get("src-numeric-value-mask")%>"
							title="Specify a digit between [0-9] to mask digits in numeric values for masked columns"/>
						</td>
					</tr>

					<tr>
						<td>Source DB Alphabetic Value Mask</td>
						<td><input type="text" size=5 id="src-alphabetic-value-mask"
							name="src-alphabetic-value-mask"
							value="<%=properties.get("src-alphabetic-value-mask")%>"
							title="Specify an alphabetic character [a-zA-Z] to mask characters in textual values of masked columns"/>
						</td>
					</tr>

					<tr>
						<td>Source DB Record Limit Per Table/View</td>
						<td><input type="number" size=30 id="src-dbreader-object-record-limit"
							name="src-dbreader-object-record-limit"
							value="<%=properties.get("src-dbreader-object-record-limit")%>"
							title="Specify a record limit per table/view. If a non-zero value is specified then the reading is limited to only that many records. The default value is 0 meaning all records are considered for replication from each source table/view."/>
						</td>
					</tr>
					
					
					<tr>
						<td>Read Records with NULL Incremental Key Value</td>
						<td><select id="src-read-null-incremental-key-records" name="src-read-null-incremental-key-records" value="<%=properties.get("src-read-null-incremental-key-records")%>" title="Specify if DBReader should read records with NULL values in the specified incremental key columns for source tables/views. Please note that the incremental replication will keep reading such records over and over again in each reading iteration.">
								<%
								if (properties.get("src-read-null-incremental-key-records").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("src-read-null-incremental-key-records").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Compute Max Value of Incremental Key Columns in Source DB</td>
						<td><select id="src-compute-max-incremental-key-in-db" name="src-compute-max-incremental-key-in-db" value="<%=properties.get("src-compute-max-incremental-key-in-db")%>" title="Specify if DBReader should leverage source database to compute current maximum value of specified incremental keys.">
								<%
								if (properties.get("src-compute-max-incremental-key-in-db").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("src-compute-max-incremental-key-in-db").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Quote Source DB Object Names</td>
						<td><select id="src-quote-object-names" name="src-quote-object-names" value="<%=properties.get("src-quote-object-names")%>" title="Specify if DBReader Should quote source DB table/view names while reading data from source DB.">
								<%
								if (properties.get("src-quote-object-names").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("src-quote-object-names").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Quote Source DB Object Column Names</td>
						<td><select id="src-quote-column-names" name="src-quote-column-names" value="<%=properties.get("src-quote-column-names")%>" title="Specify if DBReader Should quote source DB table/view column names while querying source DB.">
								<%
								if (properties.get("src-quote-column-names").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("src-quote-column-names").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Use Catalog Scope Resolution</td>
						<td><select id="src-use-catalog-scope-resolution" name="src-use-catalog-scope-resolution" value="<%=properties.get("src-use-catalog-scope-resolution")%>" title="Specify if DBReader Should use catalog name prefix while framing SQL statements on source db">
								<%
								if (properties.get("src-use-catalog-scope-resolution").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("src-use-catalog-scope-resolution").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Use Schema Scope Resolution</td>
						<td><select id="src-use-schema-scope-resolution" name="src-use-schema-scope-resolution" value="<%=properties.get("src-use-schema-scope-resolution")%>" title="Specify if DBReader Should use schema name prefix while framing SQL statements on source db">
								<%
								if (properties.get("src-use-schema-scope-resolution").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("src-use-schema-scope-resolution").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td colspan=5>
				    		<div class="divider">
				        		<span class="divider-text">Source DB Reader Configurations</span>
				    		</div>
						</td>
					</tr>

					<tr>
						<td>Source DB Reader Method</td>
						<td><select id="src-dbreader-method" name="src-dbreader-method" value="<%=properties.get("src-dbreader-method")%>" onchange="this.form.action='configureDBReader.jsp'; resetDBReaderInterval(); this.form.submit();"  title="Choose DBReader method. INCREMENTAL method leverages specified numeric/timestamp incremental keys from the source tables to identify changed records. LOG_BASED method uses native mechanism for extracting source DB table changes. For MongoDB source, LOG_BASED method works only if replica sets are configured and user has permissions to read from the oplog.rs collection of local database.">
								<%
								if (properties.get("src-dbreader-method").equals("INCREMENTAL")) {
									out.println("<option value=\"INCREMENTAL\" selected>INCREMENTAL</option>");
								} else {
									out.println("<option value=\"INCREMENTAL\">INCREMENTAL</option>");
								}
								if (properties.get("src-dbreader-method").equals("LOG_BASED")) {
									out.println("<option value=\"LOG_BASED\" selected>LOG_BASED</option>");
								} else {
									out.println("<option value=\"LOG_BASED\">LOG_BASED</option>");
								}
								%>
							</select>
						</td>
					</tr>
					
					<tr>
						<td>Source DB Reader Processors</td>
						<td><input type="number" size=30 id="src-dbreader-processors"
							name="src-dbreader-processors"
							value="<%=properties.get("src-dbreader-processors")%>"
							title="Specify maximum number of DB reader processors/threads to be used by DB reader should attempt"/>
						</td>
					</tr>
					
					<tr>
						<td>Source DB Reader Interval (s)</td>
						<td><input type="number" size=30 id="src-dbreader-interval-s"
							name="src-dbreader-interval-s"
							value="<%=properties.get("src-dbreader-interval-s")%>"
							title="Specify the interval in seconds at which the DB reader should attempt to read data from source db"/>
						</td>
					</tr>

					<tr>
						<td>Stop DB Reader After First Iteration</td>
						<td><select id="dbreader-stop-after-first-iteration" name="dbreader-stop-after-first-iteration" value="<%=properties.get("dbreader-stop-after-first-iteration")%>" title="Stop DBReader after first iteration">
								<%
								if (properties.get("dbreader-stop-after-first-iteration").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("dbreader-stop-after-first-iteration").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Retry Failed Object Reads</td>
						<td><select id="dbreader-retry-failed-objects" name="dbreader-retry-failed-objects" value="<%=properties.get("dbreader-retry-failed-objects")%>" title="Retry reading failed objects">
								<%
								if (properties.get("dbreader-retry-failed-objects").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("dbreader-retry-failed-objects").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Failed Object Retry Interval (s)</td>
						<td><input type="number" size=30 id="dbreader-failed-object-retry-interval-s"
							name="dbreader-failed-object-retry-interval-s"
							value="<%=properties.get("dbreader-failed-object-retry-interval-s")%>"
							title="Specify the interval in seconds at which the DB reader should attempt retrying read from failed objects"/>
						</td>
					</tr>

					<tr>
						<td>Source DB Reader Batch Size</td>
						<td><input type="number" size=30 id="src-dbreader-batch-size"
							name="src-dbreader-batch-size"
							value="<%=properties.get("src-dbreader-batch-size")%>" 
							title="Specify batch size for each read done on source table/view by the DB reader"/>
						</td>
					</tr>

					<tr>
						<td>Infer and Publish Schema Changes</td>
						<td><select id="src-infer-schema-changes" name="src-infer-schema-changes" value="<%=properties.get("src-infer-schema-changes")%>" title="Specify if DB reader should infer and publish structural schema changes in source DB tables/views(column deletions and additions). Please use this option with caution as it may lead to schema changes on destination tables.">
								<%
								if (properties.get("src-infer-schema-changes").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("src-infer-schema-changes").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Infer and Publish Table/View Drops</td>
						<td><select id="src-infer-object-drop" name="src-infer-object-drop" value="<%=properties.get("src-infer-object-drop")%>" title="Specify if DB reader should infer and publish dropped source DB tables/views. Please enable this option with care as it may lead to dropping tables from destination.">
								<%
								if (properties.get("src-infer-object-drop").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("src-infer-object-drop").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Infer and Publish Table/View Creations</td>
						<td><select id="src-infer-object-create" name="src-infer-object-create" value="<%=properties.get("src-infer-object-create")%>" title="Specify if DB reader should infer and publish created source DB tables/views. Please enable this option with care as it may lead to creating new tables on destination.">
								<%
								if (properties.get("src-infer-object-create").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("src-infer-object-create").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Reload All Tables/Views On Next Job Start</td>
						<td><select id="src-reload-objects" name="src-reload-objects" value="<%=properties.get("src-reload-objects")%>" title="Specify if all enabled tables/views should be reloaded from source DB. You can use Manage Objects page to selectively do this operation on individual tables/views.">
								<%
								if (properties.get("src-reload-objects").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("src-reload-objects").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Reload All Tables/Views on Each Job Restart</td>
						<td><select id="src-reload-objects-on-each-job-restart" name="src-reload-objects-on-each-job-restart" value="<%=properties.get("src-reload-objects-on-each-job-restart")%>" title="Specify if all enabled tables/views should be reloaded from source DB. You can use Manage Objects page to selectively do this operation on individual tables/views.">
								<%
								if (properties.get("src-reload-objects-on-each-job-restart").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("src-reload-objects-on-each-job-restart").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Reload All Table/View Schemas On Next Job Start</td>
						<td><select id="src-reload-object-schemas" name="src-reload-object-schemas" value="<%=properties.get("src-reload-object-schemas")%>" title="Specify if schemas should be reloaded for all tables/views. You can use Manage Objects page to selectively do this operation on individual tables/views.">
								<%
								if (properties.get("src-reload-object-schemas").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("src-reload-object-schemas").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Reload All Table/View Schemas On Each Job Restart</td>
						<td><select id="src-reload-object-schemas-on-each-job-restart" name="src-reload-object-schemas-on-each-job-restart" value="<%=properties.get("src-reload-object-schemas-on-each-job-restart")%>" title="Specify if schemas should be reloaded for all tables/views. You can use Manage Objects page to selectively do this operation on individual tables/views.">
								<%
								if (properties.get("src-reload-object-schemas-on-each-job-restart").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}
								if (properties.get("src-reload-object-schemas-on-each-job-restart").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
							</select>
						</td>
					</tr>

					<tr>
						<td>Job Trace Level</td>
						<td><select id="dbreader-trace-level" name="dbreader-trace-level" title="Specify dbreader trace level. DEBUG level indicates exhaustive tracing, ERROR level indicates only error reporting and INFO level indicates tracing of important events including errors in the trace files.">
								<%
								if (properties.get("dbreader-trace-level").equals("ERROR")) {
									out.println("<option value=\"ERROR\" selected>ERROR</option>");
								} else {
									out.println("<option value=\"ERROR\">ERROR</option>");
								}

								if (properties.get("dbreader-trace-level").equals("INFO")) {
									out.println("<option value=\"INFO\" selected>INFO</option>");
								} else {
									out.println("<option value=\"INFO\">INFO</option>");
								}

								if (properties.get("dbreader-trace-level").equals("DEBUG")) {
									out.println("<option value=\"DEBUG\" selected>DEBUG</option>");
								} else {
									out.println("<option value=\"DEBUG\">DEBUG</option>");
								}
								%>
						</select></td>
					</tr>					

					<tr>
						<td>Enable Statistics Collector</td>
						<td><select id="dbreader-enable-statistics-collector" name="dbreader-enable-statistics-collector" title="Specify if statistics collector should be enabled.">
								<%
								if (properties.get("dbreader-enable-statistics-collector").equals("true")) {
									out.println("<option value=\"true\" selected>true</option>");
								} else {
									out.println("<option value=\"true\">true</option>");
								}

								if (properties.get("dbreader-enable-statistics-collector").equals("false")) {
									out.println("<option value=\"false\" selected>false</option>");
								} else {
									out.println("<option value=\"false\">false</option>");
								}
								%>
						</select></td>
					</tr>					

					<tr>
						<td>Statistics Update Interval(s)</td>
						<td><input type="number" size=30 id="dbreader-update-statistics-interval-s"
							name="dbreader-update-statistics-interval-s"
							value="<%=properties.get("dbreader-update-statistics-interval-s")%>" 
							title="Specify statistics update interbal in seconds"/>
						</td>
					</tr>

					<tr>
						<td>Job JVM Arguments</td>
						<td><input type="text" id="jvm-arguments"
							name="jvm-arguments"
							value="<%=properties.get("jvm-arguments")%>"
							title ="Specify JVM arguments which should be used while starting the dbreader job. e.g. For setting initial and max heap size as 8GB, you can specify -Xms8g -Xmx8g"/></td>
					</tr>					

					<tr>
						<td colspan=5>
				    		<div class="divider">
				        		<span class="divider-text">SyncLite Logger Configurations</span>
				    		</div>
						</td>
					</tr>

					<tr>
						<td>SyncLite Logger Configuration File Path</td>
						<td><input type="text" size=50 id="synclite-logger-configuration-file"
							name="synclite-logger-configuration-file"
							value="<%=properties.get("synclite-logger-configuration-file")%>"
							title="Specify SyncLite Logger Configuration File Path"/>
						</td>
					</tr>					

					<tr>
						<td>SyncLite Logger Configuration</td>
						<td><textarea name="synclite-logger-configuration" id="synclite-logger-configuration" rows="25" cols="100" title="Specify SyncLite logger configuration. Specified device configurations are written into a .conf file and supplied to initialization of each database/device. Please note the defaults specified for local-stage-directory and destination-type."><%=properties.get("synclite-logger-configuration")%></textarea>
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