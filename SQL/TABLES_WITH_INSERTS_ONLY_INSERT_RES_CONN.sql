BEGIN
	APP_APEX_MICROSERVICES.PKG_KAFKA_QUEUE.stop_item(p_project_id => 16, 
																									 p_dbid 			=> :dbid,
																									 p_errors 		=> :error_text, 
																									 p_status 		=> :status);
END;