tEnv.executeSql("CREATE TABLE ${table.name}("
+ "<#list table.columns as column>${column.source} ${column.type} <#if column.class.name == 'com.machdatum.thingmanage.model.Watermark'>, WATERMARK FOR ${column.source} AS ${column.as} - INTERVAL '${column.interval}' ${column.unit}"</#if> <#if (column_has_next)>,</#if></#list>
+ ")"
+ "WITH"
+ "("
+ "'connector' = 'kafka',"
+ "'topic' = '<#list kafkaconfiguration.topics as topic>${topic}<#if (topic_has_next)>,</#if><#if !(topic_has_next)>',</#if></#list>"
+ "'properties.bootstrap.servers' = '<#list kafkaconfiguration.bootstrapServers as boot>${boot}<#if (boot_has_next)>,</#if><#if !(boot_has_next)>',</#if></#list>"
+ "'properties.group.id' = '${kafkaconfiguration.groupId}',"
+ "'scan.startup.mode' = '<#if kafkaconfiguration.startup == 'EARLIEST'>earliest-offset<#elseif kafkaconfiguration.startup == 'LATEST'>latest-offset<#else>earliest-offset</#if>',"
+ "'format' = 'json'"
+ ")")